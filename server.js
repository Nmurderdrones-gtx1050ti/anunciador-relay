const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
    pingTimeout: 60000,
    pingInterval: 25000,
});

// ===== CONFIGURAÇÃO =====
const SECRET_KEY = 'MUDE_ESSA_CHAVE_SECRETA_123';

// ===== ESTADO =====
let botSocket = null;
let botData = {
    stats: {
        botsAtivos: 0,
        totalBots: 0,
        blacklistSize: 0,
        blacklistItems: [],
        mensagens: [],
        intervalo: 180,
        username: 'Anunciador',
    },
    bots: [],
    logs: [],
    chatDatabase: [],
    servers: [], // Lista de servidores detectados
    analytics: {
        playersOverTime: [], // {timestamp, total}
        messagesPerHour: [], // {hour, count}
        topServers: [], // {serverKey, messages, players}
    }
};

// Health check
app.get('/', (req, res) => {
    res.json({
        status: 'online',
        botConectado: botSocket !== null,
        bots: botData.stats.botsAtivos,
        totalMensagens: botData.chatDatabase.length,
        totalServidores: botData.servers.length,
    });
});

app.get('/health', (req, res) => res.send('OK'));

// ===== FUNÇÕES DE ANALYTICS =====
function updateAnalytics() {
    // Atualiza contagem de jogadores ao longo do tempo
    const now = Date.now();
    let totalPlayers = 0;
    botData.servers.forEach(s => {
        if (s.status === 'online') totalPlayers += s.players.length;
    });
    
    botData.analytics.playersOverTime.push({ timestamp: now, total: totalPlayers });
    if (botData.analytics.playersOverTime.length > 720) { // Últimas 12h (1 ponto/minuto)
        botData.analytics.playersOverTime.shift();
    }
    
    // Atualiza mensagens por hora
    const currentHour = new Date().getHours();
    const hourData = botData.analytics.messagesPerHour.find(h => h.hour === currentHour);
    if (hourData) {
        hourData.count = botData.chatDatabase.filter(m => {
            const msgHour = new Date(m.timestamp).getHours();
            return msgHour === currentHour;
        }).length;
    } else {
        botData.analytics.messagesPerHour.push({ hour: currentHour, count: 1 });
    }
    
    // Top servidores
    const serverStats = {};
    botData.chatDatabase.forEach(m => {
        if (!serverStats[m.serverKey]) {
            serverStats[m.serverKey] = { messages: 0, players: new Set() };
        }
        serverStats[m.serverKey].messages++;
        serverStats[m.serverKey].players.add(m.username);
    });
    
    botData.analytics.topServers = Object.entries(serverStats)
        .map(([key, data]) => ({
            serverKey: key,
            messages: data.messages,
            uniquePlayers: data.players.size
        }))
        .sort((a, b) => b.messages - a.messages)
        .slice(0, 10);
}

// Atualiza analytics a cada minuto
setInterval(updateAnalytics, 60000);

// ===== SOCKET.IO =====
io.on('connection', (socket) => {

    // --- BOT DO PC ---
    if (socket.handshake.auth?.key === SECRET_KEY && socket.handshake.auth?.type === 'bot') {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        io.emit('botStatus', true);

        socket.on('syncData', (data) => {
            botData = { ...botData, ...data };
            updateAnalytics();
            io.emit('init', {
                stats: botData.stats,
                bots: botData.bots,
                logs: botData.logs.slice(-50),
                chatDatabase: botData.chatDatabase.slice(-100),
                servers: botData.servers,
                analytics: botData.analytics
            });
        });

        socket.on('log', (entry) => {
            botData.logs.push(entry);
            if (botData.logs.length > 200) botData.logs.shift();
            io.emit('log', entry);
        });

        socket.on('chatMessage', (data) => {
            botData.chatDatabase.push(data);
            if (botData.chatDatabase.length > 10000) botData.chatDatabase.shift();
            io.emit('chatMessage', data);
            updateAnalytics();
        });

        socket.on('serverUpdate', (serverData) => {
            const idx = botData.servers.findIndex(s => s.key === serverData.key);
            if (idx >= 0) {
                botData.servers[idx] = { ...botData.servers[idx], ...serverData };
            } else {
                botData.servers.push(serverData);
            }
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('botsUpdate', (data) => {
            botData.bots = data;
            io.emit('botsUpdate', data);
        });

        socket.on('statsUpdate', (data) => {
            botData.stats = data;
            io.emit('statsUpdate', data);
        });

        socket.on('disconnect', () => {
            console.log('🔌 Bot saiu');
            botSocket = null;
            io.emit('botStatus', false);
        });

        return;
    }

    // --- USUÁRIO WEB ---
    console.log('🌐 Web conectou');
    socket.emit('init', {
        stats: botData.stats,
        bots: botData.bots,
        logs: botData.logs.slice(-50),
        chatDatabase: botData.chatDatabase.slice(-100),
        servers: botData.servers,
        analytics: botData.analytics
    });
    socket.emit('botStatus', botSocket !== null);

    // Comandos do frontend
    ['chat', 'command', 'announce', 'addMsg', 'delMsg', 'setIntervalo',
     'setUsername', 'connect_server', 'disconnect_server',
     'clearBlacklist', 'removeBlacklist', 'jump', 'refresh',
     'addBlacklist', 'restartBots', 'clearChatDatabase'
    ].forEach(cmd => {
        socket.on(cmd, (data) => {
            if (botSocket) botSocket.emit(cmd, data);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });

    socket.on('getChatMessages', (filter) => {
        let filtered = botData.chatDatabase;

        if (filter.serverKey && filter.serverKey !== 'all') {
            filtered = filtered.filter(m => m.serverKey === filter.serverKey);
        }

        if (filter.search) {
            const s = filter.search.toLowerCase();
            filtered = filtered.filter(m =>
                m.username.toLowerCase().includes(s) ||
                m.message.toLowerCase().includes(s)
            );
        }

        socket.emit('chatMessages', filtered.slice(-500));
    });

    socket.on('getAnalytics', (timeRange) => {
        const now = Date.now();
        let startTime = now;

        switch (timeRange) {
            case '1h': startTime = now - (60 * 60 * 1000); break;
            case '24h': startTime = now - (24 * 60 * 60 * 1000); break;
            case '7d': startTime = now - (7 * 24 * 60 * 60 * 1000); break;
            default: startTime = 0;
        }

        const filteredPlayers = botData.analytics.playersOverTime.filter(p => p.timestamp >= startTime);
        const filteredMessages = botData.chatDatabase.filter(m => m.timestamp >= startTime);

        socket.emit('analyticsData', {
            playersOverTime: filteredPlayers,
            messagesPerHour: botData.analytics.messagesPerHour,
            topServers: botData.analytics.topServers,
            totalMessages: filteredMessages.length
        });
    });
});

// Atualiza clientes a cada 5s
setInterval(() => {
    io.emit('botsUpdate', botData.bots);
    io.emit('serverUpdate', botData.servers);
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Relay na porta ${PORT}`));
