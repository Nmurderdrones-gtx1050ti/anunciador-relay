const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
});

const SECRET_KEY = 'MUDE_ESSA_CHAVE_SECRETA_123';

let botSocket = null;
let botData = {
    stats: {
        botsAtivos: 0, totalBots: 0, blacklistSize: 0,
        blacklistItems: [], mensagens: [], intervalo: 180, username: 'Anunciador',
    },
    bots: [],
    logs: [],
    chatDatabase: [],
    servers: [],
    analytics: {
        playersOverTime: [],
        messagesPerHour: [],
        topServers: [],
    }
};

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

function updateAnalytics() {
    const now = Date.now();
    let totalPlayers = 0;
    botData.servers.forEach(s => {
        if (s.status === 'online' && s.players) totalPlayers += s.players.length;
    });

    botData.analytics.playersOverTime.push({ timestamp: now, total: totalPlayers });
    if (botData.analytics.playersOverTime.length > 720) botData.analytics.playersOverTime.shift();

    const currentHour = new Date().getHours();
    const hourIdx = botData.analytics.messagesPerHour.findIndex(h => h.hour === currentHour);
    const hourCount = botData.chatDatabase.filter(m => {
        const msgHour = new Date(m.timestamp).getHours();
        return msgHour === currentHour;
    }).length;

    if (hourIdx >= 0) {
        botData.analytics.messagesPerHour[hourIdx].count = hourCount;
    } else {
        botData.analytics.messagesPerHour.push({ hour: currentHour, count: hourCount });
    }

    // Keep only 24 hours
    if (botData.analytics.messagesPerHour.length > 24) {
        botData.analytics.messagesPerHour = botData.analytics.messagesPerHour.slice(-24);
    }

    const serverStats = {};
    botData.chatDatabase.forEach(m => {
        if (!serverStats[m.serverKey]) serverStats[m.serverKey] = { messages: 0, players: new Set() };
        serverStats[m.serverKey].messages++;
        serverStats[m.serverKey].players.add(m.username);
    });
    botData.analytics.topServers = Object.entries(serverStats)
        .map(([key, data]) => ({ serverKey: key, messages: data.messages, uniquePlayers: data.players.size }))
        .sort((a, b) => b.messages - a.messages)
        .slice(0, 10);
}

setInterval(updateAnalytics, 60000);

// Sync server status with bot connections
function syncServerStatus() {
    const botKeys = new Set(botData.bots.map(b => b.serverKey || b.server));
    botData.servers.forEach(s => {
        if (botKeys.has(s.key)) {
            s.status = 'online';
        }
    });
}

io.on('connection', (socket) => {
    // Bot
    if (socket.handshake.auth?.key === SECRET_KEY && socket.handshake.auth?.type === 'bot') {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        io.emit('botStatus', true);

        socket.on('syncData', (data) => {
            if (data.stats) botData.stats = data.stats;
            if (data.bots) botData.bots = data.bots;
            if (data.logs) botData.logs = data.logs;
            if (data.chatDatabase) botData.chatDatabase = data.chatDatabase;
            if (data.servers) botData.servers = data.servers;
            syncServerStatus();
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
            // serverData can be a single server or array
            if (Array.isArray(serverData)) {
                botData.servers = serverData;
            } else {
                const idx = botData.servers.findIndex(s => s.key === serverData.key);
                if (idx >= 0) botData.servers[idx] = { ...botData.servers[idx], ...serverData };
                else botData.servers.push(serverData);
            }
            syncServerStatus();
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('botsUpdate', (data) => {
            botData.bots = data;
            syncServerStatus();
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

    // Web user
    console.log('🌐 Web conectou');
    syncServerStatus();
    socket.emit('init', {
        stats: botData.stats,
        bots: botData.bots,
        logs: botData.logs.slice(-50),
        chatDatabase: botData.chatDatabase.slice(-100),
        servers: botData.servers,
        analytics: botData.analytics
    });
    socket.emit('botStatus', botSocket !== null);

    ['chat', 'command', 'announce', 'addMsg', 'delMsg', 'setIntervalo',
     'setUsername', 'connect_server', 'disconnect_server',
     'clearBlacklist', 'removeBlacklist', 'jump', 'refresh',
     'addBlacklist', 'restartBots', 'clearChatDatabase', 'saveChat'
    ].forEach(cmd => {
        socket.on(cmd, (data) => {
            if (botSocket) botSocket.emit(cmd, data);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });

    socket.on('getChatMessages', (filter) => {
        let filtered = botData.chatDatabase;
        if (filter.serverKey && filter.serverKey !== 'all')
            filtered = filtered.filter(m => m.serverKey === filter.serverKey);
        if (filter.search) {
            const s = filter.search.toLowerCase();
            filtered = filtered.filter(m =>
                m.username.toLowerCase().includes(s) || m.message.toLowerCase().includes(s)
            );
        }
        socket.emit('chatMessages', filtered.slice(-500));
    });

    socket.on('getAnalytics', (timeRange) => {
        const now = Date.now();
        let startTime = 0;
        switch (timeRange) {
            case '1h': startTime = now - 3600000; break;
            case '24h': startTime = now - 86400000; break;
            case '7d': startTime = now - 604800000; break;
        }
        socket.emit('analyticsData', {
            playersOverTime: botData.analytics.playersOverTime.filter(p => p.timestamp >= startTime),
            messagesPerHour: botData.analytics.messagesPerHour,
            topServers: botData.analytics.topServers,
            totalMessages: botData.chatDatabase.filter(m => m.timestamp >= startTime).length
        });
    });
});

setInterval(() => {
    syncServerStatus();
    io.emit('botsUpdate', botData.bots);
    io.emit('serverUpdate', botData.servers);
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Relay na porta ${PORT}`));