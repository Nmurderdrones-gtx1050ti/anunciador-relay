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

// ===== MUDE ESSA CHAVE =====
const SECRET_KEY = 'MUDE_ESSA_CHAVE_SECRETA_123';

// ===== ESTADO =====
let botSocket = null;
let botData = {
    stats: {
        botsAtivos: 0, totalBots: 0, blacklistSize: 0,
        blacklistItems: [], mensagens: [], intervalo: 180, username: 'Anunciador',
    },
    bots: [],
    logs: [],
    chatDatabase: [], // NOVO: Database de mensagens
};

// Health check (Render precisa disso)
app.get('/', (req, res) => {
    res.json({
        status: 'online',
        botConectado: botSocket !== null,
        bots: botData.stats.botsAtivos,
        totalMensagens: botData.chatDatabase.length,
    });
});

app.get('/health', (req, res) => res.send('OK'));

// ===== SOCKET.IO =====
io.on('connection', (socket) => {

    // --- BOT DO PC ---
    if (socket.handshake.auth?.key === SECRET_KEY && socket.handshake.auth?.type === 'bot') {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        io.emit('botStatus', true);

        socket.on('syncData', (data) => {
            botData = data;
            io.emit('init', { 
                stats: botData.stats, 
                bots: botData.bots, 
                logs: botData.logs.slice(-50),
                chatDatabase: botData.chatDatabase.slice(-100) // Envia últimas 100 mensagens
            });
        });

        socket.on('log', (entry) => {
            botData.logs.push(entry);
            if (botData.logs.length > 200) botData.logs.shift();
            io.emit('log', entry);
        });

        // NOVO: Recebe mensagens do chat para o database
        socket.on('chatMessage', (data) => {
            botData.chatDatabase.push(data);
            if (botData.chatDatabase.length > 5000) botData.chatDatabase.shift(); // Mantém últimas 5000
            io.emit('chatMessage', data);
        });

        socket.on('botsUpdate', (data) => { botData.bots = data; io.emit('botsUpdate', data); });
        socket.on('statsUpdate', (data) => { botData.stats = data; io.emit('statsUpdate', data); });

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
        chatDatabase: botData.chatDatabase.slice(-100)
    });
    socket.emit('botStatus', botSocket !== null);

    // Repassa tudo pro bot
    ['chat','command','announce','addMsg','delMsg','setIntervalo',
     'setUsername','connect_server','disconnect_server',
     'clearBlacklist','removeBlacklist','jump','refresh',
     'addBlacklist','restartBots','clearChatDatabase' // NOVOS
    ].forEach(cmd => {
        socket.on(cmd, (data) => {
            if (botSocket) botSocket.emit(cmd, data);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });

    // NOVO: Filtrar chat database
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
        
        socket.emit('chatMessages', filtered.slice(-500)); // Últimas 500 filtradas
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Relay na porta ${PORT}`));
