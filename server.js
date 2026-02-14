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
};

// Health check (Render precisa disso)
app.get('/', (req, res) => {
    res.json({
        status: 'online',
        botConectado: botSocket !== null,
        bots: botData.stats.botsAtivos,
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
            io.emit('init', { stats: botData.stats, bots: botData.bots, logs: botData.logs.slice(-50) });
        });

        socket.on('log', (entry) => {
            botData.logs.push(entry);
            if (botData.logs.length > 200) botData.logs.shift();
            io.emit('log', entry);
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
    socket.emit('init', { stats: botData.stats, bots: botData.bots, logs: botData.logs.slice(-50) });
    socket.emit('botStatus', botSocket !== null);

    // Repassa tudo pro bot
    ['chat','command','announce','addMsg','delMsg','setIntervalo',
     'setUsername','connect_server','disconnect_server',
     'clearBlacklist','removeBlacklist','jump','refresh'
    ].forEach(cmd => {
        socket.on(cmd, (data) => {
            if (botSocket) botSocket.emit(cmd, data);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Relay na porta ${PORT}`));