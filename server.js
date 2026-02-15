// server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

// ========== DOTENV MANUAL ==========
function loadEnv() {
    try {
        const envPath = path.join(__dirname, '.env');
        if (!fs.existsSync(envPath)) return;
        const lines = fs.readFileSync(envPath, 'utf8').split('\n');
        lines.forEach(line => {
            line = line.trim();
            if (!line || line.startsWith('#')) return;
            const eqIdx = line.indexOf('=');
            if (eqIdx === -1) return;
            const key = line.substring(0, eqIdx).trim();
            let val = line.substring(eqIdx + 1).trim();
            if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
                val = val.slice(1, -1);
            }
            if (!process.env[key]) process.env[key] = val;
        });
        console.log('📄 .env carregado');
    } catch {}
}
loadEnv();

// ========== CONFIG ==========
const PORT = parseInt(process.env.PORT) || 3000;
const SECRET_KEY = process.env.SECRET_KEY || 'MUDE_ESSA_CHAVE_SECRETA_123';
const WEB_PASSWORD = process.env.WEB_PASSWORD || '';
const WEBHOOK_CHAT = process.env.WEBHOOK_CHAT || '';
const WEBHOOK_LOGS = process.env.WEBHOOK_LOGS || '';
const GOOGLE_SHEETS_URL = process.env.GOOGLE_SHEETS_URL || '';
const SERVER_START_TIME = Date.now();

// ========== GOOGLE SHEETS ==========
const sheetsBatch = [];
let sheetsTimer = null;
let sheetsSending = false;

function sheetsEnabled() {
    return GOOGLE_SHEETS_URL.length > 20;
}

function cleanMotdText(motd) {
    if (!motd) return '';
    if (typeof motd === 'object') {
        if (motd.text !== undefined) {
            let r = cleanMotdText(motd.text);
            if (motd.extra) r += motd.extra.map(e => cleanMotdText(e)).join('');
            return r;
        }
        return motd.translate || '';
    }
    return String(motd).replace(/§[0-9a-fk-or]/gi, '').trim();
}

async function sendToSheets(data) {
    if (!sheetsEnabled()) return false;
    try {
        const resp = await fetch(GOOGLE_SHEETS_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
            redirect: 'follow',
        });
        return resp.ok;
    } catch (err) {
        console.error('⚠️ Sheets erro:', err.message);
        return false;
    }
}

function queueChatToSheets(entry) {
    if (!sheetsEnabled()) return;

    // Busca MOTD do servidor
    let motd = '';
    const srv = botData.servers.find(s => s.key === entry.serverKey);
    if (srv && srv.motd) {
        motd = cleanMotdText(srv.motd);
    }

    sheetsBatch.push({
        data: entry.date || new Date().toLocaleDateString('pt-BR'),
        hora: entry.time || new Date().toLocaleTimeString('pt-BR', { hour12: false }),
        servidor: entry.serverKey || '',
        motd: motd,
        jogador: entry.username || '',
        mensagem: entry.message || '',
    });

    if (sheetsBatch.length >= 10) {
        flushSheetsBatch();
    } else if (!sheetsTimer) {
        sheetsTimer = setTimeout(flushSheetsBatch, 30000);
    }
}

async function flushSheetsBatch() {
    if (sheetsTimer) { clearTimeout(sheetsTimer); sheetsTimer = null; }
    if (sheetsBatch.length === 0 || sheetsSending) return;

    sheetsSending = true;
    const batch = sheetsBatch.splice(0, sheetsBatch.length);

    const success = await sendToSheets({
        type: 'batch',
        messages: batch
    });

    if (success) {
        console.log(`📊 Sheets: ${batch.length} msgs salvas`);
    } else {
        if (sheetsBatch.length < 500) {
            sheetsBatch.unshift(...batch);
        }
        console.error(`❌ Sheets: falhou, ${batch.length} msgs pendentes`);
    }

    sheetsSending = false;
}

function sendLogToSheets(tipo, mensagem) {
    if (!sheetsEnabled()) return;
    sendToSheets({
        type: 'log',
        data: new Date().toLocaleDateString('pt-BR'),
        hora: new Date().toLocaleTimeString('pt-BR', { hour12: false }),
        tipo,
        mensagem,
    });
}

function sendServerToSheets(serverKey, status, players, maxPlayers, version, motd) {
    if (!sheetsEnabled()) return;
    sendToSheets({
        type: 'server',
        data: new Date().toLocaleDateString('pt-BR'),
        hora: new Date().toLocaleTimeString('pt-BR', { hour12: false }),
        ip: serverKey,
        status,
        jogadores: (players || []).length,
        max: maxPlayers || 0,
        versao: version || '?',
        motd: cleanMotdText(motd),
    });
}

// Flush periódico
setInterval(() => {
    if (sheetsBatch.length > 0) flushSheetsBatch();
}, 30000);

// ========== DISCORD WEBHOOKS (opcional) ==========
const webhookQueue = [];
let processingWebhooks = false;

async function sendWebhook(url, data) {
    if (!url) return;
    webhookQueue.push({ url, data });
    drainWebhookQueue();
}

async function drainWebhookQueue() {
    if (processingWebhooks) return;
    processingWebhooks = true;
    while (webhookQueue.length > 0) {
        const { url, data } = webhookQueue.shift();
        try {
            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
            if (resp.status === 429) {
                const body = await resp.json().catch(() => ({}));
                const wait = Math.ceil((body.retry_after || 5) * 1000);
                await new Promise(r => setTimeout(r, wait + 1000));
                webhookQueue.unshift({ url, data });
            }
        } catch {}
        await new Promise(r => setTimeout(r, 2000));
    }
    processingWebhooks = false;
}

function sendChatToDiscord(serverKey, username, message) {
    if (!WEBHOOK_CHAT) return;
    sendWebhook(WEBHOOK_CHAT, {
        username: username || 'Unknown',
        avatar_url: `https://mc-heads.net/avatar/${username}/32`,
        content: message.substring(0, 2000),
        embeds: [{ color: 0x6366f1, footer: { text: `🖥️ ${serverKey}` } }]
    });
}

function sendLogToDiscord(emoji, msg) {
    if (!WEBHOOK_LOGS) return;
    sendWebhook(WEBHOOK_LOGS, {
        username: 'Bot Logger',
        content: `${emoji} ${msg}`.substring(0, 2000)
    });
}

// ========== VALIDAÇÃO ==========
function validateConfig() {
    const warnings = [];
    if (SECRET_KEY === 'MUDE_ESSA_CHAVE_SECRETA_123') warnings.push('SECRET_KEY padrão!');
    if (!WEB_PASSWORD) warnings.push('WEB_PASSWORD não definido!');
    if (!WEBHOOK_CHAT) warnings.push('WEBHOOK_CHAT não definido');
    if (!WEBHOOK_LOGS) warnings.push('WEBHOOK_LOGS não definido');
    if (!GOOGLE_SHEETS_URL) warnings.push('GOOGLE_SHEETS_URL não definido');
    return warnings;
}

// ========== EXPRESS + SOCKET.IO ==========
const app = express();
const server_http = http.createServer(app);
const io = new Server(server_http, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6,
});

// ========== BOT DATA ==========
let botSocket = null;
const MAX_CHAT_MESSAGES = 5000;
const MAX_LOGS = 200;

let botData = {
    stats: {
        botsAtivos: 0, totalBots: 0, blacklistSize: 0,
        blacklistItems: [], mensagens: [],
        intervalo: 180, username: 'Anunciador'
    },
    bots: [],
    logs: [],
    chatDatabase: [],
    servers: [],
    analytics: {
        playersOverTime: [],
        messagesPerHour: [],
        topServers: []
    }
};

const serverLastSeen = {};

// ========== SERVER STATUS ==========
function syncServerStatus() {
    const now = Date.now();
    const TIMEOUT = 120000;
    const botKeys = new Set(botData.bots.map(b => b.serverKey || b.server || b.key));

    botData.servers.forEach(s => {
        const lastSeen = serverLastSeen[s.key] || 0;
        const hasBot = botKeys.has(s.key);
        const recent = (now - lastSeen) < TIMEOUT;
        const hasPlayers = s.players && s.players.length > 0;
        const was = s.status;

        s.status = (hasBot || recent || hasPlayers) ? 'online' : 'offline';

        if (was !== s.status) {
            sendServerToSheets(s.key, s.status, s.players, s.maxPlayers, s.version, s.motd);
        }
    });
}

// ========== ANALYTICS ==========
function updateAnalytics() {
    const now = Date.now();
    let totalPlayers = 0;
    botData.servers.forEach(s => {
        if (s.status === 'online' && s.players) totalPlayers += s.players.length;
    });

    botData.analytics.playersOverTime.push({ timestamp: now, total: totalPlayers });
    if (botData.analytics.playersOverTime.length > 720) botData.analytics.playersOverTime.shift();

    const h = new Date().getHours();
    const idx = botData.analytics.messagesPerHour.findIndex(x => x.hour === h);
    const cnt = botData.chatDatabase.filter(m => new Date(m.timestamp).getHours() === h).length;
    if (idx >= 0) botData.analytics.messagesPerHour[idx].count = cnt;
    else botData.analytics.messagesPerHour.push({ hour: h, count: cnt });
    if (botData.analytics.messagesPerHour.length > 24) {
        botData.analytics.messagesPerHour = botData.analytics.messagesPerHour.slice(-24);
    }

    const ss = {};
    botData.chatDatabase.forEach(m => {
        if (!m.serverKey) return;
        if (!ss[m.serverKey]) ss[m.serverKey] = { messages: 0, players: new Set() };
        ss[m.serverKey].messages++;
        if (m.username) ss[m.serverKey].players.add(m.username);
    });
    botData.analytics.topServers = Object.entries(ss)
        .map(([k, d]) => ({ serverKey: k, messages: d.messages, uniquePlayers: d.players.size }))
        .sort((a, b) => b.messages - a.messages)
        .slice(0, 10);
}

// ========== TIMERS ==========
const analyticsTimer = setInterval(updateAnalytics, 60000);
const serverStatusTimer = setInterval(() => { syncServerStatus(); io.emit('serverUpdate', botData.servers); }, 10000);
const botsUpdateTimer = setInterval(() => { io.emit('botsUpdate', botData.bots); }, 5000);

// ========== PROCESS SERVER ==========
function processNewServer(s) {
    if (!s || !s.key) return;
    serverLastSeen[s.key] = Date.now();
    const idx = botData.servers.findIndex(x => x.key === s.key);
    if (idx >= 0) botData.servers[idx] = { ...botData.servers[idx], ...s };
    else {
        botData.servers.push(s);
        sendLogToDiscord('🎯', `Novo servidor: ${s.key}`);
        sendLogToSheets('servidor', `Novo: ${s.key}`);
    }
}

// ========== UPTIME ==========
function getUptime() {
    const ms = Date.now() - SERVER_START_TIME;
    const s = Math.floor(ms / 1000);
    const m = Math.floor(s / 60);
    const h = Math.floor(m / 60);
    const d = Math.floor(h / 24);
    if (d > 0) return `${d}d ${h % 24}h ${m % 60}m`;
    if (h > 0) return `${h}h ${m % 60}m`;
    return `${m}m ${s % 60}s`;
}

let totalWebConnections = 0;
let totalChatMessages = 0;

// ========== ROUTES ==========
app.get('/', (req, res) => res.json({
    status: 'online',
    uptime: getUptime(),
    botConectado: !!botSocket,
    bots: botData.stats.botsAtivos,
    msgs: botData.chatDatabase.length,
    totalMsgs: totalChatMessages,
    servers: botData.servers.length,
    webhooks: { chat: WEBHOOK_CHAT ? '✅' : '❌', logs: WEBHOOK_LOGS ? '✅' : '❌' },
    sheets: sheetsEnabled() ? '✅' : '❌',
    sheetsPending: sheetsBatch.length,
    memory: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
}));

app.get('/health', (req, res) => res.json({ status: 'ok', uptime: getUptime() }));

// ========== WEB AUTH ==========
const authenticatedSockets = new Set();
function isAuthenticated(socket) { return !WEB_PASSWORD || authenticatedSockets.has(socket.id); }

function getInitData() {
    return {
        stats: botData.stats, bots: botData.bots,
        logs: botData.logs.slice(-50),
        chatDatabase: botData.chatDatabase.slice(-200),
        servers: botData.servers,
        analytics: botData.analytics,
        authRequired: !!WEB_PASSWORD,
    };
}

// ========== SOCKET.IO ==========
io.on('connection', (socket) => {

    // === BOT ===
    if (socket.handshake.auth?.key === SECRET_KEY && socket.handshake.auth?.type === 'bot') {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        io.emit('botStatus', true);
        sendLogToDiscord('🤖', 'Bot conectou!');
        sendLogToSheets('bot', 'Conectou ao relay');

        socket.on('syncData', (data) => {
            if (!data) return;
            if (data.stats) botData.stats = data.stats;
            if (Array.isArray(data.bots)) botData.bots = data.bots;
            if (Array.isArray(data.logs)) botData.logs = data.logs.slice(-MAX_LOGS);
            if (Array.isArray(data.chatDatabase)) {
                const existing = new Set(botData.chatDatabase.map(m => m.timestamp));
                const newMsgs = data.chatDatabase.filter(m => !existing.has(m.timestamp));
                botData.chatDatabase.push(...newMsgs);
                if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) {
                    botData.chatDatabase = botData.chatDatabase.slice(-MAX_CHAT_MESSAGES);
                }
            }
            if (Array.isArray(data.servers)) data.servers.forEach(processNewServer);
            syncServerStatus();
            updateAnalytics();
            io.emit('init', getInitData());
        });

        socket.on('log', (e) => {
            if (!e) return;
            botData.logs.push(e);
            if (botData.logs.length > MAX_LOGS) botData.logs.shift();
            io.emit('log', e);
        });

        socket.on('chatMessage', (data) => {
            if (!data?.serverKey) return;
            botData.chatDatabase.push(data);
            if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) botData.chatDatabase.shift();
            totalChatMessages++;
            io.emit('chatMessage', data);
            updateAnalytics();
            sendChatToDiscord(data.serverKey, data.username, data.message);
            queueChatToSheets(data);
        });

        socket.on('serverUpdate', (d) => {
            if (Array.isArray(d)) d.forEach(processNewServer);
            else if (d?.key) processNewServer(d);
            syncServerStatus();
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('botsUpdate', (data) => {
            if (!Array.isArray(data)) return;
            botData.bots = data;
            const now = Date.now();
            data.forEach(b => { const k = b.serverKey || b.server || b.key; if (k) serverLastSeen[k] = now; });
            syncServerStatus();
            io.emit('botsUpdate', data);
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('statsUpdate', (d) => { if (d) { botData.stats = d; io.emit('statsUpdate', d); } });

        socket.on('disconnect', (reason) => {
            console.log(`🔌 Bot saiu: ${reason}`);
            botSocket = null;
            io.emit('botStatus', false);
            sendLogToDiscord('🔌', `Bot desconectou: ${reason}`);
            sendLogToSheets('bot', `Desconectou: ${reason}`);
        });

        return;
    }

    // === WEB ===
    totalWebConnections++;
    console.log(`🌐 Web conectou (#${totalWebConnections})`);

    if (WEB_PASSWORD) socket.emit('requireAuth');

    socket.on('authenticate', (password, callback) => {
        const ok = !WEB_PASSWORD || password === WEB_PASSWORD;
        if (ok) {
            authenticatedSockets.add(socket.id);
            if (typeof callback === 'function') callback({ success: true });
            syncServerStatus();
            socket.emit('init', getInitData());
            socket.emit('botStatus', !!botSocket);
        } else {
            if (typeof callback === 'function') callback({ success: false });
        }
    });

    if (!WEB_PASSWORD) {
        authenticatedSockets.add(socket.id);
        syncServerStatus();
        socket.emit('init', getInitData());
        socket.emit('botStatus', !!botSocket);
    }

    const cmds = [
        'chat', 'command', 'announce', 'addMsg', 'delMsg',
        'setIntervalo', 'setUsername', 'connect_server',
        'disconnect_server', 'clearBlacklist', 'removeBlacklist',
        'jump', 'refresh', 'addBlacklist', 'restartBots',
        'clearChatDatabase', 'saveChat'
    ];

    cmds.forEach(cmd => {
        socket.on(cmd, (d) => {
            if (!isAuthenticated(socket)) { socket.emit('toast', '⛔ Não autenticado!'); return; }
            if (botSocket) botSocket.emit(cmd, d);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });

    socket.on('getChatMessages', (f) => {
        if (!isAuthenticated(socket)) return;
        let r = botData.chatDatabase;
        if (f?.serverKey && f.serverKey !== 'all') r = r.filter(m => m.serverKey === f.serverKey);
        if (f?.search) { const s = f.search.toLowerCase(); r = r.filter(m => (m.username || '').toLowerCase().includes(s) || (m.message || '').toLowerCase().includes(s)); }
        socket.emit('chatMessages', r.slice(-500));
    });

    socket.on('getAnalytics', (t) => {
        if (!isAuthenticated(socket)) return;
        const now = Date.now();
        let start = 0;
        if (t === '1h') start = now - 3600000;
        else if (t === '24h') start = now - 86400000;
        else if (t === '7d') start = now - 604800000;
        socket.emit('analyticsData', {
            playersOverTime: botData.analytics.playersOverTime.filter(p => p.timestamp >= start),
            messagesPerHour: botData.analytics.messagesPerHour,
            topServers: botData.analytics.topServers,
            totalMessages: botData.chatDatabase.filter(m => m.timestamp >= start).length
        });
    });

    socket.on('disconnect', () => authenticatedSockets.delete(socket.id));
});

// ========== SHUTDOWN ==========
async function shutdown() {
    console.log('\n🛑 Desligando relay...');
    clearInterval(analyticsTimer);
    clearInterval(serverStatusTimer);
    clearInterval(botsUpdateTimer);
    await flushSheetsBatch();
    sendLogToDiscord('🛑', 'Relay desligando...');
    sendLogToSheets('relay', 'Desligando');
    await new Promise(r => setTimeout(r, 3000));
    io.close();
    server_http.close();
    console.log('👋 Relay desligado!');
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('uncaughtException', (err) => console.error('❌ FATAL:', err.message));
process.on('unhandledRejection', (reason) => {
    const msg = String(reason?.message || reason || '').substring(0, 200);
    if (!['fetch failed', 'ECONNRESET', 'ETIMEDOUT'].some(f => msg.includes(f))) {
        console.error('⚠️ Unhandled:', msg);
    }
});

// ========== START ==========
server_http.listen(PORT, () => {
    const warnings = validateConfig();
    console.log('');
    console.log('╔══════════════════════════════════════════╗');
    console.log('║          RELAY SERVER v5.0               ║');
    console.log('╠══════════════════════════════════════════╣');
    console.log(`║ 🌐 Porta: ${PORT}`);
    console.log(`║ 🔒 Auth: ${WEB_PASSWORD ? '✅' : '⚠️'}`);
    console.log(`║ 💬 Webhook Chat: ${WEBHOOK_CHAT ? '✅' : '❌'}`);
    console.log(`║ 📋 Webhook Logs: ${WEBHOOK_LOGS ? '✅' : '❌'}`);
    console.log(`║ 📊 Google Sheets: ${sheetsEnabled() ? '✅' : '❌'}`);
    console.log('╚══════════════════════════════════════════╝');
    if (warnings.length > 0) { console.log(''); warnings.forEach(w => console.log(`   ⚠️ ${w}`)); }
    console.log('');
    sendLogToDiscord('🚀', `Relay iniciado! Porta ${PORT}`);
    sendLogToSheets('relay', `Iniciado na porta ${PORT}`);
});