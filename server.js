// server.js - v8.0 (MEGA UPDATE - Monetização + Segurança + Features)
const express = require('express');
const http = require('http');
const crypto = require('crypto');
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
            if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) val = val.slice(1, -1);
            if (!process.env[key]) process.env[key] = val;
        });
        console.log('📄 .env carregado');
    } catch {}
}
loadEnv();

// ========== CRYPTO UTILS ==========
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY || crypto.createHash('sha256').update(process.env.SECRET_KEY || 'CRVf998@+').digest();
const IV_LENGTH = 16;

function encrypt(text) {
    const iv = crypto.randomBytes(IV_LENGTH);
    const cipher = crypto.createCipheriv('aes-256-cbc', ENCRYPTION_KEY, iv);
    let encrypted = cipher.update(String(text), 'utf8', 'hex');
    encrypted += cipher.final('hex');
    return iv.toString('hex') + ':' + encrypted;
}

function decrypt(text) {
    try {
        const parts = text.split(':');
        const iv = Buffer.from(parts.shift(), 'hex');
        const encrypted = parts.join(':');
        const decipher = crypto.createDecipheriv('aes-256-cbc', ENCRYPTION_KEY, iv);
        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    } catch { return null; }
}

function hashPassword(password) {
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
    return salt + ':' + hash;
}

function verifyPassword(password, stored) {
    const [salt, hash] = stored.split(':');
    const verify = crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
    return hash === verify;
}

function generateKey() {
    return 'KEY-' + crypto.randomBytes(16).toString('hex').toUpperCase();
}

function generateSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

// ========== CONFIG ==========
const PORT = parseInt(process.env.PORT) || 3000;
const SECRET_KEY = process.env.SECRET_KEY || 'CRVf998@+';
const WEBHOOK_CHAT = process.env.WEBHOOK_CHAT || '';
const WEBHOOK_LOGS = process.env.WEBHOOK_LOGS || '';
const GOOGLE_SHEETS_URL = process.env.GOOGLE_SHEETS_URL || '';
const SERVER_START_TIME = Date.now();
const SESSION_DURATION = 60 * 1000; // 1 minuto session

// ========== DATA DIR ==========
const DATA_DIR = path.join(__dirname, 'data');
const CHAT_PERSIST_FILE = path.join(DATA_DIR, 'chat-persist.json');
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const ADS_FILE = path.join(DATA_DIR, 'ads.json');
const PLAYERS_DB_FILE = path.join(DATA_DIR, 'players-db.json');

try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}

// ========== USERS SYSTEM ==========
let usersDB = [];

function loadUsers() {
    try {
        if (fs.existsSync(USERS_FILE)) {
            usersDB = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
        }
    } catch {}

    if (usersDB.length === 0) {
        usersDB = [
            {
                id: crypto.randomUUID(),
                username: process.env.USER1_NAME || 'CRV',
                passwordHash: hashPassword(process.env.USER1_PASS || 'CRV21'),
                role: 'admin',
                createdAt: Date.now(),
            },
            {
                id: crypto.randomUUID(),
                username: process.env.USER2_NAME || 'CRVA',
                passwordHash: hashPassword(process.env.USER2_PASS || 'CRVA11'),
                role: 'viewer',
                createdAt: Date.now(),
            }
        ];
        saveUsers();
    }
}

function saveUsers() {
    try { fs.writeFileSync(USERS_FILE, JSON.stringify(usersDB, null, 2)); } catch {}
}

loadUsers();

// ========== API KEYS SYSTEM ==========
let apiKeys = [];

function loadKeys() {
    try {
        if (fs.existsSync(KEYS_FILE)) apiKeys = JSON.parse(fs.readFileSync(KEYS_FILE, 'utf8'));
    } catch {}
}

function saveKeys() {
    try { fs.writeFileSync(KEYS_FILE, JSON.stringify(apiKeys, null, 2)); } catch {}
}

loadKeys();

// ========== ADS SYSTEM (Anúncios pagos com timer) ==========
let adsDB = [];

function loadAds() {
    try {
        if (fs.existsSync(ADS_FILE)) adsDB = JSON.parse(fs.readFileSync(ADS_FILE, 'utf8'));
    } catch {}
}

function saveAds() {
    try { fs.writeFileSync(ADS_FILE, JSON.stringify(adsDB, null, 2)); } catch {}
}

loadAds();

// Timer de anúncios - só conta quando bot online
let botOnlineTime = 0;
let lastBotOnlineCheck = null;

function updateAdTimers() {
    if (!lastBotOnlineCheck) return;
    const now = Date.now();
    const elapsed = now - lastBotOnlineCheck;
    lastBotOnlineCheck = now;

    adsDB.forEach(ad => {
        if (ad.status === 'active' && ad.remainingMs > 0) {
            ad.remainingMs -= elapsed;
            if (ad.remainingMs <= 0) {
                ad.remainingMs = 0;
                ad.status = 'expired';
                console.log(`⏰ Anúncio expirou: "${ad.message.substring(0, 40)}..."`);
                sendLogToDiscord('⏰', `Anúncio expirou: "${ad.message.substring(0, 40)}"`);
            }
        }
    });
}

setInterval(() => {
    if (botSocket) {
        if (!lastBotOnlineCheck) lastBotOnlineCheck = Date.now();
        updateAdTimers();
        saveAds();
    } else {
        lastBotOnlineCheck = null;
    }
}, 10000);

function getActiveAds() {
    return adsDB.filter(ad => ad.status === 'active' && ad.remainingMs > 0);
}

// ========== PLAYERS DATABASE ==========
let playersDB = {};

function loadPlayersDB() {
    try {
        if (fs.existsSync(PLAYERS_DB_FILE)) playersDB = JSON.parse(fs.readFileSync(PLAYERS_DB_FILE, 'utf8'));
    } catch {}
}

function savePlayersDB() {
    try {
        const keys = Object.keys(playersDB);
        if (keys.length > 50000) {
            const sorted = keys.sort((a, b) => (playersDB[b].lastSeen || 0) - (playersDB[a].lastSeen || 0));
            const newDB = {};
            sorted.slice(0, 30000).forEach(k => newDB[k] = playersDB[k]);
            playersDB = newDB;
        }
        fs.writeFileSync(PLAYERS_DB_FILE, JSON.stringify(playersDB, null, 2));
    } catch {}
}

loadPlayersDB();
setInterval(savePlayersDB, 5 * 60 * 1000);

// ========== SESSIONS ==========
const sessions = new Map(); // token -> { userId, username, role, expiresAt, keyId? }

function createSession(userId, username, role, keyId = null) {
    const token = generateSessionToken();
    sessions.set(token, {
        userId, username, role, keyId,
        expiresAt: Date.now() + SESSION_DURATION,
        createdAt: Date.now(),
    });
    return token;
}

function validateSession(token) {
    const session = sessions.get(token);
    if (!session) return null;
    if (Date.now() > session.expiresAt) {
        sessions.delete(token);
        return null;
    }
    // Renova sessão
    session.expiresAt = Date.now() + SESSION_DURATION;
    return session;
}

// Limpa sessões expiradas
setInterval(() => {
    const now = Date.now();
    sessions.forEach((s, k) => { if (now > s.expiresAt) sessions.delete(k); });
}, 30000);

// ========== CHAT PERSISTENCE ==========
let persistedChat = [];

try {
    if (fs.existsSync(CHAT_PERSIST_FILE)) {
        persistedChat = JSON.parse(fs.readFileSync(CHAT_PERSIST_FILE, 'utf8'));
        console.log(`📂 Chat persistido: ${persistedChat.length} mensagens`);
    }
} catch (err) {
    console.log('⚠️ Erro ao carregar chat persistido:', err.message);
}

function savePersistedChat() {
    try {
        if (persistedChat.length > 50000) persistedChat = persistedChat.slice(-50000);
        fs.writeFileSync(CHAT_PERSIST_FILE, JSON.stringify(persistedChat));
    } catch {}
}

setInterval(savePersistedChat, 5 * 60 * 1000);

// ========== RATE LIMITING ==========
const rateLimits = new Map();
function checkRateLimit(socketId, action, maxPerMinute = 30) {
    const key = `${socketId}:${action}`;
    const now = Date.now();
    const entry = rateLimits.get(key) || { count: 0, resetAt: now + 60000 };
    if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + 60000; }
    entry.count++;
    rateLimits.set(key, entry);
    return entry.count <= maxPerMinute;
}

setInterval(() => {
    const now = Date.now();
    rateLimits.forEach((v, k) => { if (now > v.resetAt + 60000) rateLimits.delete(k); });
}, 300000);

// ========== MESSAGE QUEUE SYSTEM ==========
class MessageQueue {
    constructor() {
        this.queues = new Map(); // serverKey -> [messages]
        this.processing = new Set();
        this.stats = { totalProcessed: 0, totalFailed: 0, totalRetries: 0 };
    }

    enqueue(serverKey, message, priority = 5) {
        if (!this.queues.has(serverKey)) this.queues.set(serverKey, []);
        const queue = this.queues.get(serverKey);
        queue.push({ message, priority, addedAt: Date.now(), retries: 0 });
        queue.sort((a, b) => a.priority - b.priority);
        if (queue.length > 100) queue.splice(100);
    }

    dequeue(serverKey) {
        const queue = this.queues.get(serverKey);
        if (!queue || queue.length === 0) return null;
        return queue.shift();
    }

    getStats() {
        let totalPending = 0;
        this.queues.forEach(q => totalPending += q.length);
        return { ...this.stats, totalPending, queues: this.queues.size };
    }
}

const messageQueue = new MessageQueue();

// ========== OBSERVABILITY (built-in) ==========
class Observability {
    constructor() {
        this.metrics = {
            messagesPerSecond: [],
            errorsPerMinute: [],
            memoryUsage: [],
            botConnections: [],
            latency: [],
        };
        this.alerts = [];
        this.healthChecks = [];

        setInterval(() => this.collect(), 10000);
        setInterval(() => this.checkAlerts(), 30000);
    }

    collect() {
        const now = Date.now();
        const mem = process.memoryUsage();

        this.metrics.memoryUsage.push({ timestamp: now, heapMB: Math.round(mem.heapUsed / 1048576), rssMB: Math.round(mem.rss / 1048576) });
        if (this.metrics.memoryUsage.length > 360) this.metrics.memoryUsage.shift();

        this.metrics.botConnections.push({ timestamp: now, connected: !!botSocket, bots: botData.bots.length });
        if (this.metrics.botConnections.length > 360) this.metrics.botConnections.shift();
    }

    recordMessage() {
        const now = Date.now();
        this.metrics.messagesPerSecond.push(now);
        this.metrics.messagesPerSecond = this.metrics.messagesPerSecond.filter(t => now - t < 60000);
    }

    recordError(type, msg) {
        this.metrics.errorsPerMinute.push({ timestamp: Date.now(), type, msg });
        if (this.metrics.errorsPerMinute.length > 1000) this.metrics.errorsPerMinute.shift();
    }

    recordLatency(ms) {
        this.metrics.latency.push({ timestamp: Date.now(), ms });
        if (this.metrics.latency.length > 500) this.metrics.latency.shift();
    }

    checkAlerts() {
        const mem = process.memoryUsage();
        const heapMB = mem.heapUsed / 1048576;

        if (heapMB > 400) {
            this.alert('warning', `RAM alta: ${Math.round(heapMB)}MB`);
        }

        const recentErrors = this.metrics.errorsPerMinute.filter(e => Date.now() - e.timestamp < 300000);
        if (recentErrors.length > 50) {
            this.alert('danger', `Muitos erros: ${recentErrors.length} nos últimos 5min`);
        }
    }

    alert(level, message) {
        const a = { level, message, timestamp: Date.now() };
        this.alerts.push(a);
        if (this.alerts.length > 100) this.alerts.shift();
        if (level === 'danger') sendLogToDiscord('🚨', message);
        io.emit('systemAlert', a);
    }

    getMetrics() {
        const now = Date.now();
        return {
            messagesPerMinute: this.metrics.messagesPerSecond.length,
            recentErrors: this.metrics.errorsPerMinute.filter(e => now - e.timestamp < 300000).length,
            memoryHistory: this.metrics.memoryUsage.slice(-60),
            botHistory: this.metrics.botConnections.slice(-60),
            latencyAvg: this.metrics.latency.length > 0 ? Math.round(this.metrics.latency.reduce((a, b) => a + b.ms, 0) / this.metrics.latency.length) : 0,
            alerts: this.alerts.slice(-20),
            queueStats: messageQueue.getStats(),
        };
    }
}

const observability = new Observability();

// ========== SELF HEALING ==========
class SelfHealing {
    constructor() {
        this.issues = [];
        this.resolutions = [];
        this.patterns = new Map();

        setInterval(() => this.diagnose(), 60000);
    }

    diagnose() {
        const mem = process.memoryUsage();
        const heapMB = mem.heapUsed / 1048576;

        // Memory leak detection
        if (heapMB > 450) {
            this.heal('memory_high', () => {
                if (global.gc) { global.gc(); this.log('GC forçado'); }
                if (botData.chatDatabase.length > 10000) {
                    botData.chatDatabase = botData.chatDatabase.slice(-5000);
                    this.log('Chat truncado para liberar memória');
                }
            });
        }

        // Stale bot detection
        botData.bots.forEach(b => {
            if (b.conectado && b.lastActivity && Date.now() - b.lastActivity > 300000) {
                this.log(`Bot potencialmente travado: ${b.key}`);
            }
        });
    }

    heal(issueType, healFn) {
        const recent = this.issues.filter(i => i.type === issueType && Date.now() - i.timestamp < 300000);
        if (recent.length > 3) return; // Já tentou demais

        try {
            healFn();
            this.resolutions.push({ type: issueType, timestamp: Date.now(), success: true });
        } catch (err) {
            this.resolutions.push({ type: issueType, timestamp: Date.now(), success: false, error: err.message });
        }
        this.issues.push({ type: issueType, timestamp: Date.now() });
        if (this.issues.length > 100) this.issues.shift();
        if (this.resolutions.length > 100) this.resolutions.shift();
    }

    log(msg) {
        console.log(`🔧 [Self-Heal] ${msg}`);
    }

    getStatus() {
        return {
            recentIssues: this.issues.slice(-20),
            recentResolutions: this.resolutions.slice(-20),
        };
    }
}

const selfHealing = new SelfHealing();

// ========== GOOGLE SHEETS ==========
const sheetsBatch = [];
let sheetsTimer = null;
let sheetsSending = false;

function sheetsEnabled() { return GOOGLE_SHEETS_URL.length > 20; }

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
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data), redirect: 'follow', signal: AbortSignal.timeout(15000),
        });
        return resp.ok;
    } catch (err) {
        if (!['AbortError', 'fetch failed'].some(f => (err.name || err.message || '').includes(f)))
            console.error('⚠️ Sheets erro:', err.message);
        return false;
    }
}

function queueChatToSheets(entry) {
    if (!sheetsEnabled()) return;
    let motd = '';
    const srv = botData.servers.find(s => s.key === entry.serverKey);
    if (srv && srv.motd) motd = cleanMotdText(srv.motd);
    sheetsBatch.push({
        data: entry.date || new Date().toLocaleDateString('pt-BR'),
        hora: entry.time || new Date().toLocaleTimeString('pt-BR', { hour12: false }),
        servidor: entry.serverKey || '', motd, jogador: entry.username || '', mensagem: entry.message || '',
    });
    if (sheetsBatch.length >= 10) flushSheetsBatch();
    else if (!sheetsTimer) sheetsTimer = setTimeout(flushSheetsBatch, 30000);
}

async function flushSheetsBatch() {
    if (sheetsTimer) { clearTimeout(sheetsTimer); sheetsTimer = null; }
    if (sheetsBatch.length === 0 || sheetsSending) return;
    sheetsSending = true;
    const batch = sheetsBatch.splice(0, sheetsBatch.length);
    const success = await sendToSheets({ type: 'batch', messages: batch });
    if (success) console.log(`📊 Sheets: ${batch.length} msgs`);
    else if (sheetsBatch.length < 500) sheetsBatch.unshift(...batch);
    sheetsSending = false;
}

function sendLogToSheets(tipo, mensagem) {
    if (!sheetsEnabled()) return;
    sendToSheets({ type: 'log', data: new Date().toLocaleDateString('pt-BR'), hora: new Date().toLocaleTimeString('pt-BR', { hour12: false }), tipo, mensagem });
}

function sendServerToSheets(serverKey, status, players, maxPlayers, version, motd) {
    if (!sheetsEnabled()) return;
    sendToSheets({ type: 'server', data: new Date().toLocaleDateString('pt-BR'), hora: new Date().toLocaleTimeString('pt-BR', { hour12: false }), ip: serverKey, status, jogadores: (players || []).length, max: maxPlayers || 0, versao: version || '?', motd: cleanMotdText(motd) });
}

setInterval(() => { if (sheetsBatch.length > 0) flushSheetsBatch(); }, 30000);

// ========== DISCORD WEBHOOKS ==========
const webhookQueue = [];
let processingWebhooks = false;

async function sendWebhook(url, data) {
    if (!url) return;
    webhookQueue.push({ url, data });
    if (webhookQueue.length > 100) webhookQueue.splice(0, webhookQueue.length - 100);
    drainWebhookQueue();
}

async function drainWebhookQueue() {
    if (processingWebhooks) return;
    processingWebhooks = true;
    while (webhookQueue.length > 0) {
        const { url, data } = webhookQueue.shift();
        try {
            const resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data), signal: AbortSignal.timeout(10000) });
            if (resp.status === 429) {
                const body = await resp.json().catch(() => ({}));
                await new Promise(r => setTimeout(r, Math.ceil((body.retry_after || 5) * 1000) + 1000));
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
        avatar_url: `https://mc-heads.net/avatar/${encodeURIComponent(username || 'Steve')}/32`,
        content: (message || '').substring(0, 2000),
        embeds: [{ color: 0x6366f1, footer: { text: `🖥️ ${serverKey}` } }]
    });
}

function sendLogToDiscord(emoji, msg) {
    if (!WEBHOOK_LOGS) return;
    sendWebhook(WEBHOOK_LOGS, { username: 'Bot Logger', content: `${emoji} ${msg}`.substring(0, 2000) });
}

// ========== EXPRESS + SOCKET.IO ==========
const app = express();
const server_http = http.createServer(app);
const io = new Server(server_http, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000, pingInterval: 25000, maxHttpBufferSize: 1e6,
});

// ========== BOT DATA ==========
let botSocket = null;
const MAX_CHAT_MESSAGES = 50000;
const MAX_LOGS = 200;

let botData = {
    stats: { botsAtivos: 0, totalBots: 0, blacklistSize: 0, blacklistItems: [], tempBlacklistItems: [], mensagens: [], intervalo: 180, username: 'Anunciador' },
    bots: [], logs: [], chatDatabase: persistedChat, servers: [],
    analytics: { playersOverTime: [], messagesPerHour: [], topServers: [] },
    vulnerabilities: [], serverHealth: {},
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
        if (was !== s.status) sendServerToSheets(s.key, s.status, s.players, s.maxPlayers, s.version, s.motd);
    });
}

// ========== ANALYTICS ==========
function updateAnalytics() {
    const now = Date.now();
    let totalPlayers = 0;
    botData.servers.forEach(s => { if (s.status === 'online' && s.players) totalPlayers += s.players.length; });
    botData.analytics.playersOverTime.push({ timestamp: now, total: totalPlayers });
    if (botData.analytics.playersOverTime.length > 720) botData.analytics.playersOverTime.shift();

    const h = new Date().getHours();
    const idx = botData.analytics.messagesPerHour.findIndex(x => x.hour === h);
    const cnt = botData.chatDatabase.filter(m => { const t = m.timestamp || 0; return new Date(t).getHours() === h && (now - t) < 86400000; }).length;
    if (idx >= 0) botData.analytics.messagesPerHour[idx].count = cnt;
    else botData.analytics.messagesPerHour.push({ hour: h, count: cnt });
    if (botData.analytics.messagesPerHour.length > 24) botData.analytics.messagesPerHour = botData.analytics.messagesPerHour.slice(-24);

    const ss = {};
    botData.chatDatabase.forEach(m => {
        if (!m.serverKey) return;
        if (!ss[m.serverKey]) ss[m.serverKey] = { messages: 0, players: new Set() };
        ss[m.serverKey].messages++;
        if (m.username) ss[m.serverKey].players.add(m.username);
    });
    botData.analytics.topServers = Object.entries(ss).map(([k, d]) => ({ serverKey: k, messages: d.messages, uniquePlayers: d.players.size })).sort((a, b) => b.messages - a.messages).slice(0, 10);
}

// ========== TIMERS ==========
const analyticsTimer = setInterval(updateAnalytics, 60000);
const serverStatusTimer = setInterval(() => { syncServerStatus(); io.emit('serverUpdate', botData.servers); }, 10000);
const botsUpdateTimer = setInterval(() => { io.emit('botsUpdate', botData.bots); }, 5000);

function processNewServer(s) {
    if (!s || !s.key) return;
    serverLastSeen[s.key] = Date.now();
    const idx = botData.servers.findIndex(x => x.key === s.key);
    if (idx >= 0) botData.servers[idx] = { ...botData.servers[idx], ...s, lastSeen: Date.now() };
    else {
        botData.servers.push({ ...s, lastSeen: Date.now() });
        sendLogToDiscord('🎯', `Novo servidor: ${s.key}`);
    }
    if (botData.servers.length > 500) {
        botData.servers.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));
        botData.servers = botData.servers.slice(0, 500);
    }
}

function getUptime() {
    const ms = Date.now() - SERVER_START_TIME;
    const s = Math.floor(ms / 1000), m = Math.floor(s / 60), h = Math.floor(m / 60), d = Math.floor(h / 24);
    if (d > 0) return `${d}d ${h % 24}h ${m % 60}m`;
    if (h > 0) return `${h}h ${m % 60}m`;
    return `${m}m ${s % 60}s`;
}

function getTopServerChat() {
    const onlineServers = botData.servers.filter(s => s.status === 'online' && s.players && s.players.length > 0).sort((a, b) => (b.players?.length || 0) - (a.players?.length || 0));
    if (onlineServers.length === 0) return { serverKey: null, motd: '', messages: [] };
    const topServer = onlineServers[0];
    return { serverKey: topServer.key, motd: cleanMotdText(topServer.motd), players: topServer.players?.length || 0, messages: botData.chatDatabase.filter(m => m.serverKey === topServer.key).slice(-50) };
}

let totalWebConnections = 0;
let totalChatMessages = 0;

// ========== ROUTES ==========
// Serve o painel web
app.get('/', (req, res) => {
    const htmlPath = path.join(__dirname, 'index.html');
    if (fs.existsSync(htmlPath)) {
        res.sendFile(htmlPath);
    } else {
        res.json({ status: 'online', version: '8.0', error: 'index.html não encontrado!' });
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        version: '8.0',
        uptime: getUptime(),
        botConectado: !!botSocket,
        bots: botData.stats.botsAtivos,
        msgs: botData.chatDatabase.length,
        servers: botData.servers.length,
        memory: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
    });
});
// ========== AUTH ==========
const authenticatedSockets = new Map(); // socketId -> { username, role, sessionToken, keyId? }

function isAuthenticated(socket) { return authenticatedSockets.has(socket.id); }
function isAdmin(socket) { const u = authenticatedSockets.get(socket.id); return u && u.role === 'admin'; }

function getInitData() {
    return {
        stats: botData.stats, bots: botData.bots, logs: botData.logs.slice(-50),
        chatDatabase: botData.chatDatabase.slice(-200), servers: botData.servers,
        analytics: botData.analytics, authRequired: true, topServerChat: getTopServerChat(),
        activeAds: getActiveAds(), playersDB: Object.keys(playersDB).length,
    };
}

// ========== SOCKET.IO ==========
io.on('connection', (socket) => {

    // === BOT ===
    if (socket.handshake.auth?.key === SECRET_KEY && socket.handshake.auth?.type === 'bot') {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        lastBotOnlineCheck = Date.now();
        io.emit('botStatus', true);
        sendLogToDiscord('🤖', 'Bot conectou!');

        socket.on('syncData', (data) => {
            if (!data) return;
            if (data.stats) botData.stats = data.stats;
            if (Array.isArray(data.bots)) botData.bots = data.bots;
            if (Array.isArray(data.logs)) botData.logs = data.logs.slice(-MAX_LOGS);
            if (Array.isArray(data.chatDatabase)) {
                const existingTs = new Set(botData.chatDatabase.map(m => `${m.timestamp}-${m.serverKey}-${m.username}`));
                const newMsgs = data.chatDatabase.filter(m => !existingTs.has(`${m.timestamp}-${m.serverKey}-${m.username}`));
                botData.chatDatabase.push(...newMsgs);
                persistedChat.push(...newMsgs);
                if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) botData.chatDatabase = botData.chatDatabase.slice(-MAX_CHAT_MESSAGES);
                if (persistedChat.length > MAX_CHAT_MESSAGES) persistedChat = persistedChat.slice(-MAX_CHAT_MESSAGES);
            }
            if (Array.isArray(data.servers)) data.servers.forEach(processNewServer);
            syncServerStatus(); updateAnalytics();
            io.emit('init', getInitData());
        });

        socket.on('log', (e) => { if (!e) return; botData.logs.push(e); if (botData.logs.length > MAX_LOGS) botData.logs.shift(); io.emit('log', e); });

        socket.on('chatMessage', (data) => {
            if (!data?.serverKey) return;
            data.id = `${data.timestamp}-${data.serverKey}-${(data.username || '').substring(0, 16)}`;
            if (!data.motd) { const srv = botData.servers.find(s => s.key === data.serverKey); if (srv) data.motd = cleanMotdText(srv.motd); }
            botData.chatDatabase.push(data); persistedChat.push(data);
            if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) botData.chatDatabase.shift();
            if (persistedChat.length > MAX_CHAT_MESSAGES) persistedChat.shift();
            totalChatMessages++;
            observability.recordMessage();
            io.emit('chatMessage', data); updateAnalytics();
            sendChatToDiscord(data.serverKey, data.username, data.message);
            queueChatToSheets(data);
            io.emit('topServerChat', getTopServerChat());

            // Update player DB
            if (data.username) {
                const pKey = data.username.toLowerCase();
                if (!playersDB[pKey]) playersDB[pKey] = { username: data.username, firstSeen: Date.now(), lastSeen: Date.now(), servers: [], messageCount: 0, messages: [], hoursActive: {}, sentiment: { positive: 0, negative: 0, neutral: 0 }, toxic: false };
                const p = playersDB[pKey];
                p.lastSeen = Date.now();
                p.messageCount++;
                if (!p.servers.includes(data.serverKey)) p.servers.push(data.serverKey);
                p.messages.push({ text: data.message, server: data.serverKey, time: data.time, timestamp: data.timestamp });
                if (p.messages.length > 200) p.messages = p.messages.slice(-200);
                const hour = new Date().getHours();
                p.hoursActive[hour] = (p.hoursActive[hour] || 0) + 1;
            }
        });

        socket.on('serverUpdate', (d) => { if (Array.isArray(d)) d.forEach(processNewServer); else if (d?.key) processNewServer(d); syncServerStatus(); io.emit('serverUpdate', botData.servers); });

        socket.on('botsUpdate', (data) => {
            if (!Array.isArray(data)) return;
            botData.bots = data;
            const now = Date.now();
            data.forEach(b => { const k = b.serverKey || b.server || b.key; if (k) serverLastSeen[k] = now; });
            syncServerStatus(); io.emit('botsUpdate', data); io.emit('serverUpdate', botData.servers);
        });

        socket.on('statsUpdate', (d) => { if (d) { botData.stats = d; io.emit('statsUpdate', d); } });

        // Forward events
        ['learningData', 'learningExport', 'reputationData', 'allReputations', 'fullChat', 'chatDownload', 'botSystemInfo', 'botConsoleLog', 'vulnerabilityReport', 'serverHealthUpdate', 'tabCompleteResult', 'playerRetentionData'].forEach(evt => {
            socket.on(evt, (data) => io.emit(evt, data));
        });

        socket.on('disconnect', (reason) => {
            console.log(`🔌 Bot saiu: ${reason}`);
            botSocket = null; lastBotOnlineCheck = null;
            io.emit('botStatus', false);
            sendLogToDiscord('🔌', `Bot desconectou: ${reason}`);
        });

        return;
    }

    // === WEB ===
    totalWebConnections++;
    socket.emit('requireAuth');

    socket.on('authenticate', ({ username, password }, callback) => {
        if (!checkRateLimit(socket.id, 'auth', 10)) {
            if (typeof callback === 'function') callback({ success: false, error: 'Rate limit' });
            return;
        }

        // Check users
        const user = usersDB.find(u => u.username === username);
        if (user && verifyPassword(password, user.passwordHash)) {
            const token = createSession(user.id, user.username, user.role);
            authenticatedSockets.set(socket.id, { username: user.username, role: user.role, sessionToken: token });
            if (typeof callback === 'function') callback({ success: true, role: user.role, username: user.username, sessionToken: token });
            syncServerStatus();
            socket.emit('init', getInitData());
            socket.emit('botStatus', !!botSocket);
            console.log(`🔓 ${user.username} autenticou (${user.role})`);
            return;
        }

        // Check API keys
        const key = apiKeys.find(k => k.key === password && k.active);
        if (key) {
            if (key.expiresAt && Date.now() > key.expiresAt) {
                key.active = false; saveKeys();
                if (typeof callback === 'function') callback({ success: false, error: 'Key expirada!' });
                return;
            }
            const token = createSession(key.id, key.label || 'Key User', 'client', key.id);
            authenticatedSockets.set(socket.id, { username: key.label || 'Key User', role: 'client', sessionToken: token, keyId: key.id });
            if (typeof callback === 'function') callback({ success: true, role: 'client', username: key.label, sessionToken: token, keyData: { label: key.label, expiresAt: key.expiresAt, message: key.adMessage, remainingMs: getKeyAdRemaining(key.id) } });
            console.log(`🔑 Key login: ${key.label}`);
            return;
        }

        if (typeof callback === 'function') callback({ success: false, error: 'Credenciais inválidas' });
    });

    // Session keep-alive
    socket.on('keepAlive', ({ sessionToken }, callback) => {
        const session = validateSession(sessionToken);
        if (session) {
            if (typeof callback === 'function') callback({ valid: true, expiresIn: session.expiresAt - Date.now() });
        } else {
            authenticatedSockets.delete(socket.id);
            socket.emit('sessionExpired');
            if (typeof callback === 'function') callback({ valid: false });
        }
    });

    // === ADMIN COMMANDS ===
    const adminCmds = [
        'chat', 'command', 'announce', 'addMsg', 'delMsg', 'setIntervalo', 'setUsername',
        'connect_server', 'disconnect_server', 'clearBlacklist', 'removeBlacklist',
        'jump', 'refresh', 'addBlacklist', 'restartBots', 'clearChatDatabase', 'saveChat',
        'addServerTag', 'setServerCategory', 'scanVulnerabilities', 'tabCompleteServer',
    ];

    adminCmds.forEach(cmd => {
        socket.on(cmd, (d) => {
            if (!isAuthenticated(socket)) { socket.emit('toast', '⛔ Não autenticado!'); return; }
            if (!isAdmin(socket)) { socket.emit('toast', '⛔ Sem permissão!'); return; }
            if (!checkRateLimit(socket.id, cmd, cmd === 'refresh' ? 10 : 20)) { socket.emit('toast', '⏳ Aguarde...'); return; }
            if (cmd === 'clearChatDatabase') { botData.chatDatabase = []; persistedChat = []; savePersistedChat(); io.emit('chatMessages', []); }
            if (botSocket) botSocket.emit(cmd, d);
            else socket.emit('toast', '⚠️ Bot offline!');
        });
    });

    // === KEY MANAGEMENT (admin only) ===
    socket.on('createKey', (data, callback) => {
        if (!isAdmin(socket)) { if (typeof callback === 'function') callback({ error: 'Sem permissão' }); return; }

        const key = {
            id: crypto.randomUUID(),
            key: generateKey(),
            label: data.label || 'Cliente',
            role: 'client',
            active: true,
            createdAt: Date.now(),
            expiresAt: data.durationHours ? Date.now() + (data.durationHours * 3600000) : null,
            adMessage: data.adMessage || '',
            adDurationMs: data.durationHours ? data.durationHours * 3600000 : 0,
            adRemainingMs: data.durationHours ? data.durationHours * 3600000 : 0,
            createdBy: authenticatedSockets.get(socket.id)?.username || 'admin',
        };

        apiKeys.push(key);
        saveKeys();

        // Add ad if message provided
        if (data.adMessage) {
            adsDB.push({
                id: crypto.randomUUID(),
                keyId: key.id,
                message: data.adMessage,
                totalMs: key.adDurationMs,
                remainingMs: key.adDurationMs,
                status: 'active',
                createdAt: Date.now(),
                label: key.label,
            });
            saveAds();

            // Sync ad messages to bot
            syncAdsToBot();
        }

        if (typeof callback === 'function') callback({ success: true, key: key.key, id: key.id });
        console.log(`🔑 Nova key criada: ${key.label} (${data.durationHours || '∞'}h)`);
    });

    socket.on('getKeys', (callback) => {
        if (!isAdmin(socket)) return;
        if (typeof callback === 'function') callback(apiKeys.map(k => ({
            ...k, key: k.key.substring(0, 8) + '...' + k.key.slice(-4),
            fullKey: k.key,
        })));
    });

    socket.on('revokeKey', (keyId, callback) => {
        if (!isAdmin(socket)) return;
        const key = apiKeys.find(k => k.id === keyId);
        if (key) {
            key.active = false;
            saveKeys();
            // Deactivate related ads
            adsDB.filter(a => a.keyId === keyId).forEach(a => a.status = 'revoked');
            saveAds();
            syncAdsToBot();
            if (typeof callback === 'function') callback({ success: true });
        }
    });

    socket.on('deleteKey', (keyId, callback) => {
        if (!isAdmin(socket)) return;
        apiKeys = apiKeys.filter(k => k.id !== keyId);
        adsDB = adsDB.filter(a => a.keyId !== keyId);
        saveKeys(); saveAds(); syncAdsToBot();
        if (typeof callback === 'function') callback({ success: true });
    });

    socket.on('getAds', (callback) => {
        if (!isAuthenticated(socket)) return;
        if (typeof callback === 'function') callback(adsDB);
    });

    // === VIEWER + ADMIN COMMANDS ===
    socket.on('getChatMessages', (f) => {
        if (!isAuthenticated(socket)) return;
        if (!checkRateLimit(socket.id, 'getChatMessages', 15)) return;
        let r = botData.chatDatabase;
        if (f?.serverKey && f.serverKey !== 'all') r = r.filter(m => m.serverKey === f.serverKey);
        if (f?.search) {
            const s = f.search.toLowerCase();
            const searchType = f.searchType || 'all';
            r = r.filter(m => {
                if (searchType === 'player') return (m.username || '').toLowerCase().includes(s);
                if (searchType === 'message') return (m.message || '').toLowerCase().includes(s);
                return (m.username || '').toLowerCase().includes(s) || (m.message || '').toLowerCase().includes(s);
            });
        }
        socket.emit('chatMessages', r.slice(-500));
    });

    socket.on('getAnalytics', (t) => {
        if (!isAuthenticated(socket)) return;
        const now = Date.now();
        let start = 0;
        if (t === '1h') start = now - 3600000;
        else if (t === '24h') start = now - 86400000;
        else if (t === '7d') start = now - 604800000;
        const peakHours = {};
        botData.chatDatabase.filter(m => m.timestamp >= start).forEach(m => {
            if (!m.serverKey) return;
            const h = new Date(m.timestamp).getHours();
            if (!peakHours[m.serverKey]) peakHours[m.serverKey] = {};
            peakHours[m.serverKey][h] = (peakHours[m.serverKey][h] || 0) + 1;
        });
        socket.emit('analyticsData', {
            playersOverTime: botData.analytics.playersOverTime.filter(p => p.timestamp >= start),
            messagesPerHour: botData.analytics.messagesPerHour,
            topServers: botData.analytics.topServers,
            totalMessages: botData.chatDatabase.filter(m => m.timestamp >= start).length,
            peakHours,
        });
    });

    socket.on('getTopServerChat', () => { if (isAuthenticated(socket)) socket.emit('topServerChat', getTopServerChat()); });

    // Learning/Reputation forwarding
    ['getLearningData', 'exportLearningDB', 'getReputation', 'getAllReputations', 'getBotSystemInfo'].forEach(evt => {
        socket.on(evt, (data) => { if (isAuthenticated(socket) && botSocket) botSocket.emit(evt, data); });
    });

    socket.on('restartBotProcess', () => {
        if (!isAdmin(socket)) { socket.emit('toast', '⛔ Sem permissão!'); return; }
        if (botSocket) { botSocket.emit('restartBotProcess'); socket.emit('toast', '🔄 Reinício enviado!'); }
        else socket.emit('toast', '⚠️ Bot offline!');
    });

    socket.on('getFullChat', () => { if (isAuthenticated(socket)) socket.emit('fullChat', botData.chatDatabase); });
    socket.on('downloadChat', () => {
        if (isAuthenticated(socket)) socket.emit('chatDownload', { exportadoEm: new Date().toISOString(), totalMensagens: botData.chatDatabase.length, mensagens: botData.chatDatabase });
    });

    // Players DB
    socket.on('getPlayersDB', (filter, callback) => {
        if (!isAuthenticated(socket)) return;
        let players = Object.values(playersDB);
        if (filter?.search) {
            const s = filter.search.toLowerCase();
            players = players.filter(p => p.username.toLowerCase().includes(s));
        }
        if (filter?.server) players = players.filter(p => p.servers.includes(filter.server));
        if (filter?.toxic) players = players.filter(p => p.toxic);
        players.sort((a, b) => b.lastSeen - a.lastSeen);
        const result = players.slice(0, 200).map(p => ({
            ...p, messages: p.messages.slice(-20),
            peakHour: Object.entries(p.hoursActive).sort((a, b) => b[1] - a[1])[0]?.[0] || '?',
        }));
        if (typeof callback === 'function') callback(result);
        else socket.emit('playersDBData', result);
    });

    socket.on('getPlayerProfile', (username, callback) => {
        if (!isAuthenticated(socket)) return;
        const p = playersDB[username.toLowerCase()];
        if (typeof callback === 'function') callback(p || null);
    });

    // Observability
    socket.on('getObservability', (callback) => {
        if (!isAuthenticated(socket)) return;
        const data = { ...observability.getMetrics(), selfHealing: selfHealing.getStatus() };
        if (typeof callback === 'function') callback(data);
        else socket.emit('observabilityData', data);
    });

    socket.on('getServerList', () => {
        if (!isAuthenticated(socket)) return;
        socket.emit('serverList', botData.servers.map(s => ({
            key: s.key, motd: cleanMotdText(s.motd), status: s.status,
            players: s.players?.length || 0, maxPlayers: s.maxPlayers || 0,
            version: s.version, categoria: s.categoria || 'desconhecido',
            tags: s.tags || [], reputation: s.reputation || 50,
        })));
    });

    socket.on('disconnect', () => authenticatedSockets.delete(socket.id));
});

// ========== SYNC ADS TO BOT ==========
function syncAdsToBot() {
    if (!botSocket) return;
    const activeAds = getActiveAds();
    botSocket.emit('updateAds', activeAds.map(a => a.message));
}

function getKeyAdRemaining(keyId) {
    const ad = adsDB.find(a => a.keyId === keyId && a.status === 'active');
    return ad ? ad.remainingMs : 0;
}

// ========== SHUTDOWN ==========
let isShuttingDown = false;

async function shutdown() {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log('\n🛑 Desligando relay...');
    
    try {
        clearInterval(analyticsTimer);
        clearInterval(serverStatusTimer);
        clearInterval(botsUpdateTimer);
        savePersistedChat();
        saveKeys();
        saveAds();
        savePlayersDB();
        await flushSheetsBatch();
        sendLogToDiscord('🛑', 'Relay desligando...');
    } catch (err) {
        console.error('Erro no shutdown:', err.message);
    }
    
    setTimeout(() => {
        try {
            io.close();
            server_http.close();
        } catch {}
        console.log('👋 Relay desligado!');
        process.exit(0);
    }, 2000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', () => {
    // Render envia SIGTERM no redeploy - só desliga se já estabilizou
    if (Date.now() - SERVER_START_TIME > 30000) {
        shutdown();
    } else {
        console.log('⏳ SIGTERM ignorado (inicializando...)');
    }
});

process.on('uncaughtException', (err) => {
    console.error('❌ FATAL:', err.message);
    observability.recordError('fatal', err.message);
});

process.on('unhandledRejection', (reason) => {
    const msg = String(reason?.message || reason || '').substring(0, 200);
    if (!['fetch failed', 'ECONNRESET', 'ETIMEDOUT'].some(f => msg.includes(f))) {
        console.error('⚠️ Unhandled:', msg);
        observability.recordError('unhandled', msg);
    }
});

// ========== START ==========
server_http.listen(PORT, '0.0.0.0', () => {
    console.log('');
    console.log('╔══════════════════════════════════════════╗');
    console.log('║          RELAY SERVER v8.0               ║');
    console.log('║     Monetização + Segurança + AI         ║');
    console.log('╠══════════════════════════════════════════╣');
    console.log(`║ 🌐 Porta: ${PORT}`);
    console.log(`║ 🔒 Usuários: ${usersDB.length}`);
    console.log(`║ 🔑 API Keys: ${apiKeys.length}`);
    console.log(`║ 📢 Anúncios ativos: ${getActiveAds().length}`);
    console.log(`║ 👥 Players DB: ${Object.keys(playersDB).length}`);
    console.log(`║ 💾 Chat persistido: ${persistedChat.length} msgs`);
    console.log(`║ 🔐 Criptografia: AES-256-CBC ✅`);
    console.log('╚══════════════════════════════════════════╝');
    console.log('');
    console.log('✅ Servidor pronto e ouvindo!');
    sendLogToDiscord('🚀', `Relay v8.0 iniciado! Porta ${PORT}`);
});