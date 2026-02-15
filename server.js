// server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// ========== DOTENV MANUAL (sem dependência) ==========
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

// ========== CONFIGURAÇÃO (tudo via ENV) ==========
const PORT = parseInt(process.env.PORT) || 3000;
const SECRET_KEY = process.env.SECRET_KEY || 'MUDE_ESSA_CHAVE_SECRETA_123';
const WEB_PASSWORD = process.env.WEB_PASSWORD || '';

const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || '';
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID || '';
const DISCORD_CATEGORY_ID = process.env.DISCORD_CATEGORY_ID || '';

const DISCORD_API = 'https://discord.com/api/v10';
const DISCORD_ENABLED = DISCORD_BOT_TOKEN.length > 50;

const SERVER_START_TIME = Date.now();

// ========== VALIDAÇÃO DE CONFIG ==========
function validateConfig() {
    const warnings = [];
    if (SECRET_KEY === 'MUDE_ESSA_CHAVE_SECRETA_123') {
        warnings.push('SECRET_KEY padrão! Defina uma chave forte no .env');
    }
    if (!WEB_PASSWORD) {
        warnings.push('WEB_PASSWORD não definido! Painel sem proteção');
    }
    if (DISCORD_ENABLED && !DISCORD_GUILD_ID) {
        warnings.push('DISCORD_GUILD_ID não definido!');
    }
    if (DISCORD_ENABLED && !DISCORD_CATEGORY_ID) {
        warnings.push('DISCORD_CATEGORY_ID não definido!');
    }
    return warnings;
}

// ========== EXPRESS + SOCKET.IO ==========
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6, // 1MB max por mensagem
});

// ========== DISCORD STATE ==========
const discordChannels = {};
const channelCreationPromises = {};
const failedAttempts = {};
const MAX_FAILURES = 3;
let discordReady = false;

// ========== DISCORD QUEUE ==========
const discordQueue = [];
let processingDiscord = false;

function queueDiscord(fn, priority = false) {
    if (!DISCORD_ENABLED || !discordReady) return Promise.resolve(null);
    return new Promise((resolve) => {
        const item = { fn, resolve };
        if (priority) discordQueue.unshift(item);
        else discordQueue.push(item);
        drainDiscordQueue();
    });
}

async function drainDiscordQueue() {
    if (processingDiscord) return;
    processingDiscord = true;
    while (discordQueue.length > 0) {
        const { fn, resolve } = discordQueue.shift();
        try {
            const result = await fn();
            resolve(result);
        } catch (err) {
            console.error('❌ Discord queue error:', err.message);
            resolve(null);
        }
        await sleep(1500);
    }
    processingDiscord = false;
}

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

// ========== DISCORD FETCH (com retry robusto) ==========
async function discordFetch(endpoint, method = 'GET', body = null, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const options = {
                method,
                headers: {
                    'Authorization': `Bot ${DISCORD_BOT_TOKEN}`,
                    'Content-Type': 'application/json',
                },
            };
            if (body) options.body = JSON.stringify(body);

            const response = await fetch(`${DISCORD_API}${endpoint}`, options);

            // Rate limit
            if (response.status === 429) {
                let retryAfter = 30000;
                try {
                    const data = await response.json();
                    retryAfter = Math.ceil((data.retry_after || 10) * 1000);
                } catch {
                    try { await response.text(); } catch {}
                }
                console.log(`⏳ Rate limit em ${endpoint}, esperando ${retryAfter + 2000}ms... (tentativa ${attempt}/${retries})`);
                await sleep(retryAfter + 2000);
                continue; // Retry
            }

            // Erros fatais (não retry)
            if (response.status === 401) {
                console.error('❌ Discord token inválido!');
                return null;
            }
            if (response.status === 403) {
                console.error(`❌ Discord sem permissão: ${endpoint}`);
                return null;
            }
            if (response.status === 404) return null;
            if (response.status === 204) return {};

            const contentType = response.headers.get('content-type') || '';
            if (!contentType.includes('json')) return null;

            if (!response.ok) {
                const errText = await response.text();
                console.error(`❌ Discord ${response.status}:`, errText.substring(0, 100));
                if (attempt < retries) {
                    await sleep(10000 * attempt);
                    continue;
                }
                return null;
            }

            return await response.json();
        } catch (err) {
            console.error(`❌ Discord fetch error (tentativa ${attempt}):`, err.message);
            if (attempt < retries) {
                await sleep(3000 * attempt);
                continue;
            }
            return null;
        }
    }
    return null;
}

// ========== DISCORD INIT ==========
async function initDiscord() {
    if (!DISCORD_ENABLED) {
        console.log('⚠️ Discord não configurado (sem DISCORD_BOT_TOKEN)');
        return;
    }

    if (!DISCORD_GUILD_ID || !DISCORD_CATEGORY_ID) {
        console.error('❌ Discord: DISCORD_GUILD_ID e DISCORD_CATEGORY_ID são obrigatórios!');
        return;
    }

    console.log('🔍 Iniciando Discord em 5s...');
    await sleep(30000);

    // Testa autenticação
    let me = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
        me = await discordFetch('/users/@me');
        if (me) break;
        console.log(`⚠️ Discord tentativa ${attempt}/3 falhou...`);
        await sleep(30000 * attempt);
    }

    if (!me) {
        console.error('❌ Discord: falhou após 3 tentativas!');
        console.log('🔄 Tentando novamente em 120s...');
        setTimeout(initDiscord, 120000);
        return;
    }
    console.log(`✅ Discord Bot: ${me.username} (${me.id})`);

    await sleep(2000);

    // Testa acesso ao servidor
    const guild = await discordFetch(`/guilds/${DISCORD_GUILD_ID}`);
    if (!guild) {
        console.error('❌ Discord: servidor não encontrado!');
        console.log('🔄 Tentando novamente em 120s...');
        setTimeout(initDiscord, 120000);
        return;
    }
    console.log(`✅ Discord Servidor: ${guild.name}`);

    await sleep(2000);

    // Lista canais existentes
    const channelList = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`);
    if (!channelList || !Array.isArray(channelList)) {
        console.error('❌ Discord: não listou canais!');
        console.log('🔄 Tentando novamente em 120s...');
        setTimeout(initDiscord, 120000);
        return;
    }

    const category = channelList.find(ch => ch.id === DISCORD_CATEGORY_ID);
    if (!category) {
        console.error('❌ Discord: categoria não encontrada!');
        return;
    }
    console.log(`✅ Discord Categoria: ${category.name}`);

    // Cache dos canais existentes
    let cached = 0;
    channelList
        .filter(ch => ch.parent_id === DISCORD_CATEGORY_ID && ch.type === 0)
        .forEach(ch => {
            if (ch.topic) {
                const match = ch.topic.match(/(\d+\.\d+\.\d+\.\d+:\d+)/);
                if (match) {
                    discordChannels[match[1]] = {
                        channelId: ch.id,
                        channelName: ch.name,
                        webhookId: null,
                        webhookToken: null,
                    };
                    cached++;
                }
            }
        });

    discordReady = true;
    console.log(`🎮 Discord pronto! (${cached} canais em cache)`);

    // Carrega webhooks dos canais existentes
    for (const key of Object.keys(discordChannels)) {
        const info = discordChannels[key];
        queueDiscord(async () => {
            const webhooks = await discordFetch(`/channels/${info.channelId}/webhooks`);
            if (webhooks && Array.isArray(webhooks) && webhooks.length > 0) {
                const wh = webhooks.find(w => w.name === 'MC Chat') || webhooks[0];
                info.webhookId = wh.id;
                info.webhookToken = wh.token;
                console.log(`🔗 Webhook carregado: ${key}`);
            }
        });
    }
}

// ========== CHANNEL HELPERS ==========
function cleanMotd(motd) {
    if (!motd) return 'sem-motd';
    if (typeof motd === 'object') {
        if (motd.text !== undefined) {
            let r = cleanMotd(motd.text);
            if (motd.extra) r += motd.extra.map(e => cleanMotd(e)).join('');
            return r;
        }
        return motd.translate || 'servidor';
    }
    return String(motd).replace(/§[0-9a-fk-or]/gi, '').trim() || 'sem-motd';
}

function sanitizeChannelName(name) {
    return name
        .toLowerCase()
        .replace(/[^a-z0-9\s-]/g, '')
        .replace(/\s+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '')
        .substring(0, 100) || 'servidor';
}

// ========== GET OR CREATE CHANNEL (sem race condition) ==========
async function getOrCreateChannel(serverKey, motd) {
    // Já existe em cache
    if (discordChannels[serverKey]?.channelId) {
        return discordChannels[serverKey];
    }
    if (!discordReady) return null;
    if ((failedAttempts[serverKey] || 0) >= MAX_FAILURES) return null;

    // Se já tem uma promise pendente, aguarda ela (evita duplicata)
    if (channelCreationPromises[serverKey]) {
        return channelCreationPromises[serverKey];
    }

    channelCreationPromises[serverKey] = (async () => {
        try {
            const result = await queueDiscord(async () => {
                const motdClean = cleanMotd(motd);
                const ipFmt = serverKey.replace(/[.:]/g, '-');
                const name = sanitizeChannelName(`${motdClean}-${ipFmt}`);

                console.log(`📝 Criando canal: #${name} (${serverKey})`);

                const channel = await discordFetch(
                    `/guilds/${DISCORD_GUILD_ID}/channels`,
                    'POST',
                    {
                        name,
                        type: 0,
                        parent_id: DISCORD_CATEGORY_ID,
                        topic: `📡 MC: ${serverKey} | ${motdClean}`,
                    }
                );

                if (!channel?.id) {
                    failedAttempts[serverKey] = (failedAttempts[serverKey] || 0) + 1;
                    console.error(`❌ Falha criar canal ${serverKey} (tentativa ${failedAttempts[serverKey]})`);
                    return null;
                }

                console.log(`✅ Canal criado: #${channel.name}`);
                await sleep(1500);

                // Criar webhook
                let webhook = null;
                try {
                    webhook = await discordFetch(
                        `/channels/${channel.id}/webhooks`,
                        'POST',
                        { name: 'MC Chat' }
                    );
                } catch {}

                const info = {
                    channelId: channel.id,
                    channelName: channel.name,
                    webhookId: webhook?.id || null,
                    webhookToken: webhook?.token || null,
                };

                discordChannels[serverKey] = info;
                failedAttempts[serverKey] = 0;

                // Mensagem de boas-vindas
                queueDiscord(async () => {
                    await discordFetch(`/channels/${channel.id}/messages`, 'POST', {
                        embeds: [{
                            title: `📡 ${motdClean}`,
                            description: 'Servidor detectado!',
                            color: 0x6366f1,
                            fields: [
                                { name: '🌐 IP', value: `\`${serverKey}\``, inline: true }
                            ],
                            footer: { text: 'Central Radmin VPN' },
                            timestamp: new Date().toISOString(),
                        }]
                    });
                });

                return info;
            }, true);

            return result;
        } finally {
            delete channelCreationPromises[serverKey];
        }
    })();

    return channelCreationPromises[serverKey];
}

// ========== SEND CHAT TO DISCORD ==========
async function sendMessageViaDiscord(info, username, message) {
    // Tenta webhook primeiro (mais bonito)
    if (info.webhookId && info.webhookToken) {
        const skin = `https://mc-heads.net/avatar/${username}/32`;
        try {
            const resp = await fetch(
                `${DISCORD_API}/webhooks/${info.webhookId}/${info.webhookToken}`,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        username,
                        avatar_url: skin,
                        content: message.substring(0, 2000),
                    }),
                }
            );
            if (resp.ok || resp.status === 204) return;

            // Webhook pode ter sido deletado
            if (resp.status === 404) {
                info.webhookId = null;
                info.webhookToken = null;
                console.log(`⚠️ Webhook removido, usando mensagem normal`);
            }
        } catch {}
    }

    // Fallback: mensagem normal
    await discordFetch(`/channels/${info.channelId}/messages`, 'POST', {
        content: `**${username}** » ${message.substring(0, 1900)}`,
    });
}

function sendChatToDiscord(serverKey, motd, username, message) {
    if (!discordReady) return;

    const info = discordChannels[serverKey];

    if (info?.channelId) {
        queueDiscord(() => sendMessageViaDiscord(info, username, message));
    } else {
        getOrCreateChannel(serverKey, motd)
            .then((channelInfo) => {
                if (channelInfo?.channelId) {
                    queueDiscord(() =>
                        sendMessageViaDiscord(channelInfo, username, message)
                    );
                }
            })
            .catch(() => {});
    }
}

// ========== SEND STATUS TO DISCORD ==========
function sendStatusToDiscord(serverKey, motd, status, players, maxPlayers, version) {
    if (!discordReady) return;
    const info = discordChannels[serverKey];
    if (!info?.channelId) return;

    queueDiscord(async () => {
        await discordFetch(`/channels/${info.channelId}/messages`, 'POST', {
            embeds: [{
                title: `${status === 'online' ? '🟢' : '🔴'} ${status === 'online' ? 'Online' : 'Offline'}`,
                color: status === 'online' ? 0x22c55e : 0xef4444,
                fields: [
                    {
                        name: 'Jogadores',
                        value: `${(players || []).length}/${maxPlayers || '?'}`,
                        inline: true
                    },
                    {
                        name: 'Versão',
                        value: version || '?',
                        inline: true
                    },
                ],
                footer: { text: serverKey },
                timestamp: new Date().toISOString(),
            }]
        });
    });
}

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
    const botKeys = new Set(
        botData.bots.map(b => b.serverKey || b.server || b.key)
    );

    botData.servers.forEach(s => {
        const lastSeen = serverLastSeen[s.key] || 0;
        const hasBot = botKeys.has(s.key);
        const recent = (now - lastSeen) < TIMEOUT;
        const hasPlayers = s.players && s.players.length > 0;

        const was = s.status;
        s.status = (hasBot || recent || hasPlayers) ? 'online' : 'offline';

        if (was !== s.status) {
            sendStatusToDiscord(
                s.key, s.motd, s.status,
                s.players, s.maxPlayers, s.version
            );
        }
    });
}

// ========== ANALYTICS ==========
function updateAnalytics() {
    const now = Date.now();

    // Jogadores totais
    let totalPlayers = 0;
    botData.servers.forEach(s => {
        if (s.status === 'online' && s.players) {
            totalPlayers += s.players.length;
        }
    });

    botData.analytics.playersOverTime.push({ timestamp: now, total: totalPlayers });
    if (botData.analytics.playersOverTime.length > 720) {
        botData.analytics.playersOverTime.shift();
    }

    // Mensagens por hora
    const h = new Date().getHours();
    const idx = botData.analytics.messagesPerHour.findIndex(x => x.hour === h);
    const cnt = botData.chatDatabase.filter(
        m => new Date(m.timestamp).getHours() === h
    ).length;

    if (idx >= 0) {
        botData.analytics.messagesPerHour[idx].count = cnt;
    } else {
        botData.analytics.messagesPerHour.push({ hour: h, count: cnt });
    }

    if (botData.analytics.messagesPerHour.length > 24) {
        botData.analytics.messagesPerHour =
            botData.analytics.messagesPerHour.slice(-24);
    }

    // Top servidores
    const ss = {};
    botData.chatDatabase.forEach(m => {
        if (!m.serverKey) return;
        if (!ss[m.serverKey]) ss[m.serverKey] = { messages: 0, players: new Set() };
        ss[m.serverKey].messages++;
        if (m.username) ss[m.serverKey].players.add(m.username);
    });

    botData.analytics.topServers = Object.entries(ss)
        .map(([k, d]) => ({
            serverKey: k,
            messages: d.messages,
            uniquePlayers: d.players.size
        }))
        .sort((a, b) => b.messages - a.messages)
        .slice(0, 10);
}

// ========== TIMERS ==========
const analyticsTimer = setInterval(updateAnalytics, 60000);

const serverStatusTimer = setInterval(() => {
    syncServerStatus();
    io.emit('serverUpdate', botData.servers);
}, 10000);

const botsUpdateTimer = setInterval(() => {
    io.emit('botsUpdate', botData.bots);
}, 5000);

// ========== PROCESS NEW SERVER ==========
function processNewServer(s) {
    if (!s || !s.key) return;
    serverLastSeen[s.key] = Date.now();
    const idx = botData.servers.findIndex(x => x.key === s.key);
    if (idx >= 0) {
        botData.servers[idx] = { ...botData.servers[idx], ...s };
    } else {
        botData.servers.push(s);
        getOrCreateChannel(s.key, s.motd).catch(() => {});
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

// ========== METRICS ==========
let totalWebConnections = 0;
let totalChatMessages = 0;

// ========== ROUTES ==========
app.get('/', (req, res) => res.json({
    status: 'online',
    uptime: getUptime(),
    uptimeMs: Date.now() - SERVER_START_TIME,
    botConectado: !!botSocket,
    bots: botData.stats.botsAtivos,
    msgs: botData.chatDatabase.length,
    totalMsgs: totalChatMessages,
    servers: botData.servers.length,
    webConnections: totalWebConnections,
    discord: discordReady
        ? `${Object.keys(discordChannels).length} canais`
        : 'off',
    queue: discordQueue.length,
    memory: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
}));

app.get('/health', (req, res) => res.json({
    status: 'ok',
    uptime: getUptime(),
    memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
}));

app.get('/discord-debug', (req, res) => res.json({
    enabled: DISCORD_ENABLED,
    ready: discordReady,
    tokenPresent: DISCORD_BOT_TOKEN.length > 0,
    tokenLength: DISCORD_BOT_TOKEN.length,
    guildId: DISCORD_GUILD_ID,
    categoryId: DISCORD_CATEGORY_ID,
    channelsCount: Object.keys(discordChannels).length,
    channels: Object.fromEntries(
        Object.entries(discordChannels).map(([k, v]) => [k, {
            channelId: v.channelId,
            channelName: v.channelName,
            hasWebhook: !!(v.webhookId && v.webhookToken),
        }])
    ),
    queueLength: discordQueue.length,
    processing: processingDiscord,
    failedAttempts,
}));

// ========== WEB AUTHENTICATION ==========
const authenticatedSockets = new Set();

function isAuthenticated(socket) {
    // Se não tem senha configurada, permite tudo
    if (!WEB_PASSWORD) return true;
    return authenticatedSockets.has(socket.id);
}

function requireAuth(socket, callback) {
    if (!isAuthenticated(socket)) {
        socket.emit('toast', '⛔ Não autenticado! Recarregue a página.');
        socket.emit('requireAuth');
        return false;
    }
    if (callback) callback();
    return true;
}

// ========== SEND INIT DATA ==========
function getInitData() {
    return {
        stats: botData.stats,
        bots: botData.bots,
        logs: botData.logs.slice(-50),
        chatDatabase: botData.chatDatabase.slice(-200),
        servers: botData.servers,
        analytics: botData.analytics,
        authRequired: !!WEB_PASSWORD,
    };
}

// ========== SOCKET.IO ==========
io.on('connection', (socket) => {

    // ==========================================
    // === BOT CONNECTION ===
    // ==========================================
    if (
        socket.handshake.auth?.key === SECRET_KEY &&
        socket.handshake.auth?.type === 'bot'
    ) {
        console.log('🤖 Bot conectou!');
        if (botSocket) {
            try { botSocket.disconnect(); } catch {}
        }
        botSocket = socket;
        io.emit('botStatus', true);

        // --- SYNC DATA ---
        socket.on('syncData', (data) => {
            if (!data || typeof data !== 'object') return;

            if (data.stats && typeof data.stats === 'object') {
                botData.stats = data.stats;
            }
            if (Array.isArray(data.bots)) {
                botData.bots = data.bots;
            }
            if (Array.isArray(data.logs)) {
                botData.logs = data.logs.slice(-MAX_LOGS);
            }
            if (Array.isArray(data.chatDatabase)) {
                // Merge sem duplicatas (por timestamp)
                const existingTimestamps = new Set(
                    botData.chatDatabase.map(m => m.timestamp)
                );
                const newMsgs = data.chatDatabase.filter(
                    m => !existingTimestamps.has(m.timestamp)
                );
                botData.chatDatabase.push(...newMsgs);
                // Limita tamanho
                if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) {
                    botData.chatDatabase = botData.chatDatabase.slice(-MAX_CHAT_MESSAGES);
                }
            }
            if (Array.isArray(data.servers)) {
                data.servers.forEach(processNewServer);
            }

            syncServerStatus();
            updateAnalytics();

            io.emit('init', getInitData());
        });

        // --- LOG ---
        socket.on('log', (e) => {
            if (!e) return;
            botData.logs.push(e);
            if (botData.logs.length > MAX_LOGS) botData.logs.shift();
            io.emit('log', e);
        });

        // --- CHAT MESSAGE ---
        socket.on('chatMessage', (data) => {
            if (!data || !data.serverKey) return;

            botData.chatDatabase.push(data);
            if (botData.chatDatabase.length > MAX_CHAT_MESSAGES) {
                botData.chatDatabase.shift();
            }
            totalChatMessages++;

            io.emit('chatMessage', data);
            updateAnalytics();

            // Discord
            const srv = botData.servers.find(s => s.key === data.serverKey);
            sendChatToDiscord(
                data.serverKey,
                srv?.motd || data.serverKey,
                data.username || 'Unknown',
                data.message || ''
            );
        });

        // --- SERVER UPDATE ---
        socket.on('serverUpdate', (d) => {
            if (Array.isArray(d)) {
                d.forEach(processNewServer);
            } else if (d?.key) {
                processNewServer(d);
            }
            syncServerStatus();
            io.emit('serverUpdate', botData.servers);
        });

        // --- BOTS UPDATE ---
        socket.on('botsUpdate', (data) => {
            if (!Array.isArray(data)) return;
            botData.bots = data;
            const now = Date.now();
            botData.bots.forEach(b => {
                const k = b.serverKey || b.server || b.key;
                if (k) serverLastSeen[k] = now;
            });
            syncServerStatus();
            io.emit('botsUpdate', data);
            io.emit('serverUpdate', botData.servers);
        });

        // --- STATS UPDATE ---
        socket.on('statsUpdate', (d) => {
            if (!d || typeof d !== 'object') return;
            botData.stats = d;
            io.emit('statsUpdate', d);
        });

        // --- DISCONNECT ---
        socket.on('disconnect', (reason) => {
            console.log(`🔌 Bot saiu: ${reason}`);
            botSocket = null;
            io.emit('botStatus', false);
        });

        return;
    }

    // ==========================================
    // === WEB CONNECTION ===
    // ==========================================
    totalWebConnections++;
    console.log(`🌐 Web conectou (#${totalWebConnections})`);

    // Envia se precisa de auth
    if (WEB_PASSWORD) {
        socket.emit('requireAuth');
    }

    // --- AUTHENTICATE ---
    socket.on('authenticate', (password, callback) => {
        if (!WEB_PASSWORD) {
            // Sem senha = permite tudo
            authenticatedSockets.add(socket.id);
            if (typeof callback === 'function') {
                callback({ success: true });
            }
            syncServerStatus();
            socket.emit('init', getInitData());
            socket.emit('botStatus', !!botSocket);
            return;
        }

        if (password === WEB_PASSWORD) {
            authenticatedSockets.add(socket.id);
            console.log(`🔓 Web autenticado: ${socket.id}`);
            if (typeof callback === 'function') {
                callback({ success: true });
            }
            syncServerStatus();
            socket.emit('init', getInitData());
            socket.emit('botStatus', !!botSocket);
        } else {
            console.log(`🔒 Tentativa de login falhou: ${socket.id}`);
            if (typeof callback === 'function') {
                callback({ success: false, message: 'Senha incorreta' });
            }
        }
    });

    // Se não tem senha, envia dados direto
    if (!WEB_PASSWORD) {
        authenticatedSockets.add(socket.id);
        syncServerStatus();
        socket.emit('init', getInitData());
        socket.emit('botStatus', !!botSocket);
    }

    // --- COMMANDS (protected) ---
    const protectedCommands = [
        'chat', 'command', 'announce', 'addMsg', 'delMsg',
        'setIntervalo', 'setUsername', 'connect_server',
        'disconnect_server', 'clearBlacklist', 'removeBlacklist',
        'jump', 'refresh', 'addBlacklist', 'restartBots',
        'clearChatDatabase', 'saveChat'
    ];

    protectedCommands.forEach(cmd => {
        socket.on(cmd, (d) => {
            if (!isAuthenticated(socket)) {
                socket.emit('toast', '⛔ Não autenticado!');
                socket.emit('requireAuth');
                return;
            }

            if (botSocket) {
                botSocket.emit(cmd, d);

                // Log da ação no servidor
                const actions = {
                    'chat': '💬 Chat enviado',
                    'command': '⚡ Comando enviado',
                    'announce': '📣 Anúncio forçado',
                    'addMsg': '➕ Mensagem adicionada',
                    'delMsg': '🗑️ Mensagem removida',
                    'setIntervalo': '⏱️ Intervalo alterado',
                    'setUsername': '👤 Username alterado',
                    'connect_server': '🔌 Conexão manual',
                    'disconnect_server': '🔌 Desconexão',
                    'clearBlacklist': '🧹 Blacklist limpa',
                    'removeBlacklist': '✅ Item removido da blacklist',
                    'addBlacklist': '🚫 Item adicionado à blacklist',
                    'restartBots': '🔄 Reiniciar bots',
                    'clearChatDatabase': '🗑️ Chat limpo',
                };
                if (actions[cmd]) {
                    console.log(`📋 Web ação: ${actions[cmd]}${d ? ` (${JSON.stringify(d).substring(0, 50)})` : ''}`);
                }
            } else {
                socket.emit('toast', '⚠️ Bot offline!');
            }
        });
    });

    // --- CHAT QUERY (protected) ---
    socket.on('getChatMessages', (f) => {
        if (!isAuthenticated(socket)) return;

        let r = botData.chatDatabase;
        if (f?.serverKey && f.serverKey !== 'all') {
            r = r.filter(m => m.serverKey === f.serverKey);
        }
        if (f?.search) {
            const s = f.search.toLowerCase();
            r = r.filter(m =>
                (m.username || '').toLowerCase().includes(s) ||
                (m.message || '').toLowerCase().includes(s)
            );
        }
        socket.emit('chatMessages', r.slice(-500));
    });

    // --- LOAD MORE CHAT (paginação) ---
    socket.on('loadMoreChat', ({ before, limit = 200 }) => {
        if (!isAuthenticated(socket)) return;

        let msgs = botData.chatDatabase;
        if (before) {
            msgs = msgs.filter(m => m.timestamp < before);
        }
        socket.emit('chatMessages', msgs.slice(-limit));
    });

    // --- ANALYTICS (protected) ---
    socket.on('getAnalytics', (t) => {
        if (!isAuthenticated(socket)) return;

        const now = Date.now();
        let start = 0;
        if (t === '1h') start = now - 3600000;
        else if (t === '24h') start = now - 86400000;
        else if (t === '7d') start = now - 604800000;

        socket.emit('analyticsData', {
            playersOverTime: botData.analytics.playersOverTime.filter(
                p => p.timestamp >= start
            ),
            messagesPerHour: botData.analytics.messagesPerHour,
            topServers: botData.analytics.topServers,
            totalMessages: botData.chatDatabase.filter(
                m => m.timestamp >= start
            ).length
        });
    });

    // --- DISCONNECT ---
    socket.on('disconnect', () => {
        authenticatedSockets.delete(socket.id);
    });
});

// ========== CLEANUP ON SHUTDOWN ==========
async function shutdown() {
    console.log('\n🛑 Desligando relay...');

    clearInterval(analyticsTimer);
    clearInterval(serverStatusTimer);
    clearInterval(botsUpdateTimer);

    // Fecha todas as conexões
    io.close();
    server.close();

    console.log('👋 Relay desligado!');
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

process.on('uncaughtException', (err) => {
    console.error('❌ FATAL:', err.message);
    console.error(err.stack);
});

process.on('unhandledRejection', (reason) => {
    const msg = String(reason?.message || reason || '').substring(0, 200);
    const ignore = ['fetch failed', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND'];
    if (!ignore.some(f => msg.includes(f))) {
        console.error('⚠️ Unhandled rejection:', msg);
    }
});

// ========== START ==========
server.listen(PORT, async () => {
    const warnings = validateConfig();

    console.log('');
    console.log('╔══════════════════════════════════════════╗');
    console.log('║      RELAY + DISCORD BRIDGE v4.2         ║');
    console.log('╠══════════════════════════════════════════╣');
    console.log(`║ 🌐 Porta: ${PORT}`);
    console.log(`║ 🎮 Discord: ${DISCORD_ENABLED ? 'Ativado' : 'Desativado'}`);
    console.log(`║ 🏠 Guild: ${DISCORD_GUILD_ID || 'não definido'}`);
    console.log(`║ 📁 Categoria: ${DISCORD_CATEGORY_ID || 'não definido'}`);
    console.log(`║ 🔒 Auth Web: ${WEB_PASSWORD ? 'Ativado ✅' : 'Desativado ⚠️'}`);
    console.log(`║ 🔑 Secret: ${SECRET_KEY === 'MUDE_ESSA_CHAVE_SECRETA_123' ? 'PADRÃO ⚠️' : 'Custom ✅'}`);
    console.log(`║ 📄 .env: ${fs.existsSync(path.join(__dirname, '.env')) ? 'Encontrado ✅' : 'Não encontrado'}`);
    console.log('╚══════════════════════════════════════════╝');

    if (warnings.length > 0) {
        console.log('');
        console.log('⚠️  AVISOS:');
        warnings.forEach(w => console.log(`   • ${w}`));
    }

    console.log('');
    await initDiscord();
});