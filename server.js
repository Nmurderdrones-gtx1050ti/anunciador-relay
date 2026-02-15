const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fetch = require('node-fetch');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
});

const SECRET_KEY = 'MUDE_ESSA_CHAVE_SECRETA_123';

// ===== DISCORD CONFIG =====
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN
    || 'MTQ3MjM5MjI5MTE1NDAwMjAzNg.GrpnDs.wOJHkSd33XcuxxH1TRaYFxwg6VMrV2emX5H7CE';
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID || '1472389594308939970';
const DISCORD_CATEGORY_ID = process.env.DISCORD_CATEGORY_ID || '1472394117244915803';

const DISCORD_API = 'https://discord.com/api/v10';
const DISCORD_ENABLED = DISCORD_BOT_TOKEN.length > 50;

// Discord state
const discordChannels = {};
const channelCreationLocks = {};
const failedAttempts = {};
const MAX_FAILURES = 3;
let discordReady = false;

// ===== DISCORD QUEUE =====
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
        await new Promise(r => setTimeout(r, 1500));
    }

    processingDiscord = false;
}

// ===== DISCORD FETCH =====
async function discordFetch(endpoint, method = 'GET', body = null) {
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
            const text = await response.text();
            let retryAfter = 5000;
            try {
                const data = JSON.parse(text);
                retryAfter = (data.retry_after || 5) * 1000;
            } catch {}
            console.log(`⏳ Rate limit, esperando ${retryAfter}ms...`);
            await new Promise(r => setTimeout(r, retryAfter + 1000));

            const retry = await fetch(`${DISCORD_API}${endpoint}`, options);
            if (retry.status === 204) return {};
            if (!retry.ok) {
                console.error(`❌ Discord retry falhou: ${retry.status}`);
                return null;
            }
            const ct = retry.headers.get('content-type') || '';
            if (!ct.includes('json')) return null;
            return await retry.json();
        }

        if (response.status === 401) {
            console.error('❌ Discord token inválido!');
            return null;
        }
        if (response.status === 403) {
            console.error('❌ Discord sem permissão!');
            return null;
        }
        if (response.status === 404) return null;
        if (response.status === 204) return {};

        const contentType = response.headers.get('content-type') || '';
        if (!contentType.includes('json')) return null;
        if (!response.ok) {
            const errText = await response.text();
            console.error(`❌ Discord ${response.status}:`, errText.substring(0, 100));
            return null;
        }

        return await response.json();
    } catch (err) {
        console.error('❌ Discord fetch error:', err.message);
        return null;
    }
}

// ===== DISCORD INIT =====
async function initDiscord() {
    if (!DISCORD_ENABLED) {
        console.log('⚠️ Discord não configurado');
        return;
    }

    console.log('🔍 Iniciando Discord...');

    // Tenta até 3 vezes
    let me = null;
    for (let attempt = 1; attempt <= 3; attempt++) {
        me = await discordFetch('/users/@me');
        if (me) break;
        console.log(`⚠️ Discord tentativa ${attempt}/3 falhou...`);
        await new Promise(r => setTimeout(r, 5000));
    }

    if (!me) {
        console.error('❌ Discord: token inválido após 3 tentativas!');
        console.log('🔄 Tentando novamente em 60s...');
        setTimeout(initDiscord, 60000);
        return;
    }
    console.log(`✅ Discord Bot: ${me.username} (${me.id})`);

    await new Promise(r => setTimeout(r, 1500));

    const guild = await discordFetch(`/guilds/${DISCORD_GUILD_ID}`);
    if (!guild) {
        console.error('❌ Discord: servidor não encontrado!');
        console.log('🔄 Tentando novamente em 60s...');
        setTimeout(initDiscord, 60000);
        return;
    }
    console.log(`✅ Discord Servidor: ${guild.name}`);

    await new Promise(r => setTimeout(r, 1500));

    const channels = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`);
    if (!channels || !Array.isArray(channels)) {
        console.error('❌ Discord: não listou canais!');
        console.log('🔄 Tentando novamente em 60s...');
        setTimeout(initDiscord, 60000);
        return;
    }

    const category = channels.find(ch => ch.id === DISCORD_CATEGORY_ID);
    if (!category) {
        console.error('❌ Discord: categoria não encontrada!');
        return;
    }
    console.log(`✅ Discord Categoria: ${category.name}`);

    // Cache canais existentes
    let cached = 0;
    channels
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

    // Carrega webhooks (em queue, devagar)
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

// ===== CHANNEL HELPERS =====
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
        .replace(/[^a-z0-9\s\-]/g, '')
        .replace(/\s+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '')
        .substring(0, 100) || 'servidor';
}

// ===== GET OR CREATE CHANNEL =====
async function getOrCreateChannel(serverKey, motd) {
    // Já em cache
    if (discordChannels[serverKey]?.channelId) {
        return discordChannels[serverKey];
    }

    if (!discordReady) return null;
    if ((failedAttempts[serverKey] || 0) >= MAX_FAILURES) return null;

    // Já sendo criado
    if (channelCreationLocks[serverKey]) {
        for (let i = 0; i < 60; i++) {
            await new Promise(r => setTimeout(r, 500));
            if (discordChannels[serverKey]?.channelId) {
                return discordChannels[serverKey];
            }
            if (!channelCreationLocks[serverKey]) break;
        }
        return discordChannels[serverKey] || null;
    }

    channelCreationLocks[serverKey] = true;

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

        await new Promise(r => setTimeout(r, 1500));

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

        // Embed de boas-vindas
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

    delete channelCreationLocks[serverKey];
    return result;
}

// ===== SEND CHAT TO DISCORD =====
async function sendMessageViaDiscord(info, username, message) {
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
                        content: message
                    }),
                }
            );
            if (resp.ok || resp.status === 204) return;
        } catch {}
    }

    // Fallback: mensagem normal
    await discordFetch(`/channels/${info.channelId}/messages`, 'POST', {
        content: `**${username}** » ${message}`,
    });
}

function sendChatToDiscord(serverKey, motd, username, message) {
    if (!discordReady) return;

    const info = discordChannels[serverKey];

    if (info?.channelId) {
        // Canal existe → envia direto
        queueDiscord(() => sendMessageViaDiscord(info, username, message));
    } else {
        // Canal não existe → cria e depois envia
        getOrCreateChannel(serverKey, motd)
            .then((channelInfo) => {
                if (channelInfo?.channelId) {
                    queueDiscord(() =>
                        sendMessageViaDiscord(channelInfo, username, message)
                    );
                } else {
                    console.log(`⚠️ Sem canal para ${serverKey}, mensagem perdida: <${username}> ${message}`);
                }
            })
            .catch(() => {});
    }
}

// ===== SEND STATUS TO DISCORD =====
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

// ===== BOT DATA =====
let botSocket = null;
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

// ===== SERVER STATUS =====
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

// ===== ANALYTICS =====
function updateAnalytics() {
    const now = Date.now();

    let totalPlayers = 0;
    botData.servers.forEach(s => {
        if (s.status === 'online' && s.players) {
            totalPlayers += s.players.length;
        }
    });

    botData.analytics.playersOverTime.push({
        timestamp: now,
        total: totalPlayers
    });
    if (botData.analytics.playersOverTime.length > 720) {
        botData.analytics.playersOverTime.shift();
    }

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

    const ss = {};
    botData.chatDatabase.forEach(m => {
        if (!ss[m.serverKey]) ss[m.serverKey] = { messages: 0, players: new Set() };
        ss[m.serverKey].messages++;
        ss[m.serverKey].players.add(m.username);
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

setInterval(updateAnalytics, 60000);
setInterval(() => {
    syncServerStatus();
    io.emit('serverUpdate', botData.servers);
}, 10000);

// ===== PROCESS NEW SERVER =====
function processNewServer(s) {
    serverLastSeen[s.key] = Date.now();

    const idx = botData.servers.findIndex(x => x.key === s.key);
    if (idx >= 0) {
        botData.servers[idx] = { ...botData.servers[idx], ...s };
    } else {
        botData.servers.push(s);
        // Cria canal no Discord (async, não bloqueia)
        getOrCreateChannel(s.key, s.motd).catch(() => {});
    }
}

// ===== ROUTES =====
app.get('/', (req, res) => res.json({
    status: 'online',
    botConectado: !!botSocket,
    bots: botData.stats.botsAtivos,
    msgs: botData.chatDatabase.length,
    servers: botData.servers.length,
    discord: discordReady
        ? `${Object.keys(discordChannels).length} canais`
        : 'off',
    queue: discordQueue.length,
}));

app.get('/health', (req, res) => res.send('OK'));

// Debug do Discord
app.get('/discord-debug', (req, res) => res.json({
    enabled: DISCORD_ENABLED,
    ready: discordReady,
    tokenLength: DISCORD_BOT_TOKEN.length,
    guildId: DISCORD_GUILD_ID,
    categoryId: DISCORD_CATEGORY_ID,
    channelsCount: Object.keys(discordChannels).length,
    channels: discordChannels,
    queueLength: discordQueue.length,
    processing: processingDiscord,
    failedAttempts,
}));

// ===== SOCKET.IO =====
io.on('connection', (socket) => {

    // === BOT CONNECTION ===
    if (
        socket.handshake.auth?.key === SECRET_KEY &&
        socket.handshake.auth?.type === 'bot'
    ) {
        console.log('🤖 Bot conectou!');
        if (botSocket) try { botSocket.disconnect(); } catch {}
        botSocket = socket;
        io.emit('botStatus', true);

        socket.on('syncData', (data) => {
            if (data.stats) botData.stats = data.stats;
            if (data.bots) botData.bots = data.bots;
            if (data.logs) botData.logs = data.logs;
            if (data.chatDatabase) botData.chatDatabase = data.chatDatabase;
            if (data.servers) {
                const arr = Array.isArray(data.servers) ? data.servers : [];
                arr.forEach(processNewServer);
            }
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

        socket.on('log', (e) => {
            botData.logs.push(e);
            if (botData.logs.length > 200) botData.logs.shift();
            io.emit('log', e);
        });

        socket.on('chatMessage', (data) => {
            botData.chatDatabase.push(data);
            if (botData.chatDatabase.length > 10000) botData.chatDatabase.shift();
            io.emit('chatMessage', data);
            updateAnalytics();

            // Envia pro Discord
            const srv = botData.servers.find(s => s.key === data.serverKey);
            sendChatToDiscord(
                data.serverKey,
                srv?.motd || data.serverKey,
                data.username,
                data.message
            );
        });

        socket.on('serverUpdate', (d) => {
            if (Array.isArray(d)) d.forEach(processNewServer);
            else if (d?.key) processNewServer(d);
            syncServerStatus();
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('botsUpdate', (data) => {
            botData.bots = data || [];
            const now = Date.now();
            botData.bots.forEach(b => {
                const k = b.serverKey || b.server || b.key;
                if (k) serverLastSeen[k] = now;
            });
            syncServerStatus();
            io.emit('botsUpdate', data);
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('statsUpdate', (d) => {
            botData.stats = d;
            io.emit('statsUpdate', d);
        });

        socket.on('disconnect', () => {
            console.log('🔌 Bot saiu');
            botSocket = null;
            io.emit('botStatus', false);
        });

        return;
    }

    // === WEB CONNECTION ===
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
    socket.emit('botStatus', !!botSocket);

    // Proxy de comandos pro bot
    const commands = [
        'chat', 'command', 'announce', 'addMsg', 'delMsg',
        'setIntervalo', 'setUsername', 'connect_server',
        'disconnect_server', 'clearBlacklist', 'removeBlacklist',
        'jump', 'refresh', 'addBlacklist', 'restartBots',
        'clearChatDatabase', 'saveChat'
    ];

    commands.forEach(cmd => {
        socket.on(cmd, (d) => {
            if (botSocket) {
                botSocket.emit(cmd, d);
            } else {
                socket.emit('toast', '⚠️ Bot offline!');
            }
        });
    });

    socket.on('getChatMessages', (f) => {
        let r = botData.chatDatabase;
        if (f.serverKey && f.serverKey !== 'all') {
            r = r.filter(m => m.serverKey === f.serverKey);
        }
        if (f.search) {
            const s = f.search.toLowerCase();
            r = r.filter(m =>
                m.username.toLowerCase().includes(s) ||
                m.message.toLowerCase().includes(s)
            );
        }
        socket.emit('chatMessages', r.slice(-500));
    });

    socket.on('getAnalytics', (t) => {
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
});

// Sync periódico
setInterval(() => {
    io.emit('botsUpdate', botData.bots);
}, 5000);

// ===== START =====
const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
    console.log(`✅ Relay rodando na porta ${PORT}`);
    console.log('');
    console.log('╔════════════════════════════════════════╗');
    console.log('║        RELAY + DISCORD BRIDGE          ║');
    console.log('╠════════════════════════════════════════╣');
    console.log(`║ 🌐 Porta: ${PORT}`);
    console.log(`║ 🎮 Discord: ${DISCORD_ENABLED ? 'Ativado' : 'Desativado'}`);
    console.log(`║ 🏠 Guild: ${DISCORD_GUILD_ID}`);
    console.log(`║ 📁 Categoria: ${DISCORD_CATEGORY_ID}`);
    console.log('╚════════════════════════════════════════╝');
    console.log('');

    await initDiscord();
});