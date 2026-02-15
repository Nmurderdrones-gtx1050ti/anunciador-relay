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
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || 'MTQ3MjM5MjI5MTE1NDAwMjAzNg.GGTRED.sgY85ZjZG-_kBXwjQq0KjRQLN2T1fTJGAI2DbY';
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID || '1472389594308939970';
const DISCORD_CATEGORY_ID = process.env.DISCORD_CATEGORY_ID || '1472394117244915803';
// ===========================

const DISCORD_API = 'https://discord.com/api/v10';
const DISCORD_ENABLED = DISCORD_BOT_TOKEN && 
    DISCORD_BOT_TOKEN !== 'SEU_BOT_TOKEN_AQUI' && 
    DISCORD_BOT_TOKEN.length > 50;

// Cache: serverKey -> { channelId, webhookId, webhookToken, channelName }
const discordChannels = {};
// Prevent duplicate creation attempts
const channelCreationLocks = {};
// Track failed attempts to avoid infinite retries
const failedAttempts = {};
const MAX_FAILURES = 3;

// Message queue to avoid rate limits
const messageQueue = [];
let processingQueue = false;

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

const serverLastSeen = {};

// ===== DISCORD FUNCTIONS =====

function cleanMotd(motd) {
    if (!motd) return 'sem-motd';
    if (typeof motd === 'object') {
        if (motd.text !== undefined) {
            let result = cleanMotd(motd.text);
            if (motd.extra) result += motd.extra.map(e => cleanMotd(e)).join('');
            return result;
        }
        if (motd.translate) return motd.translate;
        return 'servidor';
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
        .substring(0, 100)
        || 'servidor';
}

async function discordFetch(endpoint, method = 'GET', body = null) {
    if (!DISCORD_ENABLED) return null;

    try {
        const options = {
            method,
            headers: {
                'Authorization': `Bot ${DISCORD_BOT_TOKEN}`,
                'Content-Type': 'application/json',
            },
        };
        if (body) options.body = JSON.stringify(body);

        const url = `${DISCORD_API}${endpoint}`;
        const response = await fetch(url, options);

        // Rate limit
        if (response.status === 429) {
            const data = await response.json().catch(() => ({ retry_after: 5 }));
            const retryAfter = (data.retry_after || 5) * 1000;
            console.log(`⏳ Discord rate limit, esperando ${retryAfter}ms...`);
            await new Promise(r => setTimeout(r, retryAfter));
            return discordFetch(endpoint, method, body);
        }

        // Auth errors
        if (response.status === 401) {
            console.error('❌ Discord: Token inválido! Verifique o DISCORD_BOT_TOKEN');
            return null;
        }

        if (response.status === 403) {
            console.error('❌ Discord: Sem permissão! Verifique se o bot tem Manage Channels, Send Messages, Manage Webhooks');
            return null;
        }

        if (response.status === 404) {
            console.error(`❌ Discord: Não encontrado (404) - ${endpoint}`);
            return null;
        }

        // No content
        if (response.status === 204) return {};

        // Check if response is JSON
        const contentType = response.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            const text = await response.text();
            console.error(`❌ Discord: Resposta não-JSON (${response.status}): ${text.substring(0, 100)}...`);
            return null;
        }

        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            console.error(`❌ Discord API error ${response.status}:`, JSON.stringify(errData).substring(0, 200));
            return null;
        }

        return await response.json();
    } catch (err) {
        console.error('❌ Discord fetch error:', err.message);
        return null;
    }
}

// Test Discord connection on startup
async function testDiscordConnection() {
    if (!DISCORD_ENABLED) {
        console.log('⚠️ Discord não configurado');
        return false;
    }

    console.log('🔍 Testando conexão Discord...');

    // Test bot token
    const me = await discordFetch('/users/@me');
    if (!me) {
        console.error('❌ Discord: Não conseguiu autenticar. Verifique o token!');
        return false;
    }
    console.log(`✅ Discord bot: ${me.username}#${me.discriminator} (${me.id})`);

    // Test guild access
    const guild = await discordFetch(`/guilds/${DISCORD_GUILD_ID}`);
    if (!guild) {
        console.error(`❌ Discord: Não conseguiu acessar o servidor ${DISCORD_GUILD_ID}. Bot foi convidado?`);
        return false;
    }
    console.log(`✅ Discord servidor: ${guild.name}`);

    // Test category exists
    const channels = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`);
    if (!channels) {
        console.error('❌ Discord: Não conseguiu listar canais');
        return false;
    }

    const category = channels.find(ch => ch.id === DISCORD_CATEGORY_ID);
    if (!category) {
        console.error(`❌ Discord: Categoria ${DISCORD_CATEGORY_ID} não encontrada!`);
        return false;
    }
    console.log(`✅ Discord categoria: ${category.name}`);

    // Cache existing channels in category
    channels.filter(ch => ch.parent_id === DISCORD_CATEGORY_ID && ch.type === 0).forEach(ch => {
        // Try to extract serverKey from topic
        if (ch.topic) {
            const match = ch.topic.match(/(\d+\.\d+\.\d+\.\d+:\d+)/);
            if (match) {
                discordChannels[match[1]] = {
                    channelId: ch.id,
                    channelName: ch.name,
                    webhookId: null,
                    webhookToken: null,
                };
                console.log(`  📌 Canal existente: #${ch.name} → ${match[1]}`);
            }
        }
    });

    console.log(`✅ Discord pronto! (${Object.keys(discordChannels).length} canais em cache)`);
    return true;
}

async function findOrCreateChannel(serverKey, motd) {
    // Already cached
    if (discordChannels[serverKey]?.channelId) {
        return discordChannels[serverKey];
    }

    if (!DISCORD_ENABLED) return null;

    // Too many failures for this server, skip
    if ((failedAttempts[serverKey] || 0) >= MAX_FAILURES) return null;

    // Already creating this channel (prevent duplicates)
    if (channelCreationLocks[serverKey]) {
        // Wait for it to finish
        return new Promise((resolve) => {
            const check = setInterval(() => {
                if (!channelCreationLocks[serverKey]) {
                    clearInterval(check);
                    resolve(discordChannels[serverKey] || null);
                }
            }, 500);
            // Timeout after 15s
            setTimeout(() => { clearInterval(check); resolve(null); }, 15000);
        });
    }

    channelCreationLocks[serverKey] = true;

    try {
        const motdClean = cleanMotd(motd);
        const ipFormatted = serverKey.replace(/[.:]/g, '-');
        const channelName = sanitizeChannelName(`${motdClean}-${ipFormatted}`);

        console.log(`📝 Criando canal Discord: #${channelName}`);

        // Create channel
        const channel = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`, 'POST', {
            name: channelName,
            type: 0,
            parent_id: DISCORD_CATEGORY_ID,
            topic: `📡 Minecraft: ${serverKey} | ${motdClean}`,
        });

        if (!channel || !channel.id) {
            console.error(`❌ Falha ao criar canal para ${serverKey}`);
            failedAttempts[serverKey] = (failedAttempts[serverKey] || 0) + 1;
            return null;
        }

        // Create webhook
        let webhook = null;
        try {
            webhook = await discordFetch(`/channels/${channel.id}/webhooks`, 'POST', {
                name: 'MC Chat',
            });
        } catch (e) {
            console.error('⚠️ Webhook creation failed:', e.message);
        }

        const result = {
            channelId: channel.id,
            channelName: channel.name,
            webhookId: webhook?.id || null,
            webhookToken: webhook?.token || null,
        };

        discordChannels[serverKey] = result;
        failedAttempts[serverKey] = 0;

        // Send welcome embed
        queueDiscordMessage(async () => {
            await discordFetch(`/channels/${channel.id}/messages`, 'POST', {
                embeds: [{
                    title: `📡 ${motdClean}`,
                    description: 'Servidor Minecraft detectado!',
                    color: 0x6366f1,
                    fields: [
                        { name: '🌐 IP', value: `\`${serverKey}\``, inline: true },
                    ],
                    footer: { text: 'Central Radmin VPN' },
                    timestamp: new Date().toISOString(),
                }]
            });
        });

        console.log(`✅ Canal criado: #${channel.name} (${channel.id})`);
        return result;

    } catch (err) {
        console.error(`❌ Erro criando canal ${serverKey}:`, err.message);
        failedAttempts[serverKey] = (failedAttempts[serverKey] || 0) + 1;
        return null;
    } finally {
        delete channelCreationLocks[serverKey];
    }
}

// Queue system to avoid rate limits
function queueDiscordMessage(fn) {
    messageQueue.push(fn);
    processQueue();
}

async function processQueue() {
    if (processingQueue || messageQueue.length === 0) return;
    processingQueue = true;

    while (messageQueue.length > 0) {
        const fn = messageQueue.shift();
        try {
            await fn();
        } catch (err) {
            console.error('❌ Queue message error:', err.message);
        }
        // Wait between messages to avoid rate limits
        await new Promise(r => setTimeout(r, 500));
    }

    processingQueue = false;
}

async function sendToDiscord(serverKey, motd, username, message, timestamp) {
    if (!DISCORD_ENABLED) return;

    const channelInfo = await findOrCreateChannel(serverKey, motd);
    if (!channelInfo) return;

    queueDiscordMessage(async () => {
        if (channelInfo.webhookId && channelInfo.webhookToken) {
            const skinUrl = `https://mc-heads.net/avatar/${username}/32`;
            try {
                const resp = await fetch(
                    `${DISCORD_API}/webhooks/${channelInfo.webhookId}/${channelInfo.webhookToken}`,
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            username: username,
                            avatar_url: skinUrl,
                            content: message,
                        })
                    }
                );
                if (!resp.ok && resp.status !== 204) {
                    // Webhook may have been deleted, fallback
                    await discordFetch(`/channels/${channelInfo.channelId}/messages`, 'POST', {
                        content: `**${username}** » ${message}`,
                    });
                }
            } catch {
                await discordFetch(`/channels/${channelInfo.channelId}/messages`, 'POST', {
                    content: `**${username}** » ${message}`,
                });
            }
        } else {
            await discordFetch(`/channels/${channelInfo.channelId}/messages`, 'POST', {
                content: `**${username}** » ${message}`,
            });
        }
    });
}

async function sendServerStatusToDiscord(serverKey, motd, status, players, maxPlayers, version) {
    if (!DISCORD_ENABLED) return;

    const channelInfo = await findOrCreateChannel(serverKey, motd);
    if (!channelInfo) return;

    const color = status === 'online' ? 0x22c55e : 0xef4444;
    const emoji = status === 'online' ? '🟢' : '🔴';

    queueDiscordMessage(async () => {
        await discordFetch(`/channels/${channelInfo.channelId}/messages`, 'POST', {
            embeds: [{
                title: `${emoji} ${status === 'online' ? 'Online' : 'Offline'}`,
                color,
                fields: [
                    { name: 'Jogadores', value: `${(players || []).length}/${maxPlayers || '?'}`, inline: true },
                    { name: 'Versão', value: version || '?', inline: true },
                ],
                footer: { text: serverKey },
                timestamp: new Date().toISOString(),
            }]
        });
    });
}

// ===== SERVER LOGIC =====

app.get('/', (req, res) => {
    res.json({
        status: 'online',
        botConectado: botSocket !== null,
        bots: botData.stats.botsAtivos,
        totalMensagens: botData.chatDatabase.length,
        totalServidores: botData.servers.length,
        discord: DISCORD_ENABLED ? `${Object.keys(discordChannels).length} canais` : 'desativado',
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
    const hourCount = botData.chatDatabase.filter(m => new Date(m.timestamp).getHours() === currentHour).length;

    if (hourIdx >= 0) botData.analytics.messagesPerHour[hourIdx].count = hourCount;
    else botData.analytics.messagesPerHour.push({ hour: currentHour, count: hourCount });
    if (botData.analytics.messagesPerHour.length > 24) botData.analytics.messagesPerHour = botData.analytics.messagesPerHour.slice(-24);

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

function syncServerStatus() {
    const now = Date.now();
    const TIMEOUT = 120000;
    const botKeys = new Set(botData.bots.map(b => b.serverKey || b.server));

    botData.servers.forEach(s => {
        const lastSeen = serverLastSeen[s.key] || 0;
        const hasBot = botKeys.has(s.key);
        const recentlySeen = (now - lastSeen) < TIMEOUT;
        const hasPlayers = s.players && s.players.length > 0;

        const wasOnline = s.status === 'online';
        s.status = (hasBot || recentlySeen || hasPlayers) ? 'online' : 'offline';

        if (wasOnline !== (s.status === 'online')) {
            sendServerStatusToDiscord(s.key, s.motd, s.status, s.players, s.maxPlayers, s.version);
        }
    });
}

setInterval(() => {
    syncServerStatus();
    io.emit('serverUpdate', botData.servers);
}, 10000);

// ===== SOCKET.IO =====

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
            if (data.servers) {
                const incoming = Array.isArray(data.servers) ? data.servers : [];
                const now = Date.now();
                incoming.forEach(s => {
                    serverLastSeen[s.key] = now;
                    const idx = botData.servers.findIndex(x => x.key === s.key);
                    if (idx >= 0) botData.servers[idx] = { ...botData.servers[idx], ...s };
                    else {
                        botData.servers.push(s);
                        // Create Discord channel (non-blocking)
                        findOrCreateChannel(s.key, s.motd).catch(() => {});
                    }
                });
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

            // Send to Discord
            const srv = botData.servers.find(s => s.key === data.serverKey);
            sendToDiscord(data.serverKey, srv?.motd || data.serverKey, data.username, data.message, data.timestamp);
        });

        socket.on('serverUpdate', (serverData) => {
            const now = Date.now();
            const process = (s) => {
                serverLastSeen[s.key] = now;
                const idx = botData.servers.findIndex(x => x.key === s.key);
                if (idx >= 0) botData.servers[idx] = { ...botData.servers[idx], ...s };
                else {
                    botData.servers.push(s);
                    findOrCreateChannel(s.key, s.motd).catch(() => {});
                }
            };
            if (Array.isArray(serverData)) serverData.forEach(process);
            else if (serverData?.key) process(serverData);
            syncServerStatus();
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('botsUpdate', (data) => {
            botData.bots = data || [];
            const now = Date.now();
            botData.bots.forEach(b => {
                const key = b.serverKey || b.server;
                if (key) serverLastSeen[key] = now;
            });
            syncServerStatus();
            io.emit('botsUpdate', data);
            io.emit('serverUpdate', botData.servers);
        });

        socket.on('statsUpdate', (data) => { botData.stats = data; io.emit('statsUpdate', data); });

        socket.on('disconnect', () => {
            console.log('🔌 Bot saiu');
            botSocket = null;
            io.emit('botStatus', false);
        });
        return;
    }

    // Web
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
            filtered = filtered.filter(m => m.username.toLowerCase().includes(s) || m.message.toLowerCase().includes(s));
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

setInterval(() => { io.emit('botsUpdate', botData.bots); }, 5000);

// ===== START =====
const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
    console.log(`✅ Relay na porta ${PORT}`);
    if (DISCORD_ENABLED) {
        const ok = await testDiscordConnection();
        if (ok) console.log('🎮 Discord integração ativa!');
        else console.log('❌ Discord falhou na inicialização');
    } else {
        console.log('⚠️ Discord não configurado');
    }
});