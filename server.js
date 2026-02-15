// server.js
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

// ===== DISCORD CONFIG =====
const DISCORD_BOT_TOKEN = 'MTQ3MjM5MjI5MTE1NDAwMjAzNg.GGTRED.sgY85ZjZG-_kBXwjQq0KjRQLN2T1fTJGAI2DbY';
const DISCORD_GUILD_ID = '1472389594308939970';
const DISCORD_CATEGORY_ID = '1472394117244915803';
// ===========================

const DISCORD_API = 'https://discord.com/api/v10';
const discordHeaders = {
    'Authorization': `Bot ${DISCORD_BOT_TOKEN}`,
    'Content-Type': 'application/json',
};

// Cache: serverKey -> { channelId, webhookId, webhookToken }
const discordChannels = {};

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
    // Discord channel names: lowercase, max 100 chars, only a-z 0-9 and hyphens
    return name
        .toLowerCase()
        .replace(/[^a-z0-9\s\-]/g, '') // remove special chars
        .replace(/\s+/g, '-')           // spaces to hyphens
        .replace(/-+/g, '-')            // multiple hyphens to one
        .replace(/^-|-$/g, '')          // trim hyphens
        .substring(0, 100)              // max 100 chars
        || 'servidor';
}

async function discordFetch(endpoint, method = 'GET', body = null) {
    try {
        const options = {
            method,
            headers: discordHeaders,
        };
        if (body) options.body = JSON.stringify(body);

        const response = await fetch(`${DISCORD_API}${endpoint}`, options);

        // Rate limit handling
        if (response.status === 429) {
            const data = await response.json();
            const retryAfter = (data.retry_after || 1) * 1000;
            console.log(`⏳ Discord rate limit, esperando ${retryAfter}ms...`);
            await new Promise(r => setTimeout(r, retryAfter));
            return discordFetch(endpoint, method, body);
        }

        if (!response.ok) {
            const errText = await response.text();
            console.error(`❌ Discord API error ${response.status}: ${errText}`);
            return null;
        }

        // 204 No Content
        if (response.status === 204) return {};

        return await response.json();
    } catch (err) {
        console.error('❌ Discord fetch error:', err.message);
        return null;
    }
}

async function findOrCreateChannel(serverKey, motd) {
    // Check cache first
    if (discordChannels[serverKey] && discordChannels[serverKey].channelId) {
        return discordChannels[serverKey];
    }

    if (!DISCORD_BOT_TOKEN || DISCORD_BOT_TOKEN === 'SEU_BOT_TOKEN_AQUI') {
        return null; // Discord not configured
    }

    const motdClean = cleanMotd(motd);
    const ipFormatted = serverKey.replace(/[.:]/g, '-');
    const channelName = sanitizeChannelName(`${motdClean}-${ipFormatted}`);

    console.log(`🔍 Buscando/criando canal Discord: #${channelName}`);

    // Search existing channels in the category
    const channels = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`);
    if (!channels) return null;

    // Find existing channel with matching topic (we store serverKey in topic)
    let channel = channels.find(ch =>
        ch.parent_id === DISCORD_CATEGORY_ID &&
        ch.topic && ch.topic.includes(serverKey)
    );

    if (!channel) {
        // Also try by name
        channel = channels.find(ch =>
            ch.parent_id === DISCORD_CATEGORY_ID &&
            ch.name === channelName
        );
    }

    if (!channel) {
        // Create new channel
        console.log(`📝 Criando canal #${channelName}`);
        channel = await discordFetch(`/guilds/${DISCORD_GUILD_ID}/channels`, 'POST', {
            name: channelName,
            type: 0, // Text channel
            parent_id: DISCORD_CATEGORY_ID,
            topic: `📡 Chat do servidor Minecraft: ${serverKey} | MOTD: ${motdClean}`,
            permission_overwrites: [
                {
                    // @everyone - deny view
                    id: DISCORD_GUILD_ID,
                    type: 0, // role
                    deny: '1024', // VIEW_CHANNEL
                    allow: '0',
                }
            ]
        });

        if (!channel) {
            console.error('❌ Falha ao criar canal Discord');
            return null;
        }

        // Send initial embed
        await discordFetch(`/channels/${channel.id}/messages`, 'POST', {
            embeds: [{
                title: `📡 ${motdClean}`,
                description: `**Servidor Minecraft detectado!**`,
                color: 0x6366f1,
                fields: [
                    { name: '🌐 IP', value: `\`${serverKey}\``, inline: true },
                    { name: '📋 MOTD', value: motdClean, inline: true },
                ],
                footer: { text: 'Central Radmin VPN • Bot Anunciador' },
                timestamp: new Date().toISOString(),
            }]
        });

        console.log(`✅ Canal criado: #${channelName} (${channel.id})`);
    }

    // Create webhook for the channel (faster message sending)
    let webhook = null;
    const webhooks = await discordFetch(`/channels/${channel.id}/webhooks`);

    if (webhooks && webhooks.length > 0) {
        // Reuse existing webhook
        webhook = webhooks.find(w => w.name === 'MC Chat Logger');
    }

    if (!webhook) {
        webhook = await discordFetch(`/channels/${channel.id}/webhooks`, 'POST', {
            name: 'MC Chat Logger',
        });
    }

    const result = {
        channelId: channel.id,
        channelName: channel.name,
        webhookId: webhook ? webhook.id : null,
        webhookToken: webhook ? webhook.token : null,
    };

    discordChannels[serverKey] = result;
    console.log(`✅ Canal pronto: #${channel.name} (webhook: ${webhook ? 'sim' : 'não'})`);
    return result;
}

async function sendToDiscord(serverKey, motd, username, message, timestamp) {
    const channelInfo = await findOrCreateChannel(serverKey, motd);
    if (!channelInfo) return;

    const time = new Date(timestamp).toLocaleTimeString('pt-BR', {
        hour: '2-digit', minute: '2-digit', second: '2-digit'
    });

    // Use webhook if available (faster, no rate limit issues, custom avatar)
    if (channelInfo.webhookId && channelInfo.webhookToken) {
        const skinUrl = `https://mc-heads.net/avatar/${username}/64`;
        try {
            await fetch(
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
        } catch (err) {
            console.error('❌ Webhook error:', err.message);
            // Fallback to regular message
            await sendRegularMessage(channelInfo.channelId, username, message, time);
        }
    } else {
        await sendRegularMessage(channelInfo.channelId, username, message, time);
    }
}

async function sendRegularMessage(channelId, username, message, time) {
    await discordFetch(`/channels/${channelId}/messages`, 'POST', {
        content: `**${username}** » ${message}`,
    });
}

// Send server status updates to Discord
async function sendServerStatusToDiscord(serverKey, motd, status, players, maxPlayers, version) {
    const channelInfo = await findOrCreateChannel(serverKey, motd);
    if (!channelInfo) return;

    const color = status === 'online' ? 0x22c55e : 0xef4444;
    const emoji = status === 'online' ? '🟢' : '🔴';

    await discordFetch(`/channels/${channelInfo.channelId}/messages`, 'POST', {
        embeds: [{
            title: `${emoji} Servidor ${status === 'online' ? 'Online' : 'Offline'}`,
            color: color,
            fields: [
                { name: 'Jogadores', value: `${(players || []).length}/${maxPlayers || '?'}`, inline: true },
                { name: 'Versão', value: version || '?', inline: true },
            ],
            footer: { text: serverKey },
            timestamp: new Date().toISOString(),
        }]
    });
}

// Update channel name if MOTD changes
async function updateChannelIfNeeded(serverKey, motd) {
    if (!discordChannels[serverKey]) return;

    const motdClean = cleanMotd(motd);
    const ipFormatted = serverKey.replace(/[.:]/g, '-');
    const newName = sanitizeChannelName(`${motdClean}-${ipFormatted}`);

    if (discordChannels[serverKey].channelName !== newName) {
        console.log(`📝 Atualizando canal: ${discordChannels[serverKey].channelName} → ${newName}`);
        await discordFetch(`/channels/${discordChannels[serverKey].channelId}`, 'PATCH', {
            name: newName,
            topic: `📡 Chat do servidor Minecraft: ${serverKey} | MOTD: ${motdClean}`,
        });
        discordChannels[serverKey].channelName = newName;
    }
}

// ===== REST OF SERVER =====

app.get('/', (req, res) => {
    res.json({
        status: 'online',
        botConectado: botSocket !== null,
        bots: botData.stats.botsAtivos,
        totalMensagens: botData.chatDatabase.length,
        totalServidores: botData.servers.length,
        discordChannels: Object.keys(discordChannels).length,
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

        // Send Discord status update on change
        if (wasOnline !== (s.status === 'online')) {
            sendServerStatusToDiscord(s.key, s.motd, s.status, s.players, s.maxPlayers, s.version)
                .catch(err => console.error('Discord status error:', err.message));
        }
    });
}

setInterval(() => {
    syncServerStatus();
    io.emit('serverUpdate', botData.servers);
}, 10000);

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
                    if (idx >= 0) {
                        botData.servers[idx] = { ...botData.servers[idx], ...s };
                    } else {
                        botData.servers.push(s);
                        // New server detected → create Discord channel
                        findOrCreateChannel(s.key, s.motd).catch(err =>
                            console.error('Discord channel error:', err.message)
                        );
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
            const motd = srv ? srv.motd : data.serverKey;
            sendToDiscord(data.serverKey, motd, data.username, data.message, data.timestamp)
                .catch(err => console.error('Discord msg error:', err.message));
        });

        socket.on('serverUpdate', (serverData) => {
            const now = Date.now();
            if (Array.isArray(serverData)) {
                serverData.forEach(s => {
                    serverLastSeen[s.key] = now;
                    const idx = botData.servers.findIndex(x => x.key === s.key);
                    if (idx >= 0) {
                        const old = botData.servers[idx];
                        botData.servers[idx] = { ...old, ...s };
                        // Update Discord channel if MOTD changed
                        if (cleanMotd(old.motd) !== cleanMotd(s.motd)) {
                            updateChannelIfNeeded(s.key, s.motd).catch(() => {});
                        }
                    } else {
                        botData.servers.push(s);
                        findOrCreateChannel(s.key, s.motd).catch(() => {});
                    }
                });
            } else if (serverData && serverData.key) {
                serverLastSeen[serverData.key] = now;
                const idx = botData.servers.findIndex(s => s.key === serverData.key);
                if (idx >= 0) {
                    botData.servers[idx] = { ...botData.servers[idx], ...serverData };
                } else {
                    botData.servers.push(serverData);
                    findOrCreateChannel(serverData.key, serverData.motd).catch(() => {});
                }
            }
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
    io.emit('botsUpdate', botData.bots);
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`✅ Relay na porta ${PORT}`);
    if (DISCORD_BOT_TOKEN && DISCORD_BOT_TOKEN !== 'SEU_BOT_TOKEN_AQUI') {
        console.log(`🎮 Discord integração ativa!`);
    } else {
        console.log(`⚠️ Discord não configurado (preencha DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CATEGORY_ID)`);
    }
});