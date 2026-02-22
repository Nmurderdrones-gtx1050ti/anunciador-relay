/**
 * Correções para o Chat e appendLog.
 * 1) appendLog: sanitizar emoji (evitar XSS).
 * 2) fmtChat: quando for mensagem do BOT (isBotMessage), exibir "[ Mensagem enviada, #N ]" e abaixo "#N = <texto>".
 * 3) Top Servers (renderTopSrv): já mostra MOTD se existir em allServers; renderTSC já mostra MOTD.
 */

// No appendLog, use esc() no emoji:
// ANTES: const h=`... ${e.emoji||''} ${esc(e.msg||'')}</div>`;
// DEPOIS:
function appendLogSafe(e){
    const p=document.getElementById('logsP'),dp=document.getElementById('dashLogs');
    const h=`<div class="log-e"><span class="log-t">[${esc(e.time||'')}]</span> ${esc(e.emoji||'')} ${esc(e.msg||'')}</div>`;
    p.insertAdjacentHTML('afterbegin',h);while(p.children.length>100)p.removeChild(p.lastChild);
    dp.insertAdjacentHTML('afterbegin',h);while(dp.children.length>10)dp.removeChild(dp.lastChild);
}

// fmtChat com "Mensagem enviada #N" para mensagens do bot:
function fmtChatWithSent(m){
    const bn=(typeof stats!=='undefined'&&stats.username?stats.username:'Anunciador').toLowerCase();
    const men=m.botMentioned;
    const isBot=m.isBotMessage;
    const srv=typeof allServers!=='undefined'?allServers.find(s=>s.key===m.serverKey):null;
    const motd=m.motd||(srv&&typeof cleanMotd==='function'?cleanMotd(srv.motd):'');
    const sentDot=m.sentiment==='positive'?'<span class="sentiment-dot positive"></span>':m.sentiment==='negative'?'<span class="sentiment-dot negative"></span>':'';
    const userClass=isBot?'cu bot-user':'cu';
    const esc=(t)=>{if(!t)return'';return String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');};
    if(isBot&&m.message){
        const idx=m.messageIndex!=null?m.messageIndex:(window._botMsgCount=(window._botMsgCount||0)+1);
        return `<div class="chat-msg">${sentDot}<span class="ct">${m.time||''}</span> <span class="cs">[${esc(m.serverKey)}${motd?' '+esc(motd):''}]</span> <span class="${userClass}">${esc(m.username||'?')}</span>: <span class="cx">[ Mensagem enviada, #${idx} ]</span></div><div class="chat-msg cm" style="padding-left:20px;font-size:11px;">#${idx} = ${esc(m.message)}</div>`;
    }
    return `<div class="chat-msg${men?' mentioned':''}">${sentDot}<span class="ct">${m.time||''}</span> <span class="cs">[${esc(m.serverKey)}${motd?' '+esc(motd):''}]</span> <span class="${userClass}">${esc(m.username||'?')}</span>: <span class="cx">${esc(m.message||'')}</span></div>`;
}
