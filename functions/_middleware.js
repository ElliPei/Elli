// 玄 · API 中转 (Cloudflare Pages Functions 版)
// 部署: 放在项目根目录 functions/ 下,文件名 _middleware.js
// 访问: https://mystic-api.pages.dev/chat 等

const FREE_MODEL = {
  baseURL: 'https://open.bigmodel.cn/api/anthropic/v1/messages',
  apiKey: 'de232fb794654816a2a774076d7c3ff8.iT21h9HWaZE094Vf',
  model: 'glm-4.5-flash',
  protocol: 'anthropic',
};
const PAID_MODEL = {
  baseURL: 'https://api.deepseek.com/chat/completions',
  apiKey: 'sk-REPLACE_WITH_YOUR_DEEPSEEK_KEY',
  model: 'deepseek-chat',
  protocol: 'openai',
};
const ZHIPU_API_KEY = 'de232fb794654816a2a774076d7c3ff8.iT21h9HWaZE094Vf';
const ZHIPU_URL = 'https://open.bigmodel.cn/api/anthropic/v1/messages';
const FREE_LIMITS = { chat: 5, combined: 3 };
const ACTIVATION_CODES = {
  'M-TEST-2026': { level: 'monthly', days: 30 },
  'Y-TEST-2026': { level: 'yearly', days: 365 },
  'M-XC01-2026': { level: 'monthly', days: 30 },
  'Y-XC01-2026': { level: 'yearly', days: 365 },
  'M-FREE-2026': { level: 'monthly', days: 30 },
};

// Pages Functions 入口:拦截所有 /api/* 请求
export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  // CORS 预检
  if (method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  try {
    if (path === '/chat' && method === 'POST') {
      return await handleChat(request, env);
    } else if (path === '/translate' && method === 'POST') {
      return await handleTranslate(request);
    } else if (path === '/activate' && method === 'POST') {
      return await handleActivate(request, env);
    } else if (path === '/member' && method === 'POST') {
      return await handleMember(request, env);
    } else if (path === '/quota' && method === 'GET') {
      return await handleQuotaGet(request, env);
    } else if (path === '/quota' && method === 'POST') {
      return await handleQuotaPost(request, env);
    }
    return json({ error: 'Not Found', path }, 404);
  } catch (e) {
    return json({ error: String(e) }, 500);
  }
}

// ===== AI对话流式(SSE) =====
async function handleChat(request, env) {
  const { uid, system, messages, max_tokens } = await request.json();
  if (!uid || !messages) return json({ error: 'uid and messages required' }, 400);

  const member = await getMember(env, uid);
  const isMember = member.level !== 'free' && member.expireAt && member.expireAt > Date.now();
  if (!isMember) {
    const quota = await getQuota(env, uid);
    if ((quota.chat || 0) >= FREE_LIMITS.chat) {
      return json({ error: 'quota_exceeded', used: quota.chat, limit: FREE_LIMITS.chat }, 429);
    }
  }

  const model = isMember ? PAID_MODEL : FREE_MODEL;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        if (model.protocol === 'anthropic') {
          await streamAnthropic(controller, encoder, system, messages, max_tokens || 8192, uid, env, isMember, model);
        } else {
          await streamOpenAI(controller, encoder, system, messages, max_tokens || 8192, uid, env, isMember, model);
        }
      } catch (e) {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'error', error: String(e) })}\n\n`));
      }
      controller.close();
    }
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      ...corsHeaders(),
    }
  });
}

async function streamAnthropic(controller, encoder, system, messages, maxTokens, uid, env, isMember, model) {
  const resp = await fetch(model.baseURL, {
    method: 'POST',
    headers: { 'x-api-key': model.apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: model.model, max_tokens: maxTokens, system, messages, stream: true, thinking: { type: 'disabled' } }),
  });
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '', gotText = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) {
      if (!line.startsWith('data:')) continue;
      const data = line.slice(5).trim();
      if (!data) continue;
      let v; try { v = JSON.parse(data); } catch { continue; }
      if (v.type === 'error') {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'error', error: (v.error && v.error.message) || 'upstream' })}\n\n`));
        return;
      }
      if (v.type === 'content_block_delta' && v.delta && v.delta.type === 'text_delta' && v.delta.text) {
        gotText = true;
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'chunk', text: v.delta.text })}\n\n`));
      }
    }
  }
  if (gotText && !isMember) await incrQuota(env, uid, 'chat');
  controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'done' })}\n\n`));
}

async function streamOpenAI(controller, encoder, system, messages, maxTokens, uid, env, isMember, model) {
  const resp = await fetch(model.baseURL, {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + model.apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: model.model, max_tokens: maxTokens, messages: system ? [{ role: 'system', content: system }, ...messages] : messages, stream: true }),
  });
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '', gotText = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) {
      if (!line.startsWith('data:')) continue;
      const data = line.slice(5).trim();
      if (data === '[DONE]') continue;
      let v; try { v = JSON.parse(data); } catch { continue; }
      const delta = v.choices && v.choices[0] && v.choices[0].delta && v.choices[0].delta.content;
      if (delta) {
        gotText = true;
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'chunk', text: delta })}\n\n`));
      }
    }
  }
  if (gotText && !isMember) await incrQuota(env, uid, 'chat');
  controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'done' })}\n\n`));
}

// ===== 古籍翻译 =====
async function handleTranslate(request) {
  const { text, book_title, chapter_title } = await request.json();
  if (!text) return json({ error: 'text required' }, 400);
  const system = '你是古籍翻译助手。用现代白话准确翻译古文,涉及术语用括号简要解释,不编造原文没有的内容,只输出翻译结果。用中文回答。';
  const user = `典籍:《${book_title || ''}》 章节:${chapter_title || ''}\n\n原文:\n${text}`;
  const resp = await fetch(ZHIPU_URL, {
    method: 'POST',
    headers: { 'x-api-key': ZHIPU_API_KEY, 'Content-Type': 'application/json', 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'glm-4.5-flash', max_tokens: 2048, system, messages: [{ role: 'user', content: user }] }),
  });
  const data = await resp.json();
  let result = '';
  if (data.content) for (const b of data.content) { if (b.type === 'text') result += b.text; }
  if (!result) return json({ error: 'empty translation' }, 502);
  return json({ translation: result });
}

// ===== 激活码 =====
async function handleActivate(request, env) {
  const { uid, code } = await request.json();
  if (!uid || !code) return json({ error: 'uid and code required' }, 400);
  const entry = ACTIVATION_CODES[code.trim().toUpperCase()];
  if (!entry) return json({ ok: false, error: '激活码无效' }, 400);
  const usedBy = env.MEMBER_KV ? await env.MEMBER_KV.get('code_used_' + code.trim().toUpperCase()) : null;
  if (usedBy && usedBy !== uid) return json({ ok: false, error: '激活码已被使用' }, 400);
  const expireAt = Date.now() + entry.days * 86400000;
  if (env.MEMBER_KV) {
    await env.MEMBER_KV.put('member_' + uid, JSON.stringify({ level: entry.level, expireAt, activatedAt: Date.now() }));
    await env.MEMBER_KV.put('code_used_' + code.trim().toUpperCase(), uid);
  }
  return json({ ok: true, level: entry.level, days: entry.days, expireAt });
}

// ===== 会员状态 =====
async function handleMember(request, env) {
  const { uid } = await request.json();
  if (!uid) return json({ error: 'uid required' }, 400);
  const member = await getMember(env, uid);
  return json(member);
}

// ===== 额度 =====
async function handleQuotaGet(request, env) {
  const url = new URL(request.url);
  const uid = url.searchParams.get('uid');
  if (!uid) return json({ error: 'uid required' }, 400);
  const quota = await getQuota(env, uid);
  return json(quota);
}

async function handleQuotaPost(request, env) {
  const { uid, type } = await request.json();
  if (!uid || !type) return json({ error: 'uid and type required' }, 400);
  await incrQuota(env, uid, type);
  const quota = await getQuota(env, uid);
  return json(quota);
}

// ===== KV helpers =====
async function getMember(env, uid) {
  if (!env || !env.MEMBER_KV) return { level: 'free' };
  const raw = await env.MEMBER_KV.get('member_' + uid);
  const m = raw ? JSON.parse(raw) : { level: 'free' };
  if (m.expireAt && m.expireAt < Date.now()) m.level = 'free';
  return m;
}
async function getQuota(env, uid) {
  if (!env || !env.MEMBER_KV) return { chat: 0, combined: 0 };
  const today = new Date().toISOString().slice(0, 10);
  const raw = await env.MEMBER_KV.get('quota_' + uid + '_' + today);
  return raw ? JSON.parse(raw) : { chat: 0, combined: 0 };
}
async function incrQuota(env, uid, type) {
  if (!env || !env.MEMBER_KV) return;
  const today = new Date().toISOString().slice(0, 10);
  const quota = await getQuota(env, uid);
  quota[type] = (quota[type] || 0) + 1;
  const endOfDay = new Date(today + 'T00:00:00Z');
  endOfDay.setUTCDate(endOfDay.getUTCDate() + 1);
  const ttl = Math.max(60, Math.floor((endOfDay.getTime() - Date.now()) / 1000));
  await env.MEMBER_KV.put('quota_' + uid + '_' + today, JSON.stringify(quota), { expirationTtl: ttl });
}

// ===== utils =====
function json(obj, status) {
  return new Response(JSON.stringify(obj), { status: status || 200, headers: { 'Content-Type': 'application/json', ...corsHeaders() } });
}
function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}
