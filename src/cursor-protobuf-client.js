// ═══════════════════════════════════════════════════════
//  Cursor IDE API client - Protobuf / StreamUnifiedChatWithTools
//  Native tool_calls support, adapted from decolua/9router
// ═══════════════════════════════════════════════════════

const { connectH2 } = require('./h2-proxy');
const { buildCursorHeaders } = require('./cursor-protobuf/checksum');
const {
  generateCursorBody,
  parseConnectRPCFrame,
  extractTextFromResponse,
} = require('./cursor-protobuf/protobuf');

const CURSOR_AUTHORITY = 'https://api2.cursor.sh';
const CHAT_PATH = '/aiserver.v1.ChatService/StreamUnifiedChatWithTools';

const DEBUG = process.env.CURSOR_DEBUG === '1';
const dlog = (...a) => DEBUG && console.log('[CURSOR]', ...a);

// ──────────────────────────────────────────────────────────────
// OpenAI message normalization → Cursor protobuf message list
// Mirrors 9router/open-sse/translator/request/openai-to-cursor.js
// (system→user prefix, tool→user xml block, assistant+tool_calls pass-through)
// ──────────────────────────────────────────────────────────────

function extractContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .filter((p) => p && typeof p === 'object' && p.type === 'text' && typeof p.text === 'string')
      .map((p) => p.text || '')
      .join('');
  }
  return '';
}

function sanitizeToolResultText(text) {
  return String(text || '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '');
}

function escapeXml(text) {
  return String(text || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function buildToolResultBlock(toolName, toolCallId, resultText) {
  return [
    '<tool_result>',
    `<tool_name>${escapeXml(toolName || 'tool')}</tool_name>`,
    `<tool_call_id>${escapeXml(toolCallId || '')}</tool_call_id>`,
    `<result>${escapeXml(sanitizeToolResultText(resultText))}</result>`,
    '</tool_result>',
  ].join('\n');
}

function convertOpenAIMessages(messages) {
  const out = [];
  const toolMeta = new Map();
  const remember = (id, name) => { if (id) toolMeta.set(id, { name: name || 'tool' }); };

  for (const m of messages || []) {
    if (m?.role === 'assistant' && Array.isArray(m.tool_calls)) {
      for (const tc of m.tool_calls) remember(tc.id, tc.function?.name);
    }
  }

  for (const m of messages || []) {
    if (!m || !m.role) continue;

    if (m.role === 'system') {
      const c = extractContent(m.content);
      if (c) out.push({ role: 'user', content: `[System Instructions]\n${c}` });
      continue;
    }

    if (m.role === 'tool') {
      const c = extractContent(m.content);
      const id = m.tool_call_id || '';
      const name = m.name || toolMeta.get(id)?.name || 'tool';
      out.push({ role: 'user', content: buildToolResultBlock(name, id, c) });
      continue;
    }

    if (m.role === 'user') {
      const c = extractContent(m.content);
      if (c) out.push({ role: 'user', content: c });
      continue;
    }

    if (m.role === 'assistant') {
      const c = extractContent(m.content);
      if (Array.isArray(m.tool_calls) && m.tool_calls.length > 0) {
        out.push({
          role: 'assistant',
          content: c || '',
          tool_calls: m.tool_calls.map(({ index, ...rest }) => rest),
        });
      } else if (c) {
        out.push({ role: 'assistant', content: c });
      }
      continue;
    }
  }

  return out;
}

// ──────────────────────────────────────────────────────────────
// Streaming HTTP/2 request → SSE-style callbacks
// ──────────────────────────────────────────────────────────────

/**
 * @param {{accessToken: string, machineId?: string, macMachineId?: string}} token
 * @param {Array} openaiMessages
 * @param {string} modelId
 * @param {object} options
 * @param {boolean} [options.stream]
 * @param {Array}  [options.tools]
 * @param {string} [options.reasoningEffort]  - "medium" | "high"
 * @param {(text:string)=>void} [options.onText]
 * @param {(tc:{id,name,argumentsDelta,index,isFirstChunk})=>void} [options.onToolCallDelta]
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<{text:string, toolCalls:Array, error?:string}>}
 */
async function chat(token, openaiMessages, modelId, options = {}) {
  const { stream = false, tools = [], reasoningEffort = null, onText, onToolCallDelta, signal } = options;

  const messages = convertOpenAIMessages(openaiMessages);
  const body = generateCursorBody(messages, modelId, tools, reasoningEffort, false);

  const machineId = token.machineId || undefined;
  const headers = buildCursorHeaders(token.accessToken, machineId, true);

  dlog('POST', CHAT_PATH, 'model=', modelId, 'msgs=', messages.length, 'tools=', tools.length, 'stream=', stream);

  const client = await connectH2(CURSOR_AUTHORITY);
  let resolved = false;
  let aborted = false;

  return await new Promise((resolve, reject) => {
    const finish = (fn) => (...args) => {
      if (resolved) return;
      resolved = true;
      try { client.close(); } catch {}
      fn(...args);
    };

    const onAbort = () => { aborted = true; try { req.close(); } catch {} };
    if (signal) {
      if (signal.aborted) { client.close(); return reject(new Error('aborted')); }
      signal.addEventListener('abort', onAbort, { once: true });
    }

    client.on('error', finish(reject));

    const req = client.request({
      ':method': 'POST',
      ':path': CHAT_PATH,
      ':authority': new URL(CURSOR_AUTHORITY).host,
      ':scheme': 'https',
      ...headers,
    });

    let respStatus = 0;
    let respHeaders = {};
    let buf = Buffer.alloc(0);
    let totalText = '';
    const toolCallsMap = new Map(); // id -> { index, function: {name, arguments}, isLast }
    const toolCalls = [];
    const emittedFirst = new Set();
    let errorMsg = null;

    req.on('response', (h) => {
      respStatus = h[':status'];
      respHeaders = h;
      dlog('response status=', respStatus);
    });

    req.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      // Try to drain as many full frames as we have.
      while (buf.length >= 5) {
        const flags = buf[0];
        const length = buf.readUInt32BE(1);
        if (buf.length < 5 + length) break;
        const frame = buf.slice(0, 5 + length);
        buf = buf.slice(5 + length);

        const parsed = parseConnectRPCFrame(frame);
        if (!parsed) continue;
        const payload = parsed.payload;

        // JSON error frame (starts with '{')
        if (payload.length > 0 && payload[0] === 0x7b) {
          try {
            const text = Buffer.from(payload).toString('utf-8');
            if (text.includes('"error"')) {
              dlog('error frame:', text.slice(0, 500));
              const hasContent = totalText || toolCallsMap.size > 0;
              if (!hasContent) errorMsg = text;
              continue;
            }
          } catch {}
        }

        let result;
        try {
          result = extractTextFromResponse(new Uint8Array(payload));
        } catch (e) {
          dlog('extract failed:', e.message);
          continue;
        }
        if (!result) continue;

        if (result.error) {
          const hasContent = totalText || toolCallsMap.size > 0;
          dlog('decoded error (hasContent=', hasContent, '):', result.error);
          if (!hasContent) errorMsg = result.error;
          continue;
        }

        if (result.toolCall) {
          const tc = result.toolCall;
          dlog('frame toolCall: id=', tc.id, 'name=', tc.function.name, 'argsLen=', (tc.function.arguments || '').length, 'isLast=', tc.isLast, 'args=', (tc.function.arguments || '').slice(0, 120));
          const existing = toolCallsMap.get(tc.id);
          const isFirst = !existing;
          if (existing) {
            existing.function.arguments += tc.function.arguments || '';
            existing.isLast = tc.isLast;
          } else {
            const idx = toolCallsMap.size;
            const entry = {
              id: tc.id,
              type: 'function',
              index: idx,
              function: { name: tc.function.name, arguments: tc.function.arguments || '' },
              isLast: tc.isLast,
            };
            toolCallsMap.set(tc.id, entry);
            toolCalls.push(entry);
          }
          if (onToolCallDelta) {
            const entry = toolCallsMap.get(tc.id);
            onToolCallDelta({
              id: entry.id,
              name: entry.function.name,
              argumentsDelta: tc.function.arguments || '',
              index: entry.index,
              isFirstChunk: isFirst && !emittedFirst.has(tc.id),
            });
            emittedFirst.add(tc.id);
          }
        }

        if (result.text) {
          totalText += result.text;
          if (onText) onText(result.text);
        }
      }
    });

    req.on('end', finish(() => {
      if (signal) signal.removeEventListener('abort', onAbort);
      if (respStatus !== 200 && !totalText && toolCalls.length === 0) {
        return resolve({
          text: '',
          toolCalls: [],
          error: errorMsg || `HTTP ${respStatus}`,
          status: respStatus,
        });
      }
      resolve({
        text: totalText,
        toolCalls: toolCalls.map(({ isLast, index, ...rest }) => ({ ...rest, index })),
        error: aborted ? 'aborted' : (errorMsg && !totalText && toolCalls.length === 0 ? errorMsg : null),
        status: respStatus,
      });
    }));

    req.on('error', finish(reject));

    req.write(body);
    req.end();
  });
}

module.exports = { chat, convertOpenAIMessages };
