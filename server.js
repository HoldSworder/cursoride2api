// ═══════════════════════════════════════════════════
//  CursorIDE2API v2 - 极简版
//  token.json → 反代 Cursor API → OpenAI 兼容接口
// ═══════════════════════════════════════════════════

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const cursorClient = require('./src/cursor-client');           // legacy: getModels only
const cursorProtoClient = require('./src/cursor-protobuf-client'); // new: chat w/ native tools
const converter = require('./src/converter');

// ── 配置 ──
const PORT = parseInt(process.env.PORT || '3000');
const HOST = process.env.HOST || '0.0.0.0';
const API_KEY = process.env.API_KEY || '';  // 留空 = 不校验
const TOKEN_FILE = process.env.TOKEN_FILE || path.join(__dirname, 'token.json');
const DEFAULT_MODEL = process.env.DEFAULT_MODEL || 'claude-4.5-sonnet';
const CLIENT_VERSION = process.env.CURSOR_CLIENT_VERSION || '2.6.20';

// ── 加载 Tokens ──
let tokens = [];
let roundRobinIndex = 0;

function loadTokens() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) {
      console.error(`  ❌ token.json not found: ${TOKEN_FILE}`);
      console.error('  📝 Create token.json with your Cursor credentials');
      process.exit(1);
    }
    const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
    tokens = (data.tokens || []).filter(t => t.accessToken && t.accessToken !== 'your-cursor-access-token-here');
    if (tokens.length === 0) {
      console.error('  ❌ No valid tokens in token.json');
      console.error('  📝 Add at least one token with a valid accessToken');
      process.exit(1);
    }
    return tokens.length;
  } catch (e) {
    console.error(`  ❌ Failed to load token.json: ${e.message}`);
    process.exit(1);
  }
}

// 监听 token.json 变化, 自动热更新
function watchTokenFile() {
  try {
    fs.watchFile(TOKEN_FILE, { interval: 5000 }, () => {
      try {
        const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
        const newTokens = (data.tokens || []).filter(t => t.accessToken && t.accessToken !== 'your-cursor-access-token-here');
        if (newTokens.length > 0) {
          tokens = newTokens;
          roundRobinIndex = 0;
          console.log(`  🔄 token.json reloaded: ${tokens.length} token(s)`);
        }
      } catch {}
    });
  } catch {}
}

// 轮询选 token
function pickToken() {
  if (tokens.length === 0) return null;
  roundRobinIndex = roundRobinIndex % tokens.length;
  const token = tokens[roundRobinIndex];
  roundRobinIndex++;
  return {
    accessToken: token.accessToken,
    machineId: token.machineId || '',
    macMachineId: token.macMachineId || '',
  };
}

// ── Express 应用 ──
const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// ── API Key 简单校验 ──
function checkApiKey(req, res, next) {
  if (!API_KEY) return next();
  const auth = req.headers.authorization;
  if (!auth) return res.status(401).json({ error: { message: 'Missing Authorization header', type: 'auth_error' } });
  const key = auth.replace(/^Bearer\s+/i, '');
  if (key !== API_KEY) return res.status(401).json({ error: { message: 'Invalid API key', type: 'auth_error' } });
  next();
}

// ── GET /v1/models ──
app.get('/v1/models', checkApiKey, async (req, res) => {
  try {
    const token = pickToken();
    if (!token) return res.json({ object: 'list', data: [] });
    const result = await cursorClient.getModels(token);
    res.json(converter.buildModelsResponse(result.models || []));
  } catch {
    res.json({ object: 'list', data: [] });
  }
});

// ── POST /v1/chat/completions ──
// Native tool_calls support via StreamUnifiedChatWithTools protobuf endpoint.
app.post('/v1/chat/completions', checkApiKey, async (req, res) => {
  const { messages, model, stream, tools, reasoning_effort } = req.body;

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json(converter.buildErrorResponse('messages is required', 'invalid_request_error', 400));
  }

  const token = pickToken();
  if (!token) {
    return res.status(503).json(converter.buildErrorResponse('No available tokens', 'server_error', 503));
  }

  const requestedModel = model || 'gpt-4';
  const cursorModel = converter.mapModel(requestedModel);
  const isStream = stream === true;
  const toolsCount = Array.isArray(tools) ? tools.length : 0;

  console.log(`  📨 [${new Date().toLocaleTimeString()}] ${requestedModel} → ${cursorModel} | stream=${isStream} | tools=${toolsCount} | msgs=${messages.length}`);

  const completionId = `chatcmpl-${uuidv4().replace(/-/g, '').substring(0, 24)}`;
  const created = Math.floor(Date.now() / 1000);

  // Build OpenAI streaming chunks consistent with spec
  const buildChunk = (delta, finishReason = null) => ({
    id: completionId,
    object: 'chat.completion.chunk',
    created,
    model: requestedModel,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  });

  if (isStream) {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    // Initial role chunk
    res.write(`data: ${JSON.stringify(buildChunk({ role: 'assistant', content: '' }))}\n\n`);

    try {
      const result = await cursorProtoClient.chat(token, messages, cursorModel, {
        stream: true,
        tools: tools || [],
        reasoningEffort: reasoning_effort || null,
        onText: (text) => {
          if (!res.writableEnded) {
            res.write(`data: ${JSON.stringify(buildChunk({ content: text }))}\n\n`);
          }
        },
        onToolCallDelta: ({ id, name, argumentsDelta, index, isFirstChunk }) => {
          if (res.writableEnded) return;
          const toolDelta = {
            tool_calls: [{
              index,
              ...(isFirstChunk ? { id, type: 'function' } : {}),
              function: {
                ...(isFirstChunk ? { name } : {}),
                arguments: argumentsDelta || '',
              },
            }],
          };
          res.write(`data: ${JSON.stringify(buildChunk(toolDelta))}\n\n`);
        },
      });

      const finishReason = (result.toolCalls?.length || 0) > 0 ? 'tool_calls' : 'stop';

      if (result.error && !result.text && (!result.toolCalls || result.toolCalls.length === 0)) {
        if (!res.writableEnded) {
          res.write(`data: ${JSON.stringify(buildChunk({ content: `\n\n[Error: ${result.error}]` }))}\n\n`);
        }
      }

      if (!res.writableEnded) {
        res.write(`data: ${JSON.stringify(buildChunk({}, finishReason))}\n\n`);
        res.write('data: [DONE]\n\n');
        res.end();
      }

      console.log(`  ✅ stream done | text=${result.text?.length || 0}c tools=${result.toolCalls?.length || 0}`);
    } catch (e) {
      console.error(`  ❌ stream error: ${e.message}`);
      if (!res.writableEnded) {
        res.write(`data: ${JSON.stringify(buildChunk({ content: `\n\n[Error: ${e.message}]` }))}\n\n`);
        res.write(`data: ${JSON.stringify(buildChunk({}, 'stop'))}\n\n`);
        res.write('data: [DONE]\n\n');
        res.end();
      }
    }
    return;
  }

  // Non-stream
  try {
    const result = await cursorProtoClient.chat(token, messages, cursorModel, {
      stream: false,
      tools: tools || [],
      reasoningEffort: reasoning_effort || null,
    });

    if (result.error && !result.text && (!result.toolCalls || result.toolCalls.length === 0)) {
      console.error(`  ❌ ${result.error}`);
      return res.status(result.status === 401 || result.status === 403 ? result.status : 500)
        .json(converter.buildErrorResponse(result.error));
    }

    const hasTools = (result.toolCalls?.length || 0) > 0;
    const finishReason = hasTools ? 'tool_calls' : 'stop';
    const message = { role: 'assistant', content: result.text || null };
    if (hasTools) {
      message.tool_calls = result.toolCalls.map((tc) => ({
        id: tc.id,
        type: tc.type || 'function',
        function: { name: tc.function.name, arguments: tc.function.arguments || '{}' },
      }));
    }

    res.json({
      id: completionId,
      object: 'chat.completion',
      created,
      model: requestedModel,
      choices: [{ index: 0, message, finish_reason: finishReason }],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    });
    console.log(`  ✅ done | text=${result.text?.length || 0}c tools=${result.toolCalls?.length || 0}`);
  } catch (e) {
    console.error(`  ❌ ${e.message}`);
    res.status(500).json(converter.buildErrorResponse(e.message));
  }
});

// ── 健康检查 ──
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    tokens: tokens.length,
    defaultModel: DEFAULT_MODEL,
    version: '2.0.0',
  });
});

// ── 启动 ──
const count = loadTokens();
watchTokenFile();

app.listen(PORT, HOST, () => {
  console.log('');
  console.log('  ╔═══════════════════════════════════════════╗');
  console.log('  ║       CursorIDE2API v2.0 (Lite)           ║');
  console.log('  ╠═══════════════════════════════════════════╣');
  console.log(`  ║  🌐 http://${HOST}:${PORT}                     ║`);
  console.log(`  ║  🔌 /v1/chat/completions                  ║`);
  console.log(`  ║  📋 /v1/models                            ║`);
  console.log('  ╠═══════════════════════════════════════════╣');
  console.log(`  ║  🔑 Tokens: ${String(count).padEnd(30)}║`);
  console.log(`  ║  🤖 Default: ${DEFAULT_MODEL.padEnd(29)}║`);
  console.log(`  ║  🔐 API Key: ${(API_KEY ? 'SET' : 'OPEN (no key)').padEnd(29)}║`);
  console.log('  ╚═══════════════════════════════════════════╝');
  console.log('');
});

// ── 优雅退出 ──
process.on('SIGINT', () => { console.log('\n  Bye!'); process.exit(0); });
process.on('SIGTERM', () => process.exit(0));
