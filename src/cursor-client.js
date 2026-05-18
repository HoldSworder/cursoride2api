// ═══════════════════════════════════════════════
//  CursorIDE2API - Cursor API 客户端
//  基于 agent.v1.AgentService/Run BiDi 协议
// ═══════════════════════════════════════════════

const http2 = require('http2');
const net = require('net');
const tls = require('tls');
const url = require('url');
const { v4: uuidv4 } = require('uuid');
const config = require('./config');

// ── HTTP CONNECT 代理支持 (HTTPS_PROXY/HTTP_PROXY) ──
// Node 原生 http2 不支持代理；这里通过 net.connect → CONNECT 隧道 →
// tls.connect (ALPN h2) → http2.connect(createConnection) 复用 socket。
function getProxyUrl(targetUrl) {
  const p = process.env.HTTPS_PROXY || process.env.https_proxy ||
            process.env.HTTP_PROXY  || process.env.http_proxy  || '';
  if (!p) return null;
  const noProxy = (process.env.NO_PROXY || process.env.no_proxy || '').split(',').map(s => s.trim()).filter(Boolean);
  try {
    const host = new URL(targetUrl).hostname;
    for (const rule of noProxy) {
      if (!rule) continue;
      if (rule === '*' || host === rule || host.endsWith('.' + rule.replace(/^\./, ''))) return null;
    }
  } catch {}
  return p;
}

function connectViaProxy(targetUrl, proxyUrl) {
  return new Promise((resolve, reject) => {
    const t = new URL(targetUrl);
    const p = new URL(proxyUrl);
    const targetHost = t.hostname;
    const targetPort = Number(t.port) || 443;
    const proxyHost  = p.hostname;
    const proxyPort  = Number(p.port) || (p.protocol === 'https:' ? 443 : 80);

    const sock = net.connect({ host: proxyHost, port: proxyPort });
    let buf = '';
    const onData = (chunk) => {
      buf += chunk.toString('binary');
      const idx = buf.indexOf('\r\n\r\n');
      if (idx < 0) return;
      const header = buf.slice(0, idx);
      sock.removeListener('data', onData);
      sock.removeListener('error', onErr);
      const statusLine = header.split('\r\n')[0] || '';
      const m = statusLine.match(/^HTTP\/1\.[01]\s+(\d{3})/);
      if (!m || m[1] !== '200') {
        sock.destroy();
        return reject(new Error(`Proxy CONNECT failed: ${statusLine}`));
      }
      // 升级到 TLS, 协商 ALPN h2
      const tlsSock = tls.connect({
        socket: sock,
        servername: targetHost,
        ALPNProtocols: ['h2'],
      }, () => {
        if (tlsSock.alpnProtocol !== 'h2') {
          tlsSock.destroy();
          return reject(new Error(`Proxy ALPN negotiation failed: got ${tlsSock.alpnProtocol}`));
        }
        resolve(tlsSock);
      });
      tlsSock.once('error', reject);
    };
    const onErr = (e) => { sock.destroy(); reject(e); };
    sock.once('error', onErr);
    sock.on('data', onData);

    const auth = (p.username || p.password)
      ? 'Proxy-Authorization: Basic ' + Buffer.from(`${decodeURIComponent(p.username)}:${decodeURIComponent(p.password)}`).toString('base64') + '\r\n'
      : '';
    sock.write(
      `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\n` +
      `Host: ${targetHost}:${targetPort}\r\n` +
      auth +
      `\r\n`
    );
  });
}

/**
 * 创建 HTTP/2 连接 (透明支持 HTTPS_PROXY)
 * @param {string} authority - e.g. 'https://api2.cursor.sh'
 * @returns {Promise<Http2Session>}
 */
async function connectH2(authority) {
  const proxy = getProxyUrl(authority);
  if (!proxy) return http2.connect(authority);
  const sock = await connectViaProxy(authority, proxy);
  return http2.connect(authority, { createConnection: () => sock });
}

// ── Checksum 生成 ──
function generateChecksum(machineId, macMachineId) {
  let k = 165;
  const t = Math.floor(Date.now() / 1e6);
  const b = new Uint8Array([
    (t >> 40) & 255, (t >> 32) & 255, (t >> 24) & 255,
    (t >> 16) & 255, (t >> 8) & 255, t & 255,
  ]);
  for (let i = 0; i < b.length; i++) {
    b[i] = ((b[i] ^ k) + (i % 256)) & 0xFF;
    k = b[i];
  }
  const prefix = Buffer.from(b).toString('base64');
  return macMachineId ? `${prefix}${machineId}/${macMachineId}` : `${prefix}${machineId}`;
}

// ── Envelope 帧编码 ──
function encodeFrame(obj) {
  const jsonBuf = Buffer.from(JSON.stringify(obj), 'utf8');
  const frame = Buffer.alloc(5 + jsonBuf.length);
  frame[0] = 0;
  frame.writeUInt32BE(jsonBuf.length, 1);
  jsonBuf.copy(frame, 5);
  return frame;
}

// ── 构建请求头 ──
function buildHeaders(token) {
  return {
    ':method': 'POST',
    ':path': '/agent.v1.AgentService/Run',
    'content-type': 'application/connect+json',
    'connect-protocol-version': '1',
    'authorization': `Bearer ${token.accessToken}`,
    'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
    'x-cursor-client-version': config.cursor.clientVersion,
    'x-cursor-timezone': Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
    'x-request-id': uuidv4(),
  };
}

// ── 处理 ExecServerMessage ──
function handleExecMessage(exec, writeFrame) {
  const { id = 0, execId = '' } = exec;

  if (exec.requestContextArgs) {
    writeFrame({
      execClientMessage: {
        id, execId,
        requestContextResult: {
          success: {
            requestContext: {
              env: {
                operatingSystem: process.platform === 'win32' ? 'windows' : process.platform,
                defaultShell: process.platform === 'win32' ? 'powershell' : 'bash',
              },
            },
          },
        },
      },
    });
    return 'requestContext';
  }

  if (exec.readArgs) {
    writeFrame({ execClientMessage: { id, execId, readResult: { fileNotFound: {} } } });
    return 'read';
  }

  if (exec.lsArgs) {
    writeFrame({ execClientMessage: { id, execId, lsResult: { error: { path: '', error: 'Headless mode' } } } });
    return 'ls';
  }

  if (exec.shellArgs) {
    writeFrame({ execClientMessage: { id, execId, shellResult: { rejected: { reason: 'Headless mode' } } } });
    return 'shell';
  }

  if (exec.grepArgs) {
    writeFrame({ execClientMessage: { id, execId, grepResult: { error: { error: 'Headless mode' } } } });
    return 'grep';
  }

  if (exec.writeArgs) {
    writeFrame({ execClientMessage: { id, execId, writeResult: {} } });
    return 'write';
  }

  if (exec.deleteArgs) {
    writeFrame({ execClientMessage: { id, execId, deleteResult: { error: { path: '', error: 'Headless mode' } } } });
    return 'delete';
  }

  if (exec.diagnosticsArgs) {
    writeFrame({ execClientMessage: { id, execId, diagnosticsResult: { diagnostics: [] } } });
    return 'diagnostics';
  }

  // Unknown exec type
  writeFrame({ execClientMessage: { id, execId, requestContextResult: { error: { error: 'Unknown exec type' } } } });
  return 'unknown';
}

/**
 * 调用 Cursor AgentService.Run
 * 
 * @param {Object} token - { accessToken, machineId, macMachineId }
 * @param {string} prompt - 用户消息
 * @param {string} modelId - 模型 ID
 * @param {Object} options - { conversationId, stream, onDelta, onThinking, signal }
 * @returns {Promise<{ text, inputTokens, outputTokens, thinkingMs, error }>}
 */
function chat(token, prompt, modelId, options = {}) {
  const {
    conversationId = uuidv4(),
    stream = false,
    onDelta = null,        // (text) => void
    onThinking = null,     // (text) => void
    signal = null,         // AbortSignal
  } = options;

  return new Promise(async (resolve, reject) => {
    let client;
    try {
      client = await connectH2(config.cursor.baseUrl);
    } catch (e) {
      return reject(new Error(`Connection failed: ${e.message}`));
    }

    client.on('error', (e) => {
      // Suppress unhandled errors after cleanup
    });

    const req = client.request(buildHeaders(token));
    req.setTimeout(config.cursor.requestTimeout);

    let fullText = '';
    let thinkingText = '';
    let inputTokens = 0;
    let outputTokens = 0;
    let thinkingMs = 0;
    let buffer = Buffer.alloc(0);
    let done = false;
    let errorMsg = '';

    // 写帧辅助
    const writeFrameFn = (obj) => {
      try { req.write(encodeFrame(obj)); } catch {}
    };

    // 心跳
    const heartbeat = setInterval(() => {
      writeFrameFn({ clientHeartbeat: {} });
    }, config.cursor.heartbeatInterval);

    // 取消信号
    if (signal) {
      signal.addEventListener('abort', () => {
        cleanup();
        reject(new Error('Request aborted'));
      }, { once: true });
    }

    function cleanup() {
      if (done) return;
      done = true;
      clearInterval(heartbeat);
      try { req.end(); } catch {}
      setTimeout(() => {
        try { req.close(); } catch {}
        try { client.close(); } catch {}
      }, 200);
    }

    function finish() {
      cleanup();
      if (errorMsg) {
        resolve({ text: fullText, inputTokens, outputTokens, thinkingMs, error: errorMsg });
      } else {
        resolve({ text: fullText, inputTokens, outputTokens, thinkingMs, error: null });
      }
    }

    req.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      let offset = 0;
      while (offset + 5 <= buffer.length) {
        const len = buffer.readUInt32BE(offset + 1);
        if (offset + 5 + len > buffer.length) break;
        const s = buffer.slice(offset + 5, offset + 5 + len).toString('utf8');
        offset += 5 + len;

        try {
          const msg = JSON.parse(s);

          // 错误
          if (msg.error) {
            let detail = '';
            if (msg.error.details?.[0]?.value) {
              try { detail = Buffer.from(msg.error.details[0].value, 'base64').toString('utf8'); } catch {}
            }
            errorMsg = detail || msg.error.message || msg.error.code || 'Unknown error';
            finish();
            return;
          }

          // ExecServerMessage
          if (msg.execServerMessage) {
            handleExecMessage(msg.execServerMessage, writeFrameFn);
            continue;
          }

          // KV (忽略, 不需要处理)
          if (msg.kvServerMessage) continue;

          // InteractionUpdate
          if (msg.interactionUpdate) {
            const iu = msg.interactionUpdate;

            // 心跳
            if (iu.heartbeat !== undefined) continue;

            // 文本增量
            if (iu.textDelta) {
              const t = typeof iu.textDelta === 'string' ? iu.textDelta : (iu.textDelta.text || iu.textDelta.delta || '');
              if (t) {
                fullText += t;
                if (onDelta) onDelta(t);
              }
              continue;
            }

            // 思考增量
            if (iu.thinkingDelta) {
              const t = iu.thinkingDelta.text || iu.thinkingDelta.delta || '';
              if (t && onThinking) {
                thinkingText += t;
                onThinking(t);
              }
              continue;
            }

            // 思考完成
            if (iu.thinkingCompleted) {
              thinkingMs = iu.thinkingCompleted.thinkingDurationMs || 0;
              continue;
            }

            // 回合结束
            if (iu.turnEnded) {
              inputTokens = parseInt(iu.turnEnded.inputTokens || '0');
              outputTokens = parseInt(iu.turnEnded.outputTokens || '0');
              finish();
              return;
            }

            // Token 增量
            if (iu.tokenDelta) continue;
            if (iu.stepCompleted) continue;

            // message 嵌套的情况
            const m = iu.message;
            if (m) {
              if (m.textDelta) {
                const t = typeof m.textDelta === 'string' ? m.textDelta : (m.textDelta.text || m.textDelta.delta || '');
                if (t) { fullText += t; if (onDelta) onDelta(t); }
              }
              if (m.turnEnded) {
                inputTokens = parseInt(m.turnEnded.inputTokens || '0');
                outputTokens = parseInt(m.turnEnded.outputTokens || '0');
                finish();
                return;
              }
              if (m.thinkingCompleted) {
                thinkingMs = m.thinkingCompleted.thinkingDurationMs || 0;
              }
            }

            continue;
          }

          // Checkpoint, InteractionQuery 等忽略
          if (msg.conversationCheckpointUpdate) continue;
          if (msg.interactionQuery) continue;

        } catch {}
      }
      buffer = buffer.slice(offset);
    });

    req.on('end', finish);
    req.on('error', (e) => { errorMsg = e.message; finish(); });
    req.on('timeout', () => { errorMsg = 'Request timeout'; finish(); });

    // ━━━ 发送 runRequest ━━━
    writeFrameFn({
      runRequest: {
        conversationState: {},
        action: {
          userMessageAction: {
            userMessage: { text: prompt },
          },
        },
        modelDetails: {
          modelId,
          displayName: modelId,
          displayNameShort: modelId,
        },
        requestedModel: { modelId },
        conversationId,
      },
    });
  });
}

/**
 * 获取可用模型列表
 */
function getModels(token) {
  return new Promise(async (resolve, reject) => {
    let client;
    try {
      client = await connectH2(config.cursor.baseUrl);
    } catch (e) {
      return reject(new Error(`Connection failed: ${e.message}`));
    }
    client.on('error', () => {});
    const req = client.request({
      ':method': 'POST',
      ':path': '/agent.v1.AgentService/GetUsableModels',
      'content-type': 'application/json',
      'connect-protocol-version': '1',
      'authorization': `Bearer ${token.accessToken}`,
      'x-cursor-checksum': generateChecksum(token.machineId || '', token.macMachineId || ''),
      'x-cursor-client-version': config.cursor.clientVersion,
      'x-request-id': uuidv4(),
    });
    req.setTimeout(15000);
    let body = '';
    req.on('data', c => body += c.toString());
    req.on('end', () => {
      client.close();
      try { resolve(JSON.parse(body)); } catch { resolve({ models: [] }); }
    });
    req.on('error', e => { client.close(); reject(e); });
    req.on('timeout', () => { req.close(); client.close(); reject(new Error('Timeout')); });
    req.write(JSON.stringify({}));
    req.end();
  });
}

/**
 * 健康检查 (通过 GetUsableModels)
 */
async function healthCheck(token) {
  try {
    const result = await getModels(token);
    return { ok: true, models: result.models?.length || 0 };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

module.exports = { chat, getModels, healthCheck, generateChecksum };
