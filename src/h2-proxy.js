// HTTP/2 connection with optional HTTPS_PROXY CONNECT tunnel.
// Node native http2 does not honor HTTPS_PROXY; tunnel manually via net→CONNECT→tls→http2.

const http2 = require('http2');
const net = require('net');
const tls = require('tls');

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
 * Create an HTTP/2 client session, transparently routing through HTTPS_PROXY when set.
 * @param {string} authority - e.g. 'https://api2.cursor.sh'
 * @returns {Promise<import('http2').ClientHttp2Session>}
 */
async function connectH2(authority) {
  const proxy = getProxyUrl(authority);
  if (!proxy) return http2.connect(authority);
  const sock = await connectViaProxy(authority, proxy);
  return http2.connect(authority, { createConnection: () => sock });
}

module.exports = { connectH2, getProxyUrl };
