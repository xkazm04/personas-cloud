// ---------------------------------------------------------------------------
// Filtering HTTP CONNECT proxy for per-persona network policies
// ---------------------------------------------------------------------------

import http from 'node:http';
import net from 'node:net';
import type { Duplex } from 'node:stream';
import type { Logger } from 'pino';
import { type NetworkPolicyRule, isAllowed, type NetworkPolicy } from '@dac-cloud/shared';

export interface ProxyHandle {
  port: number;
  cleanup: () => void;
}

export async function startFilterProxy(
  policy: NetworkPolicy,
  logger: Logger,
  executionId: string,
): Promise<ProxyHandle> {
  const activeSockets = new Set<net.Socket | Duplex>();
  let allowedCount = 0;
  let blockedCount = 0;

  const server = http.createServer((_req, res) => {
    // Non-CONNECT requests through proxy — check and forward or reject
    const url = _req.url;
    if (!url) { res.writeHead(400).end(); return; }
    try {
      const parsed = new URL(url);
      const port = parseInt(parsed.port, 10) || (parsed.protocol === 'https:' ? 443 : 80);
      if (!isAllowed(policy, parsed.hostname, port)) {
        blockedCount++;
        logger.warn({ executionId, host: parsed.hostname, port }, 'Proxy: blocked non-CONNECT request');
        res.writeHead(403, { 'Content-Type': 'text/plain' }).end('Blocked by network policy');
        return;
      }
      allowedCount++;
    } catch (err) {
      logger.warn({ err, executionId, operation: 'proxy_parse_url' }, 'Proxy URL parse error');
      res.writeHead(400).end();
      return;
    }
    // For allowed non-CONNECT, pass through (minimal — most traffic uses CONNECT)
    res.writeHead(501).end('Direct proxy not supported, use CONNECT');
  });

  server.on('connect', (req, clientSocket, head) => {
    const target = req.url ?? '';
    let host: string;
    let port: number;

    // Parse CONNECT target, handling both IPv4 "host:port" and IPv6 "[::1]:port"
    if (target.startsWith('[')) {
      const closeBracket = target.indexOf(']');
      host = closeBracket > 0 ? target.slice(1, closeBracket) : '';
      const portStr = closeBracket > 0 ? target.slice(closeBracket + 2) : '';
      port = parseInt(portStr, 10) || 443;
    } else {
      const lastColon = target.lastIndexOf(':');
      host = lastColon > 0 ? target.slice(0, lastColon) : target;
      port = lastColon > 0 ? parseInt(target.slice(lastColon + 1), 10) || 443 : 443;
    }

    if (!host || !isAllowed(policy, host, port)) {
      blockedCount++;
      logger.warn({ executionId, host, port }, 'Proxy: blocked CONNECT');
      clientSocket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
      clientSocket.destroy();
      return;
    }

    allowedCount++;

    const serverSocket = net.connect(port, host, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      serverSocket.write(head);
      serverSocket.pipe(clientSocket);
      clientSocket.pipe(serverSocket);
    });

    activeSockets.add(clientSocket);
    activeSockets.add(serverSocket);

    const removeSocket = (s: net.Socket | Duplex) => { activeSockets.delete(s); };

    serverSocket.on('close', () => removeSocket(serverSocket));
    clientSocket.on('close', () => removeSocket(clientSocket));

    serverSocket.on('error', (err) => {
      logger.error({ err, executionId, host, port }, 'Proxy: upstream connection error');
      clientSocket.destroy();
    });

    clientSocket.on('error', (err) => {
      logger.warn({ err, executionId, host, port, operation: 'proxy_client_socket' }, 'Proxy: client socket error');
      serverSocket.destroy();
    });
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as net.AddressInfo;
      logger.info({ executionId, port: addr.port, mode: policy.mode, rules: policy.allowedHosts?.length ?? 0 }, 'Network filter proxy started');
      resolve({
        port: addr.port,
        cleanup: () => {
          logger.info({ executionId, allowedCount, blockedCount }, 'Network filter proxy stopping');
          for (const socket of activeSockets) {
            socket.destroy();
          }
          activeSockets.clear();
          server.close();
        },
      });
    });
  });
}
