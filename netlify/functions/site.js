// Netlify runs this for every page and API route. Files in public/ are served
// by the CDN first (preferStatic), so app.js, app.css and engine.js skip the function.

import { Readable } from 'node:stream';
import { createApp, loadEnv } from '../../server.js';

loadEnv();
const { handle } = createApp();

function nodeRequest(request, body) {
  let started = false;
  const req = new Readable({
    read() {
      if (started) return;
      started = true;
      if (body.length) this.push(body);
      this.push(null);
    },
  });
  const url = new URL(request.url);
  req.method = request.method;
  req.url = `${url.pathname}${url.search}`;
  req.headers = Object.fromEntries(request.headers);
  return req;
}

function nodeResponse() {
  let status = 200;
  const headers = {};
  const chunks = [];
  let ended = false;
  const res = {
    get writableEnded() { return ended; },
    writeHead(code, hdrs) {
      status = code;
      if (hdrs) Object.assign(headers, hdrs);
      return res;
    },
    end(data) {
      if (data) chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(String(data)));
      ended = true;
      return res;
    },
  };
  return {
    res,
    result() {
      return { status, headers, body: Buffer.concat(chunks) };
    },
  };
}

export default async function site(request) {
  const body = request.method === 'GET' || request.method === 'HEAD'
    ? Buffer.alloc(0)
    : Buffer.from(await request.arrayBuffer());
  const bridge = nodeResponse();
  await handle(nodeRequest(request, body), bridge.res);
  if (!bridge.res.writableEnded) {
    return new Response('Something went wrong on the server.', { status: 500, headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
  }

  const { status, headers, body: payload } = bridge.result();
  const webHeaders = new Headers();
  for (const [key, value] of Object.entries(headers)) {
    if (value != null) webHeaders.set(key, String(value));
  }
  const empty = request.method === 'HEAD' || status === 204 || status === 304;
  if (empty) webHeaders.delete('content-length');
  return new Response(empty ? null : payload, { status, headers: webHeaders });
}

export const config = {
  path: '/*',
  preferStatic: true,
  excludedPath: ['/app.js', '/app.css', '/engine.js'],
};
