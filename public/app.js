// Kafka Lab browser code. Every page loads this one file; <body data-page> picks which page to start.

import {
  analyzeDesign, autoLayout, CATEGORIES, changeLines, clock, COMPONENTS, defaultsFor, failureOptions, fmtDuration, isAutoscaled, KAFKA_CONSUMERS, LiveRun, NODE_H, NODE_W, normalizeGraph, param, paramSpec, problemTraffic, REQUIREMENT_LABELS, runSuite, runTestCase, simulate, solveSuite, solveTest, times, WARM_SECONDS,
} from '/engine.js';

// ================================================================ core: ui
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ESC[c]);

class Raw {
  constructor(v) { this.v = v; }
  toString() { return this.v; }
}
const raw = (v) => new Raw(String(v));

function render(v) {
  if (v === null || v === undefined || v === false) return '';
  if (v instanceof Raw) return v.v;
  if (Array.isArray(v)) return v.map(render).join('');
  return esc(v);
}

// Escaping HTML template: interpolations are escaped unless they come from html`` or raw().
function html(strings, ...values) {
  let out = '';
  strings.forEach((s, i) => {
    out += s;
    if (i < values.length) out += render(values[i]);
  });
  return new Raw(out);
}

function setHtml(el, content) {
  el.innerHTML = render(content);
  return el;
}

function pageData() {
  const el = document.getElementById('page-data');
  if (!el) return {};
  try {
    return JSON.parse(el.textContent);
  } catch {
    return {};
  }
}

function debounce(fn, ms) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function fmtNum(n) {
  const v = Number(n) || 0;
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1)}M`;
  if (Math.abs(v) >= 1e4) return `${Math.round(v / 1e3)}k`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return `${Math.round(v)}`;
}
const fmtPct = (v, digits = 0) => `${(Number(v || 0) * 100).toFixed(digits)}%`;
const fmtLoad = (util) => {
  const p = Math.round(Number(util || 0) * 100);
  return p > 999 ? '999%+' : `${p}%`;
};
const fmtMs = (ms) => (ms >= 1000 ? `${(ms / 1000).toFixed(ms >= 10000 ? 0 : 1)} s` : `${Math.round(ms)} ms`);
const fmtDateTime = (iso) => (iso ? new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '');

let toastHost;
function toast(message, kind = 'info', ms = 3800) {
  if (!toastHost) {
    toastHost = document.createElement('div');
    toastHost.className = 'toasts';
    toastHost.setAttribute('role', 'status');
    toastHost.setAttribute('aria-live', 'polite');
    document.body.appendChild(toastHost);
  }
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = message;
  toastHost.appendChild(el);
  setTimeout(() => {
    el.classList.add('leaving');
    setTimeout(() => el.remove(), 300);
  }, ms);
}

function errorText(err) {
  return err?.message || 'Something went wrong.';
}

// Minimal promise-based dialog. Body is trusted HTML built with html``.
// An action's value may be a function of the dialog element, read before it closes.
function dialog({ title, body = '', actions = [{ label: 'OK', value: true, primary: true }] }) {
  return new Promise((resolve) => {
    const wrap = document.createElement('div');
    wrap.className = 'modal-backdrop';
    setHtml(wrap, html`<div class="modal" role="dialog" aria-modal="true" aria-label="${title}">
      <h2>${title}</h2>
      <div class="dialog-body">${body}</div>
      <div class="modal-actions">${actions.map((a, i) => html`<button type="button" class="btn ${a.primary ? 'btn-primary' : a.danger ? 'btn-danger' : 'btn-ghost'}" data-i="${i}">${a.label}</button>`)}</div>
    </div>`);
    const close = (value) => {
      const result = typeof value === 'function' ? value(wrap) : value;
      wrap.remove();
      document.removeEventListener('keydown', onKey);
      resolve(result);
    };
    const onKey = (e) => {
      if (e.key === 'Escape') close(null);
      if (e.key === 'Enter' && e.target.matches?.('.modal input')) {
        e.preventDefault();
        wrap.querySelector('.btn-primary')?.click();
      }
    };
    wrap.addEventListener('click', (e) => {
      if (e.target === wrap) return close(null);
      const btn = e.target.closest('button[data-i]');
      if (btn) close(actions[Number(btn.dataset.i)].value);
    });
    document.addEventListener('keydown', onKey);
    document.body.appendChild(wrap);
    const first = wrap.querySelector('.modal input, .modal textarea, .modal select');
    (first || wrap.querySelector('.btn-primary') || wrap.querySelector('.btn'))?.focus();
    first?.select?.();
  });
}

function busy(button, on, label) {
  if (!button) return;
  if (on) {
    button.dataset.label = button.innerHTML;
    button.disabled = true;
    setHtml(button, html`<span class="spinner"></span>${label || 'Working…'}`);
  } else {
    button.disabled = false;
    if (button.dataset.label) button.innerHTML = button.dataset.label;
  }
}

// ================================================================ core: api
class ApiError extends Error {
  constructor(status, message, data) {
    super(message);
    this.status = status;
    this.data = data;
  }
}

async function api(path, { method = 'GET', body, signal } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  let res;
  try {
    res = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal });
  } catch (err) {
    if (err.name === 'AbortError') throw err;
    throw new ApiError(0, 'Could not reach the server. Is it still running?');
  }
  const data = await res.json().catch(() => null);
  if (!res.ok) throw new ApiError(res.status, data?.error || `Request failed (${res.status}).`, data);
  return data;
}

// ================================================================ core: interview store
// The latest mock interview, kept only in this browser. Starting a new one replaces it.
const KEY = 'kafkalab:interview';
const CANVAS_PREFIX = 'kafkalab:canvas:interview:';

// randomUUID needs a secure context, which a LAN address over plain http is not.
const newId = () => (globalThis.crypto?.randomUUID
  ? crypto.randomUUID()
  : `iv-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`);

function loadInterview(id = null) {
  try {
    const session = JSON.parse(localStorage.getItem(KEY) || 'null');
    return session && (!id || session.id === id) ? session : null;
  } catch {
    return null;
  }
}

function saveInterview(session) {
  localStorage.setItem(KEY, JSON.stringify(session));
}

function startInterview(problemId, durationMin) {
  for (const key of Object.keys(localStorage)) if (key.startsWith(CANVAS_PREFIX)) localStorage.removeItem(key);
  const session = { id: newId(), problem_id: problemId, duration_min: durationMin, started_at: new Date().toISOString(), round: 1, draft: {} };
  saveInterview(session);
  return session;
}

// ================================================================ core: header
function setupNav() {
  const btn = $('#nav-toggle');
  const nav = $('#main-nav');
  btn?.addEventListener('click', () => {
    const open = nav.classList.toggle('open');
    btn.setAttribute('aria-expanded', String(open));
  });
}


// ================================================================ core: backdrop
// The designer portfolio's backdrop, on every page but the workspace: film grain and a drifting
// WebGL gradient. The landing page walks through the gradient's five palettes as it scrolls.
const BACKDROP_FRAG = `
  #ifdef GL_FRAGMENT_PRECISION_HIGH
  precision highp float;
  #else
  precision mediump float;
  #endif
  varying vec2 uv;
  uniform vec2 res;
  uniform float time;
  uniform float scroll;
  vec3 perm(vec3 x) { return mod(((x * 34.0) + 1.0) * x, 289.0); }
  float snoise(vec2 v) {
    const vec4 C = vec4(0.211324865405187, 0.366025403784439, -0.577350269189626, 0.024390243902439);
    vec2 i = floor(v + dot(v, C.yy));
    vec2 x0 = v - i + dot(i, C.xx);
    vec2 i1 = (x0.x > x0.y) ? vec2(1.0, 0.0) : vec2(0.0, 1.0);
    vec4 x12 = x0.xyxy + C.xxzz;
    x12.xy -= i1;
    i = mod(i, 289.0);
    vec3 p = perm(perm(i.y + vec3(0.0, i1.y, 1.0)) + i.x + vec3(0.0, i1.x, 1.0));
    vec3 m = max(0.5 - vec3(dot(x0, x0), dot(x12.xy, x12.xy), dot(x12.zw, x12.zw)), 0.0);
    m = m * m; m = m * m;
    vec3 x = 2.0 * fract(p * C.www) - 1.0;
    vec3 h = abs(x) - 0.5;
    vec3 ox = floor(x + 0.5);
    vec3 a0 = x - ox;
    m *= 1.79284291400159 - 0.85373472095314 * (a0 * a0 + h * h);
    vec3 g;
    g.x = a0.x * x0.x + h.x * x0.y;
    g.yz = a0.yz * x12.xz + h.yz * x12.yw;
    return 130.0 * dot(m, g);
  }
  float blob(vec2 p, vec2 c, float r) { vec2 d = p - c; return exp(-dot(d, d) / (r * r)); }

  vec3 hero(vec2 p, float a, float t) {
    vec2 w = p + 0.12 * vec2(snoise(p * 1.4 + vec2(t * 0.05, 0.0)), snoise(p * 1.4 + vec2(5.2, -t * 0.04)));
    float g1 = blob(w, vec2((0.80 + 0.14 * sin(t * 0.11)) * a, 0.24 + 0.12 * sin(t * 0.07 + 1.0)), 0.30);
    float g2 = blob(w, vec2((0.30 + 0.12 * sin(t * 0.09 + 2.0)) * a, 0.07 + 0.06 * cos(t * 0.13)), 0.22);
    float g3 = blob(w, vec2((0.88 + 0.08 * cos(t * 0.10)) * a, 0.68 + 0.10 * sin(t * 0.08 + 3.0)), 0.26);
    float g4 = blob(w, vec2((0.40 + 0.20 * sin(t * 0.06 + 4.0)) * a, 0.62 + 0.10 * cos(t * 0.09)), 0.20);
    vec3 blue = vec3(0.105, 0.20, 0.33);
    vec3 col = vec3(0.0, 0.004, 0.008);
    col += blue * (0.78 * g1 + 0.62 * g2 + 0.72 * g3) + vec3(0.03, 0.09, 0.13) * g4;
    col += vec3(0.04, 0.06, 0.09) * g1 * g1 * g1;
    col *= mix(0.18, 1.0, smoothstep(1.0, 0.66, p.y));
    col *= mix(0.4, 1.0, smoothstep(0.0, 0.32, p.x / a));
    return col;
  }

  vec3 acc; float tot;
  void add(vec3 c, vec2 w, vec2 at, float r, float k) { float b = blob(w, at, r) * k; acc += c * b; tot += b; }

  vec3 play(vec2 p, float a, float t) {
    vec2 w = p + 0.10 * vec2(snoise(p * 1.7 + vec2(t * 0.06, 1.3)), snoise(p * 1.7 + vec2(-2.1, t * 0.05)));
    acc = vec3(0.0); tot = 0.35;
    add(vec3(0.03, 0.60, 0.57), w, vec2((0.80 + 0.05 * sin(t * 0.12)) * a, 0.12 + 0.04 * cos(t * 0.10)), 0.30, 3.0);
    add(vec3(0.05, 0.66, 0.62), w, vec2((0.98 + 0.04 * sin(t * 0.08)) * a, 0.26), 0.18, 1.8);
    add(vec3(0.02, 0.42, 0.42), w, vec2((0.57 + 0.04 * cos(t * 0.09)) * a, 0.22), 0.14, 1.6);
    add(vec3(0.31, 0.01, 0.21), w, vec2((0.16 + 0.04 * sin(t * 0.08)) * a, 0.24 + 0.05 * sin(t * 0.11)), 0.22, 2.0);
    add(vec3(0.77, 0.58, 0.10), w, vec2((-0.02 + 0.03 * sin(t * 0.10)) * a, 0.52 + 0.06 * cos(t * 0.07)), 0.22, 2.4);
    add(vec3(0.47, 0.38, 0.02), w, vec2(0.06 * a, 0.95), 0.20, 1.6);
    add(vec3(0.58, 0.40, 0.66), w, vec2((0.30 + 0.04 * sin(t * 0.13)) * a, 0.80), 0.11, 1.2);
    add(vec3(0.85, 0.70, 0.56), w, vec2(0.13 * a, 0.66), 0.07, 0.8);
    add(vec3(0.83, 0.24, 0.07), w, vec2((0.70 + 0.06 * sin(t * 0.09)) * a, 1.05), 0.26, 3.2);
    add(vec3(0.84, 0.22, 0.10), w, vec2((0.96 + 0.03 * cos(t * 0.11)) * a, 0.98), 0.18, 2.2);
    add(vec3(0.86, 0.44, 0.11), w, vec2(0.46 * a, 1.0), 0.15, 2.0);
    return acc / tot;
  }

  vec3 contact(vec2 p, float a, float t) {
    vec2 w = p + 0.10 * vec2(snoise(p * 1.5 + vec2(t * 0.05, 0.0)), snoise(p * 1.5 + vec2(9.0, -t * 0.04)));
    vec3 col = vec3(0.004, 0.006, 0.01);
    col += vec3(0.42, 0.24, 0.0) * blob(w, vec2((0.12 + 0.06 * sin(t * 0.10)) * a, 0.98), 0.36);
    col += vec3(0.0, 0.30, 0.28) * blob(w, vec2((0.88 + 0.05 * cos(t * 0.09)) * a, 0.10), 0.34);
    col += vec3(0.10, 0.18, 0.32) * blob(w, vec2((0.55 + 0.10 * sin(t * 0.07)) * a, 0.45), 0.30);
    return col;
  }

  vec3 palette(float k, vec2 p, float a, float t) {
    if (k < 0.5) return hero(p, a, t);
    if (k < 1.5) return play(p, a, t);
    if (k < 2.5) return hero(vec2(a - p.x, p.y), a, t + 20.0);
    if (k < 3.5) return play(vec2(a - p.x, p.y), a, t + 40.0);
    return contact(p, a, t);
  }

  void main() {
    float a = res.x / res.y;
    vec2 p = vec2(uv.x * a, 1.0 - uv.y);
    float s = clamp(scroll, 0.0, 4.0);
    float k = min(floor(s), 3.0);
    float f = s - k;
    float e = 1.0 - f;
    float m = smoothstep(e - 0.55 * f, e + 0.55 * (1.0 - f), p.y);
    vec3 col = vec3(0.0);
    if (m < 0.999) col = palette(k, p, a, time);
    if (m > 0.001) col = mix(col, palette(k + 1.0, p, a, time), m);
    col += (fract(sin(dot(gl_FragCoord.xy, vec2(12.9898, 78.233))) * 43758.5453) - 0.5) / 255.0;
    gl_FragColor = vec4(col, 1.0);
  }
`;

function setupBackdrop() {
  const grain = $('.grain');
  if (grain) {
    const c = document.createElement('canvas');
    c.width = c.height = 180;
    const x = c.getContext('2d');
    const d = x.createImageData(180, 180);
    for (let i = 0; i < d.data.length; i += 4) {
      const v = (Math.random() * 255) | 0;
      d.data[i] = d.data[i + 1] = d.data[i + 2] = v;
      d.data[i + 3] = 255;
    }
    x.putImageData(d, 0, 0);
    grain.style.backgroundImage = `url(${c.toDataURL()})`;
  }

  const canvas = $('#backdrop');
  if (!canvas) return;
  let gl = null;
  try {
    gl = canvas.getContext('webgl', { antialias: false, depth: false, stencil: false, alpha: false, powerPreference: 'low-power' });
  } catch {
    gl = null;
  }
  if (!gl) return;
  const shader = (type, source) => {
    const s = gl.createShader(type);
    gl.shaderSource(s, source);
    gl.compileShader(s);
    if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(s));
    return s;
  };
  let uRes, uTime, uScroll;
  try {
    const prog = gl.createProgram();
    gl.attachShader(prog, shader(gl.VERTEX_SHADER, 'attribute vec2 p; varying vec2 uv; void main() { uv = p * 0.5 + 0.5; gl_Position = vec4(p, 0.0, 1.0); }'));
    gl.attachShader(prog, shader(gl.FRAGMENT_SHADER, BACKDROP_FRAG));
    gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(prog));
    gl.useProgram(prog);
    gl.bindBuffer(gl.ARRAY_BUFFER, gl.createBuffer());
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 3, -1, -1, 3]), gl.STATIC_DRAW);
    const loc = gl.getAttribLocation(prog, 'p');
    gl.enableVertexAttribArray(loc);
    gl.vertexAttribPointer(loc, 2, gl.FLOAT, false, 0, 0);
    uRes = gl.getUniformLocation(prog, 'res');
    uTime = gl.getUniformLocation(prog, 'time');
    uScroll = gl.getUniformLocation(prog, 'scroll');
  } catch (err) {
    console.warn('Background shader disabled:', err);
    return;
  }

  const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const landing = document.body.dataset.page === 'landing';
  const size = () => {
    const k = Math.min(0.6, 1100 / Math.max(innerWidth, 1));
    canvas.width = Math.max(2, Math.round(innerWidth * k));
    canvas.height = Math.max(2, Math.round(innerHeight * k));
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.uniform2f(uRes, canvas.width, canvas.height);
  };
  const scroll = () => {
    if (!landing) return 0;
    const max = document.documentElement.scrollHeight - innerHeight;
    return max > 0 ? 4 * Math.min(1, scrollY / max) : 0;
  };
  const t0 = performance.now();
  const draw = (now) => {
    gl.uniform1f(uTime, reduce ? 6 : ((now - t0) / 1000) * 2.2);
    gl.uniform1f(uScroll, scroll());
    gl.drawArrays(gl.TRIANGLES, 0, 3);
  };
  size();
  draw(t0);
  addEventListener('resize', () => { size(); draw(performance.now()); });
  if (reduce) {
    addEventListener('scroll', () => draw(performance.now()), { passive: true });
  } else {
    let last = 0;
    const frame = (now) => {
      // 30 fps is enough for a slow drift
      if (now - last > 32) { last = now; draw(now); }
      requestAnimationFrame(frame);
    };
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(() => document.documentElement.classList.add('bg-on'));
}

// ================================================================ workspace: chart
// Rolling charts for a live run over the last `window` seconds: requests arriving and served,
// then the error rate and p95 latency against the target, then (with Kafka) consumer lag.
function liveCharts(history, { targetMs, window = 120, kafka = false }) {
  const now = history.at(-1).t;
  const from = now - window;
  const pts = history.filter((p) => p.t >= from);
  const W = 330;
  const H = 108;
  const L = 40;
  const R = 44;
  const T = 10;
  const B = 18;
  const iw = W - L - R;
  const ih = H - T - B;
  const x = (t) => (L + ((t - from) / window) * iw).toFixed(1);
  const line = (y) => pts.map((p, i) => `${i ? 'L' : 'M'}${x(p.t)},${y(p).toFixed(1)}`).join('');
  const base = T + ih;
  const axis = html`<line class="axis" x1="${L}" x2="${W - R}" y1="${base}" y2="${base}"/>
    ${[0, 0.5, 1].map((f) => html`<text x="${L + f * iw}" y="${H - 4}" text-anchor="middle">${f === 1 ? 'now' : `−${Math.round((1 - f) * window)} s`}</text>`)}`;

  const maxRps = Math.max(1, ...pts.map((p) => p.rps)) * 1.15;
  const yRps = (v) => base - (v / maxRps) * ih;
  const arriving = `${line((p) => yRps(p.rps))}L${x(pts.at(-1).t)},${base}L${x(pts[0].t)},${base}Z`;

  const maxErr = Math.max(0.05, ...pts.map((p) => p.error)) * 1.15;
  const yErr = (e) => base - Math.min(1, e / maxErr) * ih;
  const maxLat = Math.max(targetMs * 1.6, ...pts.map((p) => p.p95)) * 1.1;
  const yLat = (v) => base - Math.min(1, v / maxLat) * ih;

  return html`<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Requests arriving and served over time">
    ${[0.5, 1].map((f) => html`<line class="gridline" x1="${L}" x2="${W - R}" y1="${base - f * ih}" y2="${base - f * ih}"/><text x="${L - 6}" y="${base - f * ih + 3}" text-anchor="end">${fmtNum(maxRps * f)}</text>`)}
    ${axis}
    <path class="s-arrive" d="${arriving}"/>
    <path class="s-served" d="${line((p) => yRps(p.served))}"/>
  </svg>
  <div class="chart-legend"><span><i style="background:var(--accent)"></i>Arriving /s</span><span><i style="background:var(--ok)"></i>Served /s</span></div>
  <svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Errors and slow requests over time">
    ${[0.5, 1].map((f) => html`<line class="gridline" x1="${L}" x2="${W - R}" y1="${base - f * ih}" y2="${base - f * ih}"/>
      <text x="${L - 6}" y="${base - f * ih + 3}" text-anchor="end">${(maxErr * f * 100).toFixed(maxErr > 0.2 ? 0 : 1)}%</text>
      <text x="${W - R + 6}" y="${base - f * ih + 3}">${Math.round(maxLat * f)}</text>`)}
    <line class="target" x1="${L}" x2="${W - R}" y1="${yLat(targetMs)}" y2="${yLat(targetMs)}"/>
    ${axis}
    <path class="s-latency" d="${line((p) => yLat(p.p95))}"/>
    <path class="s-error" d="${line((p) => yErr(p.error))}"/>
  </svg>
  <div class="chart-legend"><span><i style="background:var(--bad)"></i>Errors (left)</span><span><i style="background:var(--accent)"></i>p95 ms (right)</span><span><i style="background:var(--warm)"></i>Target ${targetMs} ms</span></div>
  ${kafka ? lagChart(pts, { x, line, axis, base, ih, L, R, W, H }) : ''}`;
}

function lagChart(pts, { x, line, axis, base, ih, L, R, W, H }) {
  const maxLag = Math.max(10, ...pts.map((p) => p.lag || 0)) * 1.15;
  const maxAge = Math.max(5, ...pts.map((p) => p.age || 0)) * 1.15;
  const yLag = (v) => base - Math.min(1, (v || 0) / maxLag) * ih;
  const yAge = (v) => base - Math.min(1, (v || 0) / maxAge) * ih;
  const area = `${line((p) => yLag(p.lag))}L${x(pts.at(-1).t)},${base}L${x(pts[0].t)},${base}Z`;
  return html`<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Consumer lag and data age over time">
    ${[0.5, 1].map((f) => html`<line class="gridline" x1="${L}" x2="${W - R}" y1="${base - f * ih}" y2="${base - f * ih}"/>
      <text x="${L - 6}" y="${base - f * ih + 3}" text-anchor="end">${fmtNum(maxLag * f)}</text>
      <text x="${W - R + 6}" y="${base - f * ih + 3}">${Math.round(maxAge * f)} s</text>`)}
    ${axis}
    <path class="s-lag" d="${area}"/>
    <path class="s-age" d="${line((p) => yAge(p.age))}"/>
  </svg>
  <div class="chart-legend"><span><i style="background:var(--cat-kafka)"></i>Messages behind (left)</span><span><i style="background:var(--warm)"></i>Data age (right)</span></div>`;
}

// Two-axis line chart for the stress ramp: error rate on the left, p95 latency on the right.
function rampChart({ points, targetMs, cursor = null }) {
  const W = 330;
  const H = 190;
  const L = 36;
  const R = 44;
  const T = 12;
  const B = 30;
  const iw = W - L - R;
  const ih = H - T - B;
  const n = points.length;
  const x = (i) => L + (n === 1 ? iw / 2 : (i / (n - 1)) * iw);
  const maxLat = Math.max(targetMs * 1.6, ...points.map((p) => p.latency_ms), 1);
  const yErr = (e) => T + ih - Math.min(1, e) * ih;
  const yLat = (ms) => T + ih - Math.min(1, ms / maxLat) * ih;
  const errPath = points.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${yErr(p.error).toFixed(1)}`).join('');
  const latPath = points.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${yLat(p.latency_ms).toFixed(1)}`).join('');
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  return html`<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Errors and slow requests as traffic grows">
    ${ticks.map((t) => html`<line class="gridline" x1="${L}" x2="${W - R}" y1="${T + ih - t * ih}" y2="${T + ih - t * ih}"/>
      <text x="${L - 6}" y="${T + ih - t * ih + 3}" text-anchor="end">${Math.round(t * 100)}%</text>
      <text x="${W - R + 6}" y="${T + ih - t * ih + 3}">${Math.round(t * maxLat)}</text>`)}
    <line class="target" x1="${L}" x2="${W - R}" y1="${yLat(targetMs)}" y2="${yLat(targetMs)}"/>
    <line class="axis" x1="${L}" x2="${W - R}" y1="${T + ih}" y2="${T + ih}"/>
    ${points.map((p, i) => html`<text x="${x(i)}" y="${H - 12}" text-anchor="middle">${p.x}×</text>`)}
    <text x="${L + iw / 2}" y="${H - 1}" text-anchor="middle">traffic (× normal)</text>
    ${cursor !== null ? html`<line class="cursor" x1="${x(cursor)}" x2="${x(cursor)}" y1="${T}" y2="${T + ih}"/>` : ''}
    <path class="s-latency" d="${latPath}"/>
    <path class="s-error" d="${errPath}"/>
    ${points.map((p, i) => html`<circle class="pt-latency" r="2.6" cx="${x(i)}" cy="${yLat(p.latency_ms)}"/><circle class="pt-error" r="2.6" cx="${x(i)}" cy="${yErr(p.error)}"/>`)}
  </svg>
  <div class="chart-legend"><span><i style="background:var(--bad)"></i>Errors (left)</span><span><i style="background:var(--accent)"></i>p95 ms (right)</span><span><i style="background:var(--warm)"></i>Target ${targetMs} ms</span></div>`;
}

// ================================================================ workspace: editor
const GRID = 10;
const snap = (v) => Math.round(v / GRID) * GRID;
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

function paramOf(node, key) {
  const c = node.config || {};
  for (const b of ['basic', 'advanced', 'security']) if (c[b] && c[b][key] !== undefined) return c[b][key];
  return paramSpec(node.type, key)?.default;
}

function withDefaults(node) {
  const base = defaultsFor(node.type);
  const c = node.config || {};
  return {
    id: String(node.id),
    type: node.type,
    label: node.label || COMPONENTS[node.type].label,
    x: Number(node.x) || 0,
    y: Number(node.y) || 0,
    config: {
      basic: { ...base.basic, ...(c.basic || {}) },
      advanced: { ...base.advanced, ...(c.advanced || {}) },
      security: { ...base.security, ...(c.security || {}) },
    },
  };
}

const pct = (v) => `${Math.round(v * 100)}%`;
const REDUCED_MOTION = !!globalThis.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

// Parts that end a request path; a new box added after one of these is not wired to it.
const SINKS = new Set(['Database', 'BlobStorage', 'SearchIndex', 'ThirdPartyAPI']);

// Publishing to a queue or topic, and change data capture from a database, never wait.
function alwaysAsync(sourceType, targetType) {
  return targetType === 'Queue' || targetType === 'KafkaTopic' || (sourceType === 'Database' && targetType === 'KafkaConnect');
}
const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

class Editor extends EventTarget {
  constructor(host, { readOnly = false } = {}) {
    super();
    this.host = host;
    this.readOnly = readOnly;
    this.nodes = [];
    this.edges = [];
    this.sel = null;
    this.view = { x: 40, y: 40, k: 1 };
    this.history = [];
    this.future = [];
    this.overlay = null;
    this.flashIds = new Set();
    this.gesture = null;
    this.newDefaults = {};
    this.mount();
    this.bind();
  }

  // ---------------------------------------------------------------- setup
  mount() {
    this.host.classList.add('cv');
    this.host.classList.toggle('read-only', this.readOnly);
    this.host.tabIndex = 0;
    this.host.innerHTML = `
      <div class="cv-viewport">
        <svg class="cv-svg" aria-hidden="true">
          <defs>
            <marker id="cv-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerUnits="userSpaceOnUse" markerWidth="11" markerHeight="11" orient="auto-start-reverse"><path class="cv-arrow" d="M0 0L10 5L0 10z"/></marker>
            <marker id="cv-arrow-on" viewBox="0 0 10 10" refX="9" refY="5" markerUnits="userSpaceOnUse" markerWidth="11" markerHeight="11" orient="auto-start-reverse"><path class="cv-arrow-on" d="M0 0L10 5L0 10z"/></marker>
          </defs>
          <g class="cv-edges"></g>
          <g class="cv-particles"></g>
          <path class="cv-temp hidden"/>
        </svg>
        <div class="cv-nodes"></div>
      </div>
      <div class="cv-empty hidden"><strong>Your canvas is empty</strong><span>Drag a part from the left, or click one to add it.</span><span class="small">Connect two boxes by dragging from the small dot on the right of a box. Or select a box first: the next part you click is added after it, with an arrow.</span></div>
      <div class="cv-banner hidden"></div>
      <div class="cv-zoom" role="group" aria-label="Zoom">
        <button type="button" data-z="out" title="Zoom out" aria-label="Zoom out">−</button>
        <button type="button" data-z="in" title="Zoom in" aria-label="Zoom in">+</button>
        <button type="button" data-z="fit" title="Fit to screen">Fit</button>
      </div>`;
    this.viewport = this.host.querySelector('.cv-viewport');
    this.svg = this.host.querySelector('.cv-svg');
    this.edgeLayer = this.host.querySelector('.cv-edges');
    this.particleLayer = this.host.querySelector('.cv-particles');
    this.nodeLayer = this.host.querySelector('.cv-nodes');
    this.temp = this.host.querySelector('.cv-temp');
    this.empty = this.host.querySelector('.cv-empty');
    this.banner = this.host.querySelector('.cv-banner');
    this.applyView();
  }

  bind() {
    this.host.addEventListener('pointerdown', (e) => this.onDown(e));
    window.addEventListener('pointermove', (e) => this.onMove(e));
    window.addEventListener('pointerup', (e) => this.onUp(e));
    this.host.addEventListener('wheel', (e) => this.onWheel(e), { passive: false });
    this.host.addEventListener('keydown', (e) => this.onKey(e));
    this.host.addEventListener('dblclick', (e) => {
      const nodeEl = e.target.closest('.cv-node');
      if (nodeEl) this.emit('edit', { id: nodeEl.dataset.id });
    });
    this.host.querySelector('.cv-zoom').addEventListener('click', (e) => {
      const z = e.target.closest('button')?.dataset.z;
      if (z === 'in') this.zoomBy(1.2);
      else if (z === 'out') this.zoomBy(1 / 1.2);
      else if (z === 'fit') this.fit();
    });
    if (!this.readOnly) {
      this.host.addEventListener('dragover', (e) => {
        if ([...e.dataTransfer.types].includes('application/x-hld-type')) {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'copy';
        }
      });
      this.host.addEventListener('drop', (e) => {
        const type = e.dataTransfer.getData('application/x-hld-type');
        if (!type || !COMPONENTS[type]) return;
        e.preventDefault();
        const p = this.toWorld(e.clientX, e.clientY);
        this.addNode(type, p.x - NODE_W / 2, p.y - NODE_H / 2);
      });
    }
    new ResizeObserver(() => this.applyView()).observe(this.host);
  }

  emit(name, detail = {}) {
    this.dispatchEvent(new CustomEvent(name, { detail }));
  }

  changed(reason) {
    this.emit('change', { reason });
    this.emit('history', { canUndo: this.history.length > 0, canRedo: this.future.length > 0 });
  }

  // ---------------------------------------------------------------- model
  load(graph, { fit = true, keepHistory = false } = {}) {
    if (!keepHistory) {
      this.history = [];
      this.future = [];
    }
    this.nodes = (graph?.nodes || []).filter((n) => COMPONENTS[n.type]).map(withDefaults);
    const ids = new Set(this.nodes.map((n) => n.id));
    this.edges = (graph?.edges || [])
      .filter((e) => ids.has(e.source) && ids.has(e.target))
      .map((e) => ({ id: e.id || `${e.source}->${e.target}`, source: e.source, target: e.target, kind: e.kind === 'async' ? 'async' : 'sync', ratio: e.ratio ?? null, optional: !!e.optional, read_share: e.read_share ?? null }));
    this.sel = null;
    this.overlay = null;
    this.render();
    if (fit) requestAnimationFrame(() => this.fit());
    this.emit('select', { sel: null });
    this.changed('load');
  }

  toGraph() {
    return JSON.parse(JSON.stringify({ nodes: this.nodes, edges: this.edges }));
  }

  node(id) { return this.nodes.find((n) => n.id === id); }
  edge(id) { return this.edges.find((e) => e.id === id); }
  param(id, key) { const n = this.node(id); return n ? paramOf(n, key) : undefined; }

  snapshot() {
    this.history.push(JSON.stringify({ nodes: this.nodes, edges: this.edges }));
    if (this.history.length > 80) this.history.shift();
    this.future = [];
  }

  restore(json) {
    const s = JSON.parse(json);
    this.nodes = s.nodes;
    this.edges = s.edges;
    if (this.sel && !(this.sel.kind === 'node' ? this.node(this.sel.id) : this.edge(this.sel.id))) this.sel = null;
    this.render();
    this.emit('select', { sel: this.sel });
  }

  setReadOnly(on) {
    this.readOnly = !!on;
    this.gesture = null;
    this.host.classList.toggle('read-only', this.readOnly);
    this.render();
  }

  undo() {
    if (this.readOnly || !this.history.length) return;
    this.future.push(JSON.stringify({ nodes: this.nodes, edges: this.edges }));
    this.restore(this.history.pop());
    this.changed('undo');
  }

  redo() {
    if (this.readOnly || !this.future.length) return;
    this.history.push(JSON.stringify({ nodes: this.nodes, edges: this.edges }));
    this.restore(this.future.pop());
    this.changed('redo');
  }

  nextId(type) {
    const base = type.toLowerCase();
    let i = this.nodes.filter((n) => n.type === type).length + 1;
    while (this.node(`${base}-${i}`)) i++;
    return `${base}-${i}`;
  }

  addNode(type, x, y, { linkFrom = null } = {}) {
    if (this.readOnly) return null;
    this.snapshot();
    const n = withDefaults({ id: this.nextId(type), type, x: snap(x), y: snap(y) });
    for (const [key, value] of Object.entries(this.newDefaults[type] || {})) {
      const spec = paramSpec(type, key);
      if (spec) n.config[spec.block][key] = value;
    }
    const count = this.nodes.filter((m) => m.type === type).length;
    if (count > 0) n.label = `${COMPONENTS[type].label} ${count + 1}`;
    this.nodes.push(n);
    if (linkFrom && this.node(linkFrom) && type !== 'User') {
      const kind = alwaysAsync(this.node(linkFrom).type, type) ? 'async' : 'sync';
      this.edges.push({ id: `${linkFrom}->${n.id}`, source: linkFrom, target: n.id, kind, ratio: null, optional: false, read_share: null });
    }
    this.sel = { kind: 'node', id: n.id };
    this.render();
    this.emit('select', { sel: this.sel });
    this.changed('add');
    return n.id;
  }

  isFree(x, y) {
    return !this.nodes.some((n) => Math.abs(n.x - x) < NODE_W + 24 && Math.abs(n.y - y) < NODE_H + 20);
  }

  // Nearest free grid slot, searching outwards in rings around (x, y).
  freeSpot(x, y) {
    const sx = NODE_W + 50;
    const sy = NODE_H + 34;
    for (let ring = 0; ring < 14; ring++) {
      for (let dy = -ring; dy <= ring; dy++) {
        for (let dx = -ring; dx <= ring; dx++) {
          if (Math.max(Math.abs(dx), Math.abs(dy)) !== ring) continue;
          const px = snap(x + dx * sx);
          const py = snap(y + dy * sy);
          if (this.isFree(px, py)) return { x: px, y: py };
        }
      }
    }
    return { x: snap(x), y: snap(y) };
  }

  // Palette click: next to the selected box (and wired to it), or in a free spot in view.
  addNodeAtCenter(type) {
    const from = this.sel?.kind === 'node' ? this.node(this.sel.id) : null;
    let pos;
    if (from) {
      const x = snap(from.x + NODE_W + 80);
      const step = NODE_H + 34;
      const slot = [0, 1, -1, 2, -2, 3, -3].map((i) => snap(from.y + i * step)).find((y) => this.isFree(x, y));
      pos = slot !== undefined ? { x, y: slot } : this.freeSpot(x, from.y);
    } else {
      const r = this.host.getBoundingClientRect();
      const c = this.toWorld(r.left + r.width / 2, r.top + r.height / 2);
      pos = this.freeSpot(c.x - NODE_W / 2, c.y - NODE_H / 2);
    }
    const link = from && !SINKS.has(from.type) ? from.id : null;
    const id = this.addNode(type, pos.x, pos.y, { linkFrom: link });
    this.ensureVisible(id);
    return id;
  }

  ensureVisible(id) {
    const n = this.node(id);
    const r = this.host.getBoundingClientRect();
    if (!n || !r.width) return;
    const { k } = this.view;
    const left = n.x * k + this.view.x;
    const top = n.y * k + this.view.y;
    const m = 30;
    let dx = 0;
    let dy = 0;
    if (left < m) dx = m - left;
    else if (left + NODE_W * k > r.width - m) dx = r.width - m - (left + NODE_W * k);
    if (top < m) dy = m - top;
    else if (top + (NODE_H + 24) * k > r.height - m) dy = r.height - m - (top + (NODE_H + 24) * k);
    if (dx || dy) {
      this.view.x += dx;
      this.view.y += dy;
      this.applyView();
    }
  }

  connect(source, target, { keepSelection = false } = {}) {
    if (this.readOnly || source === target) return null;
    const t = this.node(target);
    if (!t) return null;
    if (t.type === 'User') {
      this.emit('notice', { message: 'Arrows show where requests go. Nothing sends requests to Users.' });
      return null;
    }
    if (this.edges.some((e) => e.source === source && e.target === target)) return null;
    this.snapshot();
    const kind = alwaysAsync(this.node(source)?.type, t.type) ? 'async' : 'sync';
    const e = { id: `${source}->${target}`, source, target, kind, ratio: null, optional: false, read_share: null };
    this.edges.push(e);
    if (!keepSelection) this.sel = { kind: 'edge', id: e.id };
    this.render();
    if (!keepSelection) this.emit('select', { sel: this.sel });
    this.changed('connect');
    return e.id;
  }

  removeEdge(id) {
    if (this.readOnly || !this.edge(id)) return;
    this.snapshot();
    this.edges = this.edges.filter((e) => e.id !== id);
    const wasSelected = this.sel?.kind === 'edge' && this.sel.id === id;
    if (wasSelected) this.sel = null;
    this.render();
    if (wasSelected) this.emit('select', { sel: null });
    this.changed('disconnect');
  }

  removeSelected() {
    if (this.readOnly || !this.sel) return;
    this.snapshot();
    if (this.sel.kind === 'node') {
      const id = this.sel.id;
      this.nodes = this.nodes.filter((n) => n.id !== id);
      this.edges = this.edges.filter((e) => e.source !== id && e.target !== id);
    } else {
      this.edges = this.edges.filter((e) => e.id !== this.sel.id);
    }
    this.sel = null;
    this.render();
    this.emit('select', { sel: null });
    this.changed('remove');
  }

  duplicate(id = this.sel?.kind === 'node' ? this.sel.id : null) {
    const n = id && this.node(id);
    if (!n || this.readOnly) return;
    this.snapshot();
    const copy = JSON.parse(JSON.stringify(n));
    copy.id = this.nextId(n.type);
    copy.x += 30;
    copy.y += 30;
    copy.label = `${n.label} copy`;
    this.nodes.push(copy);
    this.sel = { kind: 'node', id: copy.id };
    this.render();
    this.emit('select', { sel: this.sel });
    this.changed('add');
  }

  // Inspector edits: one undo step per field focus, not per keystroke.
  beginEdit() {
    if (!this.editing) {
      this.snapshot();
      this.editing = true;
    }
  }

  endEdit() {
    this.editing = false;
  }

  setLabel(id, label) {
    const n = this.node(id);
    if (!n) return;
    this.beginEdit();
    n.label = String(label).slice(0, 60) || COMPONENTS[n.type].label;
    this.renderNodes();
    this.changed('edit');
  }

  setParam(id, key, value) {
    const n = this.node(id);
    const spec = n && paramSpec(n.type, key);
    if (!spec) return;
    this.beginEdit();
    n.config[spec.block][key] = value;
    this.renderNodes();
    this.changed('edit');
  }

  updateEdge(id, patch) {
    const e = this.edge(id);
    if (!e) return;
    this.beginEdit();
    Object.assign(e, patch);
    this.renderEdges();
    this.changed('edit');
  }

  select(sel) {
    this.sel = sel;
    this.render();
    this.emit('select', { sel });
  }

  autoArrange() {
    if (this.readOnly || !this.nodes.length) return;
    this.snapshot();
    const laid = autoLayout({ nodes: this.nodes, edges: this.edges });
    const pos = new Map(laid.nodes.map((n) => [n.id, n]));
    for (const n of this.nodes) {
      n.x = pos.get(n.id).x;
      n.y = pos.get(n.id).y;
    }
    this.render();
    this.fit();
    this.changed('move');
  }

  // ---------------------------------------------------------------- overlays
  setOverlay(sim, { banner = null } = {}) {
    if (!sim) return this.clearOverlay();
    const nodes = new Map();
    for (const m of sim.node_metrics || []) {
      if (m.status === 'source') continue;
      let text;
      if (m.status === 'down') text = 'down';
      else if (m.status === 'idle') text = 'no traffic';
      else text = `${fmtLoad(m.utilization)} · ${fmtNum(m.demand_rps)} rps`;
      nodes.set(m.id, { status: m.status, text });
    }
    const edges = new Map((sim.edge_flows || []).map((e) => [e.id, e.rps]));
    // A live run repaints several times a second; skip it when nothing visible changed, so
    // clicks and double-clicks on boxes are not interrupted.
    const key = JSON.stringify([[...nodes], [...edges].map(([id, rps]) => [id, fmtNum(rps)]), banner?.html ?? null]);
    if (this.overlay?.key === key) return;
    this.overlay = { nodes, edges, max: Math.max(1, ...edges.values()), key };
    this.setBanner(banner);
    this.render();
  }

  // Moving dots along arrows during a live run: [{ id: edgeId, dots, bad, async }], or null.
  setParticles(list) {
    this.particles = list && !REDUCED_MOTION ? list : null;
    this.renderParticles();
  }

  renderParticles() {
    if (!this.particles) {
      if (this.particleKey) this.particleLayer.innerHTML = '';
      this.particleKey = '';
      return;
    }
    const parts = [];
    let key = '';
    for (const p of this.particles) {
      const e = this.edge(p.id);
      const path = e && this.edgePath(e);
      if (!path) continue;
      const a = this.node(e.source);
      const b = this.node(e.target);
      const dur = clamp((Math.hypot(b.x - a.x - NODE_W, b.y - a.y) + 60) / 150, 0.8, 4).toFixed(2);
      key += `${p.id}|${path.d}|${p.dots}|${p.bad}|${p.async};`;
      for (let i = 0; i < p.dots; i++) {
        const bad = i < p.bad;
        const begin = (-(dur * i) / p.dots).toFixed(2);
        const fade = bad ? `<animate attributeName="opacity" values="1;1;0" keyTimes="0;0.7;1" dur="${dur}s" begin="${begin}s" repeatCount="indefinite"/>` : '';
        parts.push(`<circle class="cv-dot ${bad ? 'bad' : p.async ? 'async' : 'ok'}" r="3.4"><animateMotion dur="${dur}s" begin="${begin}s" repeatCount="indefinite" path="${path.d}"/>${fade}</circle>`);
      }
    }
    if (key === this.particleKey) return;
    this.particleKey = key;
    this.particleLayer.innerHTML = parts.join('');
  }

  setHeat(load, { banner = null } = {}) {
    const nodes = new Map();
    for (const [id, v] of Object.entries(load || {})) {
      const status = v < 0 ? 'down' : v > 1 ? 'overloaded' : v >= 0.85 ? 'hot' : v >= 0.7 ? 'warm' : v > 0 ? 'ok' : 'idle';
      nodes.set(id, { status, text: v < 0 ? 'down' : v > 0 ? fmtLoad(v) : 'no traffic' });
    }
    this.overlay = { nodes, edges: new Map(), max: 1 };
    this.setBanner(banner);
    this.render();
  }

  clearOverlay() {
    this.overlay = null;
    this.setBanner(null);
    this.render();
  }

  setBanner(banner) {
    if (!banner) {
      this.banner.classList.add('hidden');
      this.banner.innerHTML = '';
      return;
    }
    this.banner.classList.remove('hidden');
    this.banner.innerHTML = `<span>${banner.html}</span>${banner.action ? `<button type="button" class="btn btn-sm btn-ghost">${esc(banner.action)}</button>` : ''}`;
    const btn = this.banner.querySelector('button');
    if (btn) btn.addEventListener('click', () => this.emit('banner-action'));
  }

  flash(ids) {
    this.flashIds = new Set(ids);
    this.renderNodes();
    clearTimeout(this.flashTimer);
    this.flashTimer = setTimeout(() => {
      this.flashIds = new Set();
      this.renderNodes();
    }, 2200);
    const first = ids.map((id) => this.node(id)).find(Boolean);
    if (first) this.centerOn(first);
  }

  // ---------------------------------------------------------------- view
  toWorld(clientX, clientY) {
    const r = this.host.getBoundingClientRect();
    return { x: (clientX - r.left - this.view.x) / this.view.k, y: (clientY - r.top - this.view.y) / this.view.k };
  }

  applyView() {
    this.viewport.style.transform = `translate(${this.view.x}px, ${this.view.y}px) scale(${this.view.k})`;
  }

  zoomAt(factor, clientX, clientY) {
    const r = this.host.getBoundingClientRect();
    const k = clamp(this.view.k * factor, 0.25, 2);
    const px = clientX - r.left;
    const py = clientY - r.top;
    this.view.x = px - ((px - this.view.x) * k) / this.view.k;
    this.view.y = py - ((py - this.view.y) * k) / this.view.k;
    this.view.k = k;
    this.applyView();
  }

  zoomBy(factor) {
    const r = this.host.getBoundingClientRect();
    this.zoomAt(factor, r.left + r.width / 2, r.top + r.height / 2);
  }

  fit() {
    const r = this.host.getBoundingClientRect();
    if (!this.nodes.length || !r.width) {
      this.view = { x: 40, y: 40, k: 1 };
      return this.applyView();
    }
    const xs = this.nodes.map((n) => n.x);
    const ys = this.nodes.map((n) => n.y);
    const minX = Math.min(...xs);
    const minY = Math.min(...ys);
    const w = Math.max(...xs) + NODE_W - minX;
    const h = Math.max(...ys) + NODE_H + 24 - minY;
    const k = clamp(Math.min((r.width - 100) / w, (r.height - 100) / h), 0.3, 1.15);
    this.view = { k, x: (r.width - w * k) / 2 - minX * k, y: (r.height - h * k) / 2 - minY * k };
    this.applyView();
  }

  centerOn(n) {
    const r = this.host.getBoundingClientRect();
    const k = this.view.k;
    const x = n.x * k + this.view.x;
    const y = n.y * k + this.view.y;
    if (x > 40 && y > 40 && x < r.width - NODE_W * k - 40 && y < r.height - NODE_H * k - 40) return;
    this.view.x = r.width / 2 - (n.x + NODE_W / 2) * k;
    this.view.y = r.height / 2 - (n.y + NODE_H / 2) * k;
    this.applyView();
  }

  // ---------------------------------------------------------------- rendering
  render() {
    this.renderNodes();
    this.renderEdges();
    this.empty.classList.toggle('hidden', this.nodes.length > 0 || this.readOnly);
  }

  subtitle(n) {
    const p = (k) => paramOf(n, k);
    switch (n.type) {
      case 'User': return `${fmtNum(p('users'))} users × ${p('requests_per_user')}/s`;
      case 'CDN': return `hit ${pct(p('cache_hit_ratio'))}`;
      case 'Cache': return `hit ${pct(p('hit_ratio'))} · ${p('replicas')} node${p('replicas') === 1 ? '' : 's'}`;
      case 'Database': {
        const parts = [String(p('engine')).toUpperCase()];
        if (p('read_replicas') > 0) parts.push(`${p('read_replicas')} replica${p('read_replicas') === 1 ? '' : 's'}`);
        if (p('shards') > 1) parts.push(`${p('shards')} shards`);
        return parts.join(' · ');
      }
      case 'SearchIndex': return `${p('shards')} shard${p('shards') === 1 ? '' : 's'} · ${p('replicas')} copies`;
      case 'Queue': return `holds ${fmtNum(p('max_depth'))}`;
      case 'ThirdPartyAPI': return `${p('provider')} · ${p('base_latency')} ms`;
      case 'BlobStorage': return `${p('storage_class')}`;
      case 'KafkaTopic': return `${plural(p('partitions'), 'partition')} · ${p('replication_factor')} ${p('replication_factor') === 1 ? 'copy' : 'copies'}`;
      case 'ConsumerGroup': return p('autoscaling') ? `autoscale ${p('autoscale_min')}–${p('autoscale_max')}` : plural(p('replicas'), 'consumer');
      case 'StreamProcessor': return `${plural(p('replicas'), 'instance')} · ${p('state')}`;
      case 'KafkaConnect': return `${p('mode')} · ${p('connector')}`;
      case 'SchemaRegistry': return `${p('format')} · ${p('compatibility')}`;
      case 'MirrorMaker': return plural(p('replicas'), 'task');
      default:
        if (p('autoscaling')) return `autoscale ${p('autoscale_min')}–${p('autoscale_max')}`;
        if (p('replicas') !== undefined) return `${p('replicas')} instance${p('replicas') === 1 ? '' : 's'}`;
        return COMPONENTS[n.type].label;
    }
  }

  renderNodes() {
    const ov = this.overlay;
    const focusedId = document.activeElement?.closest?.('.cv-node')?.dataset.id;
    this.nodeLayer.innerHTML = this.nodes.map((n) => {
      const comp = COMPONENTS[n.type];
      const m = ov?.nodes.get(n.id);
      const cls = ['cv-node', `cat-${comp.category}`];
      const selected = this.sel?.kind === 'node' && this.sel.id === n.id;
      if (selected) cls.push('selected');
      if (m) cls.push(`st-${m.status}`);
      if (this.flashIds.has(n.id)) cls.push('flash');
      const name = n.label === comp.label ? n.label : `${n.label} (${comp.label})`;
      return `<div class="${cls.join(' ')}" data-id="${esc(n.id)}" style="left:${n.x}px;top:${n.y}px" title="${esc(comp.label)}" role="button" tabindex="0" aria-pressed="${selected}" aria-label="${esc(m ? `${name}: ${m.text}` : name)}">
        <div class="cv-badge" aria-hidden="true">${esc(comp.abbr)}</div>
        <div class="cv-text"><div class="cv-label">${esc(n.label)}</div><div class="cv-sub">${esc(this.subtitle(n))}</div></div>
        ${m ? `<div class="cv-metric">${esc(m.text)}</div>` : ''}
        ${this.readOnly ? '' : `<div class="cv-port" title="Drag to another box to connect" role="button" aria-label="Connect ${esc(n.label)} to another box"></div>`}
      </div>`;
    }).join('');
    if (focusedId) this.nodeLayer.querySelector(`[data-id="${CSS.escape(focusedId)}"]`)?.focus({ preventScroll: true });
  }

  edgePath(e) {
    const a = this.node(e.source);
    const b = this.node(e.target);
    if (!a || !b) return null;
    const x1 = a.x + NODE_W;
    const y1 = a.y + NODE_H / 2;
    const x2 = b.x;
    const y2 = b.y + NODE_H / 2;
    const dx = x2 >= x1 ? Math.max(40, (x2 - x1) * 0.5) : 120;
    const c1 = [x1 + dx, y1];
    const c2 = [x2 - dx, y2];
    const mid = [(x1 + 3 * c1[0] + 3 * c2[0] + x2) / 8, (y1 + 3 * c1[1] + 3 * c2[1] + y2) / 8];
    return { d: `M${x1},${y1} C${c1[0]},${c1[1]} ${c2[0]},${c2[1]} ${x2 - 2},${y2}`, mid };
  }

  // Chrome skips repainting SVG text that overflows a tiny <svg>, so size it to the content.
  fitSvg() {
    const pad = 400;
    const xs = this.nodes.map((n) => n.x);
    const ys = this.nodes.map((n) => n.y);
    const minX = Math.min(0, ...xs) - pad;
    const minY = Math.min(0, ...ys) - pad;
    const w = Math.max(0, ...xs) + NODE_W + pad - minX;
    const h = Math.max(0, ...ys) + NODE_H + pad - minY;
    Object.assign(this.svg.style, { left: `${minX}px`, top: `${minY}px`, width: `${w}px`, height: `${h}px` });
    this.svg.setAttribute('viewBox', `${minX} ${minY} ${w} ${h}`);
  }

  renderEdges() {
    this.fitSvg();
    const ov = this.overlay;
    const parts = [];
    for (const e of this.edges) {
      const p = this.edgePath(e);
      if (!p) continue;
      const cls = ['cv-edge-g'];
      if (e.kind === 'async' || alwaysAsync(this.node(e.source)?.type, this.node(e.target)?.type)) cls.push('async');
      if (e.optional) cls.push('optional');
      const selected = this.sel?.kind === 'edge' && this.sel.id === e.id;
      if (selected) cls.push('selected');
      let width = 1.8;
      const rps = ov?.edges.get(e.id);
      if (ov && rps !== undefined) {
        if (rps > 0.5) cls.push('flow');
        width = 1.5 + 3.5 * (Math.log1p(rps) / Math.log1p(ov.max));
      }
      if (ov?.nodes.get(e.target)?.status === 'down') cls.push('dead');
      const labels = [];
      if (e.ratio !== null && e.ratio !== undefined) labels.push(`${Math.round(e.ratio * 100)}%`);
      if (e.optional) labels.push('optional');
      if (ov && rps > 0.5) labels.push(`${fmtNum(rps)}/s`);
      const marker = selected || cls.includes('flow') ? 'url(#cv-arrow-on)' : 'url(#cv-arrow)';
      parts.push(`<g class="${cls.join(' ')}" data-edge="${esc(e.id)}">
        <path class="cv-hit" d="${p.d}"/>
        <path class="cv-line" d="${p.d}" marker-end="${marker}" style="stroke-width:${width.toFixed(2)}px"/>
        ${labels.length ? `<text class="cv-edge-label" x="${p.mid[0]}" y="${p.mid[1] - 6}" text-anchor="middle">${esc(labels.join(' · '))}</text>` : ''}
      </g>`);
    }
    this.edgeLayer.innerHTML = parts.join('');
    this.renderParticles();
  }

  // ---------------------------------------------------------------- input
  onDown(e) {
    if (e.button !== 0) return;
    if (e.target.closest('.cv-zoom, .cv-banner, .cv-hud')) return;
    const port = e.target.closest('.cv-port');
    const nodeEl = e.target.closest('.cv-node');
    if (nodeEl && !port) nodeEl.focus({ preventScroll: true });
    else this.host.focus({ preventScroll: true });
    const edgeEl = e.target.closest('.cv-edge-g');
    if (port && nodeEl && !this.readOnly) {
      e.preventDefault();
      this.gesture = { type: 'connect', from: nodeEl.dataset.id };
      this.temp.classList.remove('hidden');
      this.drawTemp(e);
    } else if (nodeEl) {
      const n = this.node(nodeEl.dataset.id);
      this.select({ kind: 'node', id: n.id });
      if (!this.readOnly) this.gesture = { type: 'drag', id: n.id, start: this.toWorld(e.clientX, e.clientY), orig: { x: n.x, y: n.y }, moved: false };
    } else if (edgeEl) {
      this.select({ kind: 'edge', id: edgeEl.dataset.edge });
    } else {
      if (this.sel) this.select(null);
      this.gesture = { type: 'pan', sx: e.clientX, sy: e.clientY, ox: this.view.x, oy: this.view.y };
      this.host.style.cursor = 'grabbing';
    }
  }

  drawTemp(e) {
    const a = this.node(this.gesture.from);
    const p = this.toWorld(e.clientX, e.clientY);
    const x1 = a.x + NODE_W;
    const y1 = a.y + NODE_H / 2;
    const dx = Math.max(30, Math.abs(p.x - x1) * 0.5);
    this.temp.setAttribute('d', `M${x1},${y1} C${x1 + dx},${y1} ${p.x - dx},${p.y} ${p.x},${p.y}`);
  }

  onMove(e) {
    const g = this.gesture;
    if (!g) return;
    if (g.type === 'connect') {
      this.drawTemp(e);
    } else if (g.type === 'drag') {
      const p = this.toWorld(e.clientX, e.clientY);
      const dx = p.x - g.start.x;
      const dy = p.y - g.start.y;
      if (!g.moved && Math.hypot(dx, dy) < 3) return;
      if (!g.moved) {
        this.snapshot();
        g.moved = true;
      }
      const n = this.node(g.id);
      n.x = snap(g.orig.x + dx);
      n.y = snap(g.orig.y + dy);
      const el = this.nodeLayer.querySelector(`[data-id="${CSS.escape(n.id)}"]`);
      if (el) {
        el.style.left = `${n.x}px`;
        el.style.top = `${n.y}px`;
      }
      this.renderEdges();
    } else if (g.type === 'pan') {
      this.view.x = g.ox + (e.clientX - g.sx);
      this.view.y = g.oy + (e.clientY - g.sy);
      this.applyView();
    }
  }

  onUp(e) {
    const g = this.gesture;
    if (!g) return;
    this.gesture = null;
    if (g.type === 'connect') {
      this.temp.classList.add('hidden');
      const target = document.elementFromPoint(e.clientX, e.clientY)?.closest('.cv-node');
      if (target && this.host.contains(target)) this.connect(g.from, target.dataset.id);
    } else if (g.type === 'drag' && g.moved) {
      this.changed('move');
    } else if (g.type === 'pan') {
      this.host.style.cursor = '';
    }
  }

  onWheel(e) {
    e.preventDefault();
    if (e.ctrlKey || e.metaKey) {
      this.zoomAt(Math.exp(-e.deltaY * 0.0025), e.clientX, e.clientY);
    } else {
      this.view.x -= e.deltaX;
      this.view.y -= e.deltaY;
      this.applyView();
    }
  }

  onKey(e) {
    const nodeEl = e.target.closest?.('.cv-node');
    if (e.target !== this.host && !nodeEl) return;
    const mod = e.metaKey || e.ctrlKey;
    if (nodeEl && (e.key === 'Enter' || e.key === ' ')) {
      e.preventDefault();
      this.select({ kind: 'node', id: nodeEl.dataset.id });
    } else if ((e.key === 'Delete' || e.key === 'Backspace') && this.sel) {
      e.preventDefault();
      this.removeSelected();
    } else if (mod && e.key.toLowerCase() === 'z') {
      e.preventDefault();
      if (e.shiftKey) this.redo();
      else this.undo();
    } else if (mod && e.key.toLowerCase() === 'y') {
      e.preventDefault();
      this.redo();
    } else if (mod && e.key.toLowerCase() === 'd') {
      e.preventDefault();
      this.duplicate();
    } else if (e.key === 'Escape') {
      this.select(null);
    } else if (this.sel?.kind === 'node' && e.key.startsWith('Arrow') && !this.readOnly) {
      e.preventDefault();
      const n = this.node(this.sel.id);
      const step = e.shiftKey ? 50 : GRID;
      this.beginEdit();
      if (e.key === 'ArrowLeft') n.x -= step;
      if (e.key === 'ArrowRight') n.x += step;
      if (e.key === 'ArrowUp') n.y -= step;
      if (e.key === 'ArrowDown') n.y += step;
      this.endEdit();
      this.render();
      this.changed('move');
    }
  }
}

// ================================================================ workspace: heuristics
// Instant, rule-of-thumb checks that run in the browser on every edit. The server's rule
// engine and simulator are the real judges; these hints just point you in a direction.

const p = (n, key) => {
  for (const b of ['basic', 'advanced', 'security']) if (n.config?.[b]?.[key] !== undefined) return n.config[b][key];
  return paramSpec(n.type, key)?.default;
};

function facts(graph) {
  const nodes = graph.nodes;
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const out = new Map(nodes.map((n) => [n.id, []]));
  for (const e of graph.edges) if (out.has(e.source) && byId.has(e.target)) out.get(e.source).push(e);
  const ofType = (t) => nodes.filter((n) => n.type === t);
  const has = (t) => ofType(t).length > 0;
  const instances = (n) => (p(n, 'autoscaling') ? Math.max(2, p(n, 'autoscale_max')) : p(n, 'replicas') || 1);
  const users = ofType('User');
  const onPath = new Set();
  const stack = users.map((u) => u.id);
  while (stack.length) {
    const id = stack.pop();
    if (onPath.has(id)) continue;
    onPath.add(id);
    for (const e of out.get(id) || []) if (e.kind !== 'async' && !['Queue', 'KafkaTopic'].includes(byId.get(e.target)?.type)) stack.push(e.target);
  }
  const inputs = (id) => graph.edges.filter((e) => e.target === id && byId.get(e.source)?.type === 'KafkaTopic').map((e) => byId.get(e.source));
  const readers = nodes.filter((n) => inputs(n.id).length);
  const partitionsFor = (n) => inputs(n.id).reduce((s, t) => s + (p(t, 'partitions') || 1), 0);
  return { nodes, byId, out, ofType, has, instances, users, onPath, edges: graph.edges, readers, partitionsFor };
}

const consumerCount = (n) => (p(n, 'autoscaling') ? p(n, 'autoscale_min') : p(n, 'replicas') || 1);
const DLQ_TYPES = new Set(['ConsumerGroup', 'StreamProcessor', 'KafkaConnect']);
const writesTo = (f, n, type) => (f.out.get(n.id) || []).some((e) => f.byId.get(e.target)?.type === type && (type !== 'Database' || e.kind !== 'async'));

const RULES = [
  { sev: 'critical', when: 'Now', test: (f) => !f.users.length, title: 'Add Users', text: 'Drag a Users box onto the canvas. Traffic starts there.' },
  { sev: 'critical', when: 'Now', test: (f) => f.users.some((u) => !f.out.get(u.id).length), ids: (f) => f.users.map((u) => u.id), title: 'Connect your users', text: 'Draw an arrow from Users to the first part of your system (a CDN, load balancer or server).' },
  { sev: 'critical', when: 'Now', test: (f) => f.users.some((u) => f.out.get(u.id).some((e) => ['Database', 'Cache', 'Queue', 'SearchIndex', 'KafkaTopic'].includes(f.byId.get(e.target)?.type))), title: 'Users talk to storage directly', text: 'Put an app server in between. It checks permissions and business rules.' },
  { sev: 'warn', when: 'Now', test: (f) => f.ofType('Server').concat(f.ofType('Service')).some((n) => f.onPath.has(n.id) && f.instances(n) < 2), ids: (f) => f.ofType('Server').concat(f.ofType('Service')).filter((n) => f.instances(n) < 2).map((n) => n.id), title: 'Only one server', text: 'If it crashes or restarts, everything stops. Run 2 or more, or turn on autoscaling.' },
  { sev: 'warn', when: 'Now', test: (f) => f.ofType('Database').some((d) => !f.ofType('Cache').length && p(d, 'read_ratio') >= 0.7), ids: (f) => f.ofType('Database').map((d) => d.id), title: 'Every read hits the database', text: 'Most traffic is reads. A cache would answer repeated reads in memory.' },
  { sev: 'warn', when: 'Now', test: (f) => f.users.some((u) => f.out.get(u.id).filter((e) => ['Server', 'Service'].includes(f.byId.get(e.target)?.type)).length >= 2), title: 'No load balancer', text: 'Users are connected straight to several servers. A load balancer spreads the work and skips dead servers.' },
  { sev: 'good', when: 'Now', test: (f) => f.has('Cache') && f.has('Database'), title: 'Cache in front of the database', text: 'Repeated reads are answered from memory.' },
  { sev: 'good', when: 'Now', test: (f) => f.ofType('LoadBalancer').some((lb) => f.out.get(lb.id).length >= 2 || f.out.get(lb.id).some((e) => f.instances(f.byId.get(e.target)) >= 2)), title: 'Load is spread', text: 'A load balancer shares traffic across several servers.' },

  { sev: 'warn', when: 'Spike', test: (f) => !f.ofType('Server').concat(f.ofType('Service')).some((n) => p(n, 'autoscaling')) && f.has('Server'), title: 'Servers cannot grow', text: 'With autoscaling off, a traffic spike hits a fixed number of servers.' },
  { sev: 'warn', when: 'Spike', test: (f) => !f.has('RateLimiter') && !f.ofType('APIGateway').some((g) => p(g, 'rate_limiting')) && f.users.length > 0, title: 'No rate limit', text: 'One noisy client or bot can use up capacity meant for everyone.' },
  { sev: 'good', when: 'Spike', test: (f) => f.has('CDN'), title: 'CDN takes the edge load', text: 'Cacheable requests are answered close to users.' },
  { sev: 'good', when: 'Spike', test: (f) => f.has('Queue') && f.has('Worker'), title: 'Queue soaks up bursts', text: 'Slow work waits in the queue instead of piling onto servers.' },

  { sev: 'critical', when: 'Failure', test: (f) => f.ofType('Database').some((d) => p(d, 'read_replicas') === 0), ids: (f) => f.ofType('Database').filter((d) => p(d, 'read_replicas') === 0).map((d) => d.id), title: 'Database has no copy', text: 'If it fails, reads and writes stop. Add a read replica and turn on failover.' },
  { sev: 'warn', when: 'Failure', test: (f) => f.ofType('Database').some((d) => p(d, 'read_replicas') > 0 && !p(d, 'failover')), title: 'No automatic failover', text: 'A replica exists, but nothing promotes it when the main database dies.' },
  { sev: 'warn', when: 'Failure', test: (f) => f.ofType('Cache').some((c) => !p(c, 'request_coalescing') && p(c, 'replicas') < 2) && f.has('Database'), ids: (f) => f.ofType('Cache').map((c) => c.id), title: 'Cache restart would flood the database', text: 'Turn on request coalescing, or run 2 or more cache nodes.' },
  { sev: 'warn', when: 'Failure', test: (f) => f.ofType('LoadBalancer').some((lb) => !p(lb, 'health_checks')), title: 'Health checks are off', text: 'The load balancer keeps sending traffic to dead servers.' },
  { sev: 'good', when: 'Failure', test: (f) => f.ofType('Database').some((d) => p(d, 'read_replicas') > 0 && p(d, 'failover')), title: 'Database can fail over', text: 'A replica takes over if the main database dies.' },

  { sev: 'critical', when: 'Async', test: (f) => f.ofType('Queue').some((q) => !f.out.get(q.id).length), ids: (f) => f.ofType('Queue').filter((q) => !f.out.get(q.id).length).map((q) => q.id), title: 'Nobody reads the queue', text: 'Connect the queue to a Worker, or messages pile up until it is full.' },
  { sev: 'warn', when: 'Async', test: (f) => f.ofType('Worker').some((w) => !p(w, 'idempotent')) && f.has('Queue'), ids: (f) => f.ofType('Worker').filter((w) => !p(w, 'idempotent')).map((w) => w.id), title: 'Jobs might run twice', text: 'Queues can deliver a message again after a crash. Make workers idempotent.' },
  { sev: 'warn', when: 'Async', test: (f) => [...f.onPath].some((id) => f.byId.get(id)?.type === 'ThirdPartyAPI' && ['email', 'sms', 'push'].includes(p(f.byId.get(id), 'provider'))), title: 'Users wait for an email or SMS', text: 'Send messages through a queue and a worker, so users do not wait.' },
  { sev: 'warn', when: 'Async', test: (f) => f.edges.some((e) => f.byId.get(e.target)?.type === 'ThirdPartyAPI' && !p(f.byId.get(e.target), 'circuit_breaker') && !p(f.byId.get(e.source), 'circuit_breaker')), title: 'Outside service without a circuit breaker', text: 'If the provider gets slow, your servers will wait and retry. A circuit breaker fails fast instead.' },

  { sev: 'critical', when: 'Kafka', test: (f) => f.ofType('KafkaTopic').some((t) => !f.out.get(t.id).length), ids: (f) => f.ofType('KafkaTopic').filter((t) => !f.out.get(t.id).length).map((t) => t.id), title: 'Nobody reads the topic', text: 'Connect a Consumer Group, Stream Processor or Kafka Connect sink. Kafka never pushes data anywhere by itself.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.ofType('KafkaTopic').some((t) => p(t, 'replication_factor') < 2), ids: (f) => f.ofType('KafkaTopic').filter((t) => p(t, 'replication_factor') < 2).map((t) => t.id), title: 'One copy of each partition', text: 'A broker failure loses its partitions. Use 3 copies with min.insync.replicas 2.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.ofType('KafkaTopic').some((t) => p(t, 'acks') !== 'all'), ids: (f) => f.ofType('KafkaTopic').filter((t) => p(t, 'acks') !== 'all').map((t) => t.id), title: 'Producers do not wait for copies', text: 'With acks=1 or 0, a leader crash loses recent messages. Use acks=all.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.readers.some((n) => consumerCount(n) > f.partitionsFor(n)), ids: (f) => f.readers.filter((n) => consumerCount(n) > f.partitionsFor(n)).map((n) => n.id), title: 'More consumers than partitions', text: 'Each partition goes to one consumer per group, so the extra consumers sit idle. Add partitions.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.ofType('KafkaTopic').some((t) => p(t, 'partition_key') === 'skewed'), ids: (f) => f.ofType('KafkaTopic').filter((t) => p(t, 'partition_key') === 'skewed').map((t) => t.id), title: 'A hot key', text: 'Messages with the busiest key all land on one partition and one consumer. Key by something more even.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.ofType('ConsumerGroup').some((n) => p(n, 'commit') === 'before_processing'), ids: (f) => f.ofType('ConsumerGroup').filter((n) => p(n, 'commit') === 'before_processing').map((n) => n.id), title: 'Messages can be skipped', text: 'Committing before processing loses the messages in flight when a consumer crashes. Commit after.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.ofType('ConsumerGroup').some((n) => p(n, 'commit') === 'after_processing' && !p(n, 'idempotent')), ids: (f) => f.ofType('ConsumerGroup').filter((n) => p(n, 'commit') === 'after_processing' && !p(n, 'idempotent')).map((n) => n.id), title: 'Messages can be processed twice', text: 'After a crash, messages since the last commit come back. Make processing idempotent.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.readers.some((n) => DLQ_TYPES.has(n.type) && !p(n, 'dead_letter_topic')), ids: (f) => f.readers.filter((n) => DLQ_TYPES.has(n.type) && !p(n, 'dead_letter_topic')).map((n) => n.id), title: 'One bad message can block a partition', text: 'Send messages that keep failing to a dead letter topic.' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.nodes.some((n) => !f.readers.includes(n) && writesTo(f, n, 'Database') && writesTo(f, n, 'KafkaTopic')), ids: (f) => f.nodes.filter((n) => !f.readers.includes(n) && writesTo(f, n, 'Database') && writesTo(f, n, 'KafkaTopic')).map((n) => n.id), title: 'Database and Kafka written separately', text: 'A crash between the two writes leaves them disagreeing. Use an outbox with CDC (Kafka Connect).' },
  { sev: 'warn', when: 'Kafka', test: (f) => f.has('KafkaTopic') && !f.has('SchemaRegistry') && f.readers.length >= 2, title: 'Nothing checks message formats', text: 'A careless producer change can break every consumer. Add a Schema Registry.' },
  { sev: 'good', when: 'Kafka', test: (f) => f.has('KafkaTopic') && f.ofType('KafkaTopic').every((t) => p(t, 'replication_factor') >= 3 && p(t, 'acks') === 'all' && p(t, 'min_insync_replicas') >= 2), title: 'Durable topics', text: 'Three copies and acks=all: a broker can fail without losing a confirmed message.' },
  { sev: 'good', when: 'Kafka', test: (f) => f.edges.some((e) => f.byId.get(e.source)?.type === 'Database' && f.byId.get(e.target)?.type === 'KafkaConnect'), title: 'Events come from the database log', text: 'CDC publishes exactly what was committed, so events and data always agree.' },
  { sev: 'good', when: 'Kafka', test: (f) => f.has('SchemaRegistry'), title: 'Schemas are checked', text: 'Breaking format changes are rejected before consumers see them.' },

  { sev: 'warn', when: 'Global', test: (f) => f.users.length && !f.has('CDN'), title: 'No CDN', text: 'Far-away users pay a long round trip for every file. A CDN serves them from nearby.' },
  { sev: 'warn', when: 'Global', test: (f) => f.ofType('CDN').some((c) => p(c, 'cache_api_responses')), title: 'CDN caches personal data', text: 'User-specific answers can be shown to the wrong person. Cache public content only.' },
];

function confidence(f) {
  const clamp = (v) => Math.max(1, Math.min(3, v));
  const compute = f.ofType('Server').concat(f.ofType('Service'));
  const dbs = f.ofType('Database');
  const scalability = clamp((f.ofType('LoadBalancer').length || compute.some((n) => p(n, 'autoscaling')) ? 1 : 0) + (f.has('Cache') || f.has('CDN') ? 1 : 0) + (dbs.some((d) => p(d, 'read_replicas') > 0 || p(d, 'shards') > 1) ? 1 : 0));
  const reliability = clamp((compute.length && compute.every((n) => f.instances(n) >= 2) ? 1 : 0) + (dbs.length && dbs.every((d) => p(d, 'read_replicas') > 0 && p(d, 'failover')) ? 1 : 0) + (f.ofType('Cache').every((c) => p(c, 'request_coalescing') || p(c, 'replicas') >= 2) ? 1 : 0));
  const speed = clamp((f.has('Cache') || f.has('CDN') ? 1 : 0) + (![...f.onPath].some((id) => f.byId.get(id)?.type === 'ThirdPartyAPI' && ['email', 'sms', 'push'].includes(p(f.byId.get(id), 'provider'))) ? 1 : 0) + (f.has('SearchIndex') || f.has('Queue') ? 1 : 0));
  const security = clamp((f.has('RateLimiter') || f.ofType('APIGateway').some((g) => p(g, 'rate_limiting')) ? 1 : 0) + (f.nodes.some((n) => ['LoadBalancer', 'CDN', 'APIGateway', 'ReverseProxy'].includes(n.type) && p(n, 'https')) ? 1 : 0) + (f.nodes.some((n) => ['Server', 'APIGateway', 'Service', 'WebSocketServer'].includes(n.type) && p(n, 'auth') && p(n, 'auth') !== 'none') ? 1 : 0));
  return { Scalability: scalability, Reliability: reliability, Speed: speed, Security: security };
}

function liveHints(graph) {
  const f = facts(graph);
  const hints = [];
  for (const r of RULES) {
    let hit = false;
    try { hit = r.test(f); } catch { hit = false; }
    if (hit) hints.push({ sev: r.sev, when: r.when, title: r.title, text: r.text, ids: r.ids ? r.ids(f) : [] });
  }
  return { hints, confidence: confidence(f) };
}

const SEV_LABEL = { critical: 'Fix', warn: 'Risk', good: 'Good' };
const LEVEL = ['', 'Weak', 'OK', 'Strong'];

function renderHints(el, { hints, confidence }, preview, { onHint } = {}) {
  const groups = ['Now', 'Spike', 'Failure', 'Async', 'Kafka', 'Global'];
  const busiest = preview?.node_metrics?.filter((n) => n.status !== 'source' && n.demand_rps > 0).sort((a, b) => b.utilization - a.utilization)[0];
  setHtml(el, html`
    ${preview ? html`<div class="section-card">
      <h3 style="margin-top:0">Live preview <span class="muted small">at normal traffic</span></h3>
      <div class="stat-grid" style="margin-bottom:6px">
        <div class="stat-box"><span>Errors</span><strong class="${preview.error_rate > 0.05 ? 'bad' : preview.error_rate > 0.01 ? 'warn' : 'good'}">${fmtPct(preview.error_rate, 1)}</strong></div>
        <div class="stat-box"><span>Slow requests (p95)</span><strong>${fmtMs(preview.latency_ms.p95)}</strong></div>
        <div class="stat-box"><span>Traffic in</span><strong>${fmtNum(preview.incoming_rps)}/s</strong></div>
        <div class="stat-box"><span>Busiest part</span><strong class="${busiest && busiest.utilization > 1 ? 'bad' : busiest && busiest.utilization >= 0.85 ? 'warn' : ''}">${busiest ? fmtLoad(busiest.utilization) : '–'}</strong></div>
      </div>
      <p class="muted small" style="margin:0">${busiest ? `Busiest: ${busiest.label}. Colours on the canvas update as you edit.` : 'No part gets traffic yet. Draw arrows from Users.'}</p>
    </div>` : ''}
    <h3>How strong is it?</h3>
    <div class="kv" style="margin-bottom: 14px">${Object.entries(confidence).map(([k, v]) => html`<span>${k}</span><strong class="${v === 3 ? 'good' : v === 2 ? 'warn' : 'bad'}">${LEVEL[v]}</strong>`)}</div>
    ${groups.map((g) => {
      const list = hints.filter((h) => h.when === g);
      if (!list.length) return '';
      return html`<h3>${{ Now: 'Right now', Spike: 'When traffic spikes', Failure: 'When something fails', Async: 'Background work', Kafka: 'Kafka', Global: 'Users far away' }[g]}</h3>
        ${list.map((h, i) => html`<div class="finding" data-hint="${g}:${i}"><div class="f-top"><span class="sev sev-${h.sev === 'warn' ? 'warning' : h.sev}">${SEV_LABEL[h.sev]}</span><span class="f-title">${h.title}</span></div><p>${h.text}</p></div>`)}`;
    })}
    <p class="muted small">These are quick rules of thumb. Run the tests to get real numbers.</p>`);
  el.onclick = (e) => {
    const card = e.target.closest('[data-hint]');
    if (!card) return;
    const [g, i] = card.dataset.hint.split(':');
    const h = hints.filter((x) => x.when === g)[Number(i)];
    if (h?.ids.length && onHint) onHint(h.ids);
  };
}

// ================================================================ workspace: inspector
function kafkaRows(k) {
  if (!k) return '';
  const row = (label, value, cls = '') => html`<div class="row"><span class="muted">${label}</span><strong class="${cls}">${value}</strong></div>`;
  if (k.role === 'topic') {
    return html`${row('Brokers up', `${k.brokers_alive} of ${k.brokers}`, k.brokers_alive < k.brokers ? 'warn' : '')}
      ${row('Partitions', k.partitions)}
      ${k.write_fail > 0 ? row('Writes rejected', `${Math.round(k.write_fail * 100)}%`, 'bad') : ''}
      ${k.hot_share > 1.5 / k.partitions ? row('Hottest partition', `${Math.round(k.hot_share * 100)}% of messages`, 'warn') : ''}`;
  }
  return html`${row('Consumers working', k.idle > 0 ? `${k.active} (${k.idle} idle)` : k.active, k.idle > 0 ? 'warn' : '')}
    ${row('Lag grows by', `${fmtNum(k.lag_rps)}/s`, k.lag_rps > 0.5 ? 'bad' : '')}
    ${row('Data age', k.freshness_ms === null ? 'never catches up' : fmtDuration(k.freshness_ms / 1000), k.freshness_ms === null || k.freshness_ms > 5000 ? 'bad' : '')}
    ${k.stuck_share > 0 ? row('Stuck', `${Math.round(k.stuck_share * 100)}% of messages`, 'bad') : ''}
    ${k.rebalancing ? row('Rebalancing', 'again and again', 'bad') : ''}`;
}

const BLOCK_TITLES = { basic: 'Basic', advanced: 'Advanced', security: 'Security' };

function fieldHtml(spec, value, disabled) {
  const id = `f-${spec.key}`;
  let control;
  if (spec.type === 'toggle') {
    control = html`<label class="switch"><input type="checkbox" id="${id}" data-key="${spec.key}" ${value ? 'checked' : ''} ${disabled ? 'disabled' : ''}><span></span></label>`;
  } else if (spec.type === 'select') {
    control = html`<select class="input" id="${id}" data-key="${spec.key}" ${disabled ? 'disabled' : ''}>${spec.options.map((o) => html`<option value="${o}" ${o === value ? 'selected' : ''}>${o.replace(/_/g, ' ')}</option>`)}</select>`;
  } else {
    control = html`<input class="input" type="number" id="${id}" data-key="${spec.key}" value="${value}" ${spec.min !== undefined ? html`min="${spec.min}"` : ''} ${spec.max !== undefined ? html`max="${spec.max}"` : ''} step="${spec.step || 'any'}" ${disabled ? 'disabled' : ''}>`;
  }
  return html`<div class="insp-field">
    <label for="${id}">${spec.label}${spec.unit ? html`<span class="unit">${spec.unit}</span>` : ''}</label>
    ${control}
    ${spec.help ? html`<div class="help">${spec.help}</div>` : ''}
  </div>`;
}

class Inspector {
  constructor(el, editor, { metrics = () => null, emptyExtra = () => '', readOnly = false } = {}) {
    this.el = el;
    this.editor = editor;
    this.metrics = metrics;
    this.emptyExtra = emptyExtra;
    this.readOnly = readOnly;
    editor.addEventListener('select', () => this.render());
    editor.addEventListener('change', (e) => {
      if (['undo', 'redo', 'load', 'remove', 'disconnect'].includes(e.detail.reason)) this.render();
      else if (e.detail.reason === 'connect' && editor.sel?.kind === 'node') this.render();
    });
    el.addEventListener('focusout', () => editor.endEdit());
    el.addEventListener('input', (e) => this.onInput(e));
    el.addEventListener('change', (e) => this.onInput(e, true));
    el.addEventListener('click', (e) => this.onClick(e));
    this.render();
  }

  setReadOnly(on) {
    this.readOnly = on;
    this.render();
  }

  refreshMetrics() {
    const box = this.el.querySelector('[data-metrics]');
    if (box && this.editor.sel?.kind === 'node') setHtml(box, this.metricsHtml(this.editor.sel.id));
  }

  render() {
    const sel = this.editor.sel;
    if (sel?.kind === 'node' && this.editor.node(sel.id)) return this.renderNode(this.editor.node(sel.id));
    if (sel?.kind === 'edge' && this.editor.edge(sel.id)) return this.renderEdge(this.editor.edge(sel.id));
    return this.renderEmpty();
  }

  renderEmpty() {
    const counts = new Map();
    for (const n of this.editor.nodes) {
      const cat = COMPONENTS[n.type].category;
      counts.set(cat, (counts.get(cat) || 0) + 1);
    }
    const ro = this.readOnly || this.editor.readOnly;
    setHtml(this.el, html`
      <h3>Nothing selected</h3>
      <p class="panel-intro">${ro ? 'Click a box or an arrow to see its settings. The design cannot be changed right now.' : 'Click a box to change its settings, or click an arrow to change how traffic flows through it.'}</p>
      ${this.emptyExtra()}
      <h3>Your design</h3>
      ${this.editor.nodes.length ? html`<div class="kv">${CATEGORIES.filter((c) => counts.get(c.id)).map((c) => html`<span>${c.label}</span><strong>${counts.get(c.id)}</strong>`)}<span>Connections</span><strong>${this.editor.edges.length}</strong></div>` : html`<p class="muted small">No components yet.</p>`}
      <h3>Shortcuts</h3>
      <ul class="shortcuts">
        ${ro ? '' : html`<li><span>Connect two boxes</span><span>drag from the dot</span></li>
        <li><span>Add after a box</span><span>select it, click a part</span></li>
        <li><span>Delete selected</span><span class="kbd">Del</span></li>
        <li><span>Undo / redo</span><span><span class="kbd">⌘/Ctrl Z</span> <span class="kbd">⇧</span></span></li>
        <li><span>Duplicate</span><span class="kbd">⌘/Ctrl D</span></li>`}
        <li><span>Move the view</span><span>drag empty space</span></li>
        <li><span>Zoom</span><span>⌘/Ctrl + scroll</span></li>
      </ul>`);
  }

  metricsHtml(id) {
    const sim = this.metrics();
    const m = sim?.node_metrics?.find((x) => x.id === id);
    if (!m || m.status === 'source') return '';
    const status = { ok: 'Healthy', warm: 'Busy', hot: 'Very busy', overloaded: 'Overloaded', down: 'Down', idle: 'No traffic' }[m.status] || m.status;
    return html`<div class="metrics-box">
      <div class="row"><span class="muted">Status</span><strong class="${m.status === 'overloaded' || m.status === 'down' ? 'bad' : m.status === 'hot' || m.status === 'warm' ? 'warn' : 'good'}">${status}</strong></div>
      <div class="row"><span class="muted">How busy</span><strong>${fmtLoad(m.utilization)}</strong></div>
      <div class="row"><span class="muted">Traffic in</span><strong>${fmtNum(m.demand_rps)}/s</strong></div>
      ${m.capacity_rps !== null ? html`<div class="row"><span class="muted">Can handle</span><strong>${fmtNum(m.capacity_rps)}/s</strong></div>` : ''}
      <div class="row"><span class="muted">${m.type === 'KafkaTopic' ? 'Time per write' : 'Time per request'}</span><strong>${fmtMs(m.latency_ms)}</strong></div>
      ${kafkaRows(m.kafka)}
      ${m.notes.length ? html`<ul>${m.notes.map((t) => html`<li>${t}</li>`)}</ul>` : ''}
    </div>`;
  }

  renderNode(n) {
    const comp = COMPONENTS[n.type];
    const ro = this.readOnly || this.editor.readOnly;
    const block = (name) => {
      const specs = comp[name];
      if (!specs.length) return '';
      return html`<details class="insp-group" ${name === 'basic' || (name === 'advanced' && !comp.basic.length) ? 'open' : ''}>
        <summary>${BLOCK_TITLES[name]}</summary>
        <div class="insp-fields">${specs.map((s) => fieldHtml(s, n.config[name][s.key], ro))}</div>
      </details>`;
    };
    setHtml(this.el, html`
      <div class="insp-head"><span class="cat-${comp.category}"><span class="pal-badge">${comp.abbr}</span></span><div><h3>${comp.label}</h3><span class="muted small">${n.id}</span></div></div>
      <p class="insp-desc">${comp.description}</p>
      <div data-metrics>${this.metricsHtml(n.id)}</div>
      <div class="insp-field" style="grid-template-columns: 1fr; margin-bottom: 12px"><label for="f-label">Name</label><input class="input" id="f-label" data-label value="${n.label}" maxlength="60" ${ro ? 'disabled' : ''}></div>
      ${block('basic')}${block('advanced')}${block('security')}
      ${this.arrowsHtml(n, ro)}
      ${ro ? '' : html`<div class="insp-actions"><button class="btn btn-sm" type="button" data-act="dup">Duplicate</button><button class="btn btn-sm btn-danger" type="button" data-act="del">Delete</button></div>`}`);
  }

  arrowsHtml(n, ro) {
    const ed = this.editor;
    const outs = ed.edges.filter((e) => e.source === n.id);
    const ins = ed.edges.filter((e) => e.target === n.id);
    const choices = ed.nodes.filter((o) => o.id !== n.id && o.type !== 'User' && !outs.some((e) => e.target === o.id));
    const row = (e, other) => html`<li><button class="link-btn" type="button" data-goto="${e.id}">${ed.node(other).label}</button>${ro ? '' : html`<button class="icon-btn" type="button" data-unlink="${e.id}" aria-label="Remove the arrow to ${ed.node(other).label}" title="Remove arrow">×</button>`}</li>`;
    return html`<details class="insp-group" open>
      <summary>Arrows</summary>
      <div class="insp-arrows">
        ${n.type === 'User' ? '' : html`<div class="muted small">Gets traffic from</div>
        ${ins.length ? html`<ul class="arrow-list">${ins.map((e) => html`<li><button class="link-btn" type="button" data-goto="${e.id}">${ed.node(e.source).label}</button></li>`)}</ul>` : html`<p class="muted small">Nothing yet. Nothing reaches this part.</p>`}`}
        <div class="muted small">Sends traffic to</div>
        ${outs.length ? html`<ul class="arrow-list">${outs.map((e) => row(e, e.target))}</ul>` : html`<p class="muted small">Nothing yet.</p>`}
        ${ro || !choices.length ? '' : html`<label class="sr-only" for="f-link">Add an arrow to</label>
          <select class="input" id="f-link" data-link><option value="">Add an arrow to…</option>${choices.map((o) => html`<option value="${o.id}">${o.label}</option>`)}</select>`}
      </div>
    </details>`;
  }

  renderEdge(e) {
    const src = this.editor.node(e.source);
    const dst = this.editor.node(e.target);
    const ro = this.readOnly || this.editor.readOnly;
    const forced = alwaysAsync(src.type, dst.type);
    const fromTopic = src.type === 'KafkaTopic';
    const sim = this.metrics();
    const flow = sim?.edge_flows?.find((x) => x.id === e.id);
    let desc = html`<strong>${src.label}</strong> sends requests to <strong>${dst.label}</strong>.`;
    if (dst.type === 'KafkaTopic') desc = html`<strong>${src.label}</strong> publishes messages to <strong>${dst.label}</strong>.`;
    else if (fromTopic) desc = html`<strong>${dst.label}</strong> reads every message from <strong>${src.label}</strong> at its own pace.`;
    else if (src.type === 'Database' && dst.type === 'KafkaConnect') desc = html`<strong>${dst.label}</strong> streams every committed change from <strong>${src.label}</strong>'s log.`;
    let kindHelp = 'Sync: the caller waits for the answer. Async: the caller moves on right away.';
    if (dst.type === 'Queue') kindHelp = 'Putting work on a queue never waits, so this is always async.';
    else if (dst.type === 'KafkaTopic') kindHelp = 'Publishing never waits for consumers, so this is always async. The producer only waits for the write itself (see acks on the topic).';
    else if (forced) kindHelp = 'Change data capture reads the database log, so it never slows down writes.';
    let ratioHelp = 'The share of requests that make this call, from 0 to 1. Empty means every request.';
    if (['LoadBalancer', 'APIGateway', 'ReverseProxy', 'RateLimiter', 'User', 'CDN'].includes(src.type)) ratioHelp = 'This part splits traffic between its arrows. Empty means an even split.';
    else if (fromTopic) ratioHelp = 'Every group reads every message. Set this below 1 only if the reader acts on just part of the stream.';
    setHtml(this.el, html`
      <h3>Connection</h3>
      <p class="insp-desc">${desc}</p>
      ${flow ? html`<div class="metrics-box"><div class="row"><span class="muted">Traffic on this arrow</span><strong>${fmtNum(flow.rps)}/s</strong></div></div>` : ''}
      <div class="insp-fields" style="padding: 0">
        <div class="insp-field"><label for="e-kind">Type</label>
          <select class="input" id="e-kind" data-edge="kind" ${ro || forced ? 'disabled' : ''}>
            <option value="sync" ${e.kind === 'sync' && !forced ? 'selected' : ''}>Waits (sync)</option>
            <option value="async" ${e.kind === 'async' || forced ? 'selected' : ''}>Fire and forget (async)</option>
          </select>
          <div class="help">${kindHelp}</div>
        </div>
        <div class="insp-field"><label for="e-ratio">Share of traffic</label>
          <input class="input" type="number" id="e-ratio" data-edge="ratio" min="0" max="1" step="0.05" value="${e.ratio ?? ''}" placeholder="auto" ${ro ? 'disabled' : ''}>
          <div class="help">${ratioHelp}</div>
        </div>
        ${dst.type === 'Database' ? html`<div class="insp-field"><label for="e-read">Read share</label>
          <input class="input" type="number" id="e-read" data-edge="read_share" min="0" max="1" step="0.05" value="${e.read_share ?? ''}" placeholder="auto" ${ro ? 'disabled' : ''}>
          <div class="help">Share of these calls that only read. Empty uses the database setting. A worker that only writes would be 0.</div>
        </div>` : ''}
        <label class="check"><input type="checkbox" data-edge="optional" ${e.optional ? 'checked' : ''} ${ro ? 'disabled' : ''}> Optional: the request still works if this call fails (it still costs time).</label>
      </div>
      ${ro ? '' : html`<div class="insp-actions"><button class="btn btn-sm btn-danger" type="button" data-act="del">Delete connection</button></div>`}`);
  }

  onInput(e, committed = false) {
    const t = e.target;
    const sel = this.editor.sel;
    if (!sel) return;
    if (t.matches('[data-link]')) {
      if (committed && t.value) {
        this.editor.connect(sel.id, t.value, { keepSelection: true });
        this.el.querySelector('[data-link]')?.focus();
      }
      return;
    }
    if (t.matches('[data-label]')) {
      this.editor.setLabel(sel.id, t.value);
      return;
    }
    if (t.dataset.key && sel.kind === 'node') {
      let value;
      if (t.type === 'checkbox') value = t.checked;
      else if (t.type === 'number') {
        if (t.value === '') return;
        value = Number(t.value);
        if (!Number.isFinite(value)) return;
        if (t.min !== '' && value < Number(t.min)) value = Number(t.min);
        if (t.max !== '' && value > Number(t.max)) value = Number(t.max);
        if (committed) t.value = String(value);
      } else value = t.value;
      this.editor.setParam(sel.id, t.dataset.key, value);
      return;
    }
    if (t.dataset.edge && sel.kind === 'edge') {
      const key = t.dataset.edge;
      let value;
      if (key === 'optional') value = t.checked;
      else if (key === 'kind') value = t.value;
      else value = t.value === '' ? null : Math.max(0, Math.min(1, Number(t.value)));
      if (value !== null && typeof value === 'number' && !Number.isFinite(value)) return;
      this.editor.updateEdge(sel.id, { [key]: value });
    }
  }

  onClick(e) {
    const act = e.target.closest('[data-act]')?.dataset.act;
    if (act === 'del') this.editor.removeSelected();
    if (act === 'dup') this.editor.duplicate();
    const unlink = e.target.closest('[data-unlink]')?.dataset.unlink;
    if (unlink) this.editor.removeEdge(unlink);
    const go = e.target.closest('[data-goto]')?.dataset.goto;
    if (go) this.editor.select({ kind: 'edge', id: go });
  }
}

// ================================================================ workspace: results
const STATUS_WORD = { ok: 'Healthy', warm: 'Busy', hot: 'Very busy', overloaded: 'Overloaded', down: 'Down', idle: 'No traffic' };
const SCENARIO_WORD = { normal: 'Normal', spike: 'Spike', failure: 'Failure', kafka: 'Kafka' };

function kafkaMeta(k) {
  if (!k) return '';
  const parts = [];
  if (k.lag_rps >= 0.5) parts.push(`lag +${fmtNum(k.lag_rps)}/s`);
  parts.push(k.freshness_ms === null ? 'data never fresh' : `data ${fmtDuration(k.freshness_ms / 1000)} old`);
  return ` · ${parts.join(' · ')}`;
}

const requirementsHtml = (reqs = []) => (reqs.length
  ? html`<div class="req-chips">${reqs.map((r) => html`<span class="req-chip ${r.met ? 'met' : 'broken'}" title="${r.met ? 'Kept in this test' : 'Broken in this test'}">${r.met ? '✓' : '✗'} ${r.label}</span>`)}</div>`
  : '');

const scoreClass = (s) => (s >= 75 ? 'good' : s >= 60 ? 'warn' : 'bad');

function scoreBanner(score, title, text) {
  return html`<div class="score-banner"><div class="big ${scoreClass(score)}">${score}</div><div><strong>${title}</strong><p>${text}</p></div></div>`;
}

function metricsGrid(m) {
  return html`<div class="stat-grid">
    <div class="stat-box"><span>Errors</span><strong class="${m.error_rate > 0.05 ? 'bad' : m.error_rate > 0.01 ? 'warn' : 'good'}">${fmtPct(m.error_rate, 1)}</strong></div>
    <div class="stat-box"><span>Slow requests (p95)</span><strong>${fmtMs(m.latency_ms.p95)}</strong></div>
    <div class="stat-box"><span>Served</span><strong>${fmtNum(m.throughput_rps)}/s</strong></div>
    <div class="stat-box"><span>Busiest part</span><strong class="${m.max_utilization > 1 ? 'bad' : m.max_utilization >= 0.85 ? 'warn' : ''}">${fmtLoad(m.max_utilization)}</strong></div>
  </div>`;
}

function testsHtml(tests, { solvable = false } = {}) {
  return tests.map((t, i) => html`<div class="test-row ${t.pass ? 'pass' : 'fail'}">
    <div class="test-top"><strong>${t.name}</strong><span class="${t.pass ? 'chip-pass' : 'chip-fail'}">${t.pass ? 'PASS' : 'FAIL'} · ${t.score}</span></div>
    <div class="test-meta">${t.tier === 'system' ? 'System test' : 'Basic test'} · ${SCENARIO_WORD[t.scenario] || t.scenario} · ${fmtPct(t.metrics.error_rate, 1)} errors · p95 ${fmtMs(t.metrics.latency_ms.p95)}${kafkaMeta(t.metrics.kafka)}</div>
    ${requirementsHtml(t.requires)}
    <p class="test-note">${t.note}${t.bottlenecks?.length ? ` Busiest: ${t.bottlenecks.slice(0, 2).join(', ')}.` : ''}</p>
    ${t.violations?.length ? html`<ul class="test-violations">${t.violations.map((v) => html`<li>${v.text}</li>`)}</ul>` : ''}
    <div class="test-links"><button type="button" class="link-btn" data-show="${i}">Show on canvas</button>${solvable && !t.pass ? html`<button type="button" class="link-btn" data-solve="${i}" aria-expanded="false">Show the solution</button>` : ''}</div>
    <div data-solution="${i}"></div>
  </div>`);
}

function fixesHtml(fixes) {
  return html`<ol class="sol-fixes">${fixes.map((f) => html`<li><strong>${f.label}</strong>
    <ul>${changeLines(f).map((line) => html`<li>${line}</li>`)}</ul>
    ${f.reason ? html`<small>Why: ${f.reason}</small>` : ''}</li>`)}</ol>`;
}

// A worked solution for one failing test: why it fails, what to change, and the result.
function solutionHtml(sol, index) {
  return html`<div class="solution">
    <p class="sol-title">Why it fails</p>
    <ul class="sol-why">${sol.why.map((w) => html`<li>${w}</li>`)}</ul>
    ${sol.fixes.length ? html`<p class="sol-title">What to change</p>${fixesHtml(sol.fixes)}` : html`<p class="muted small">No change of settings fixes this on its own.</p>`}
    <p class="sol-result ${sol.after.pass ? 'good' : 'warn'}">${sol.after.pass
      ? `With these changes this test scores ${sol.after.score} and passes.`
      : `These changes raise the score from ${sol.before.score} to ${sol.after.score}, still short of a pass (60).`}</p>
    ${sol.advice.length ? html`<ul class="sol-advice">${sol.advice.map((a) => html`<li>${a}</li>`)}</ul>` : ''}
    ${sol.fixes.length ? html`<div class="insp-actions"><button type="button" class="btn btn-sm btn-primary" data-apply="${index}">Apply to my design</button><span class="muted small">You can undo it.</span></div>` : ''}
  </div>`;
}

// One set of changes that makes every failing test pass, where possible.
function suiteSolutionHtml(sol) {
  const all = sol.after.passed === sol.after.total;
  return html`<div class="solution">
    <p class="sol-title">${all ? 'Changes that make every test pass' : `Changes that make ${sol.after.passed} of ${sol.after.total} tests pass`}</p>
    ${sol.fixes.length ? fixesHtml(sol.fixes) : html`<p class="muted small">No change of settings helps here.</p>`}
    <p class="sol-result ${all ? 'good' : 'warn'}">Tests passing: ${sol.before.passed} → ${sol.after.passed} of ${sol.after.total}.</p>
    ${sol.advice.length ? html`<ul class="sol-advice">${sol.advice.map((a) => html`<li>${a}</li>`)}</ul>` : ''}
    ${sol.fixes.length ? html`<div class="insp-actions"><button type="button" class="btn btn-sm btn-primary" data-apply-all>Apply all to my design</button><span class="muted small">You can undo it.</span></div>` : ''}
  </div>`;
}

const SEV = { critical: 'critical', warning: 'warning', info: 'info' };

function findingsHtml(rules, { limit = 12 } = {}) {
  const design = rules.findings.filter((f) => f.category !== 'security');
  const security = rules.findings.filter((f) => f.category === 'security');
  const card = (f) => html`<div class="finding" data-ids="${(f.node_ids || []).join(',')}">
    <div class="f-top"><span class="sev sev-${SEV[f.severity]}">${f.severity}</span><span class="f-title">${f.title}</span></div>
    <p>${f.reason}</p>
    <p class="fix">→ ${f.recommendation}</p>
  </div>`;
  return html`
    <h3>Design review <span class="muted small">score ${rules.score.total} (${rules.score.grade})</span></h3>
    ${design.length ? design.slice(0, limit).map(card) : html`<p class="muted small">No design problems found.</p>`}
    ${design.length > limit ? html`<p class="muted small">…and ${design.length - limit} more.</p>` : ''}
    <h3>Security <span class="muted small">${rules.security.passed} of ${rules.security.total} checks pass</span></h3>
    ${security.length ? security.map(card) : html`<p class="muted small">All security checks pass.</p>`}
    ${rules.strengths.length ? html`<h3>What is working</h3><ul class="checks">${rules.strengths.map((s) => html`<li>${s}</li>`)}</ul>` : ''}`;
}

function nodeTableHtml(sim) {
  const rows = sim.node_metrics.filter((n) => n.status !== 'source').sort((a, b) => b.utilization - a.utilization);
  return html`<h3>Every component</h3>
    <table class="node-table"><thead><tr><th>Component</th><th>In /s</th><th>Busy</th><th>State</th></tr></thead>
    <tbody>${rows.map((n) => html`<tr><td>${n.label}</td><td>${fmtNum(n.demand_rps)}</td><td>${fmtLoad(n.utilization)}</td><td class="${n.status === 'overloaded' || n.status === 'down' ? 'bad' : n.status === 'hot' || n.status === 'warm' ? 'warn' : 'good'}">${STATUS_WORD[n.status] || n.status}</td></tr>`)}</tbody></table>`;
}

// Practice and learn test results. With a `solver`, failing tests get worked solutions:
// solver.solve(testId), solver.solveAll() and solver.apply(graph, message).
function renderSuite(el, data, { title, intro = '', onShow, onFinding, solver = null }) {
  const { summary, tests, rules, simulation, submission } = data;
  const banner = submission
    ? scoreBanner(submission.total, `Score ${submission.total} · grade ${submission.grade}`, `Tests ${submission.parts.tests}, design ${submission.parts.design}, security ${submission.parts.security}. ${summary.passed} of ${summary.total} tests passed.`)
    : scoreBanner(summary.score, title || `${summary.passed} of ${summary.total} tests passed`, intro || 'A test passes at 60 or more.');
  const failing = tests.filter((t) => !t.pass).length;
  setHtml(el, html`${banner}
    ${solver && failing ? html`<div class="section-card solve-all"><div class="solve-all-top"><span><strong>Stuck?</strong> Get one set of changes that fixes ${failing === 1 ? 'the failing test' : `all ${failing} failing tests`}.</span><button type="button" class="btn btn-sm" data-solve-all>Show the solution</button></div><div data-solution-all></div></div>` : ''}
    <h3>At normal traffic</h3>
    ${metricsGrid(simulation)}
    <h3>Tests</h3>
    ${testsHtml(tests, { solvable: !!solver })}
    ${rules ? findingsHtml(rules) : ''}
    ${nodeTableHtml(simulation)}`);
  bindResults(el, { tests, onShow, onFinding });
  if (solver) bindSolutions(el, tests, solver);
}

// The results panel outlives each render, so the previous render's click handler is replaced.
const solutionHandlers = new WeakMap();

function bindSolutions(el, tests, solver) {
  const solved = new Map();
  let all = null;
  el.removeEventListener('click', solutionHandlers.get(el));
  const onClick = (e) => {
    const solveBtn = e.target.closest('[data-solve]');
    if (solveBtn) {
      const i = Number(solveBtn.dataset.solve);
      const slot = el.querySelector(`[data-solution="${i}"]`);
      const open = solveBtn.getAttribute('aria-expanded') === 'true';
      if (open) {
        slot.innerHTML = '';
      } else {
        if (!solved.has(i)) solved.set(i, solver.solve(tests[i].id));
        setHtml(slot, solutionHtml(solved.get(i), i));
      }
      solveBtn.setAttribute('aria-expanded', String(!open));
      solveBtn.textContent = open ? 'Show the solution' : 'Hide the solution';
      return;
    }
    if (e.target.closest('[data-solve-all]')) {
      const slot = el.querySelector('[data-solution-all]');
      if (slot.innerHTML) {
        slot.innerHTML = '';
        return;
      }
      all ??= solver.solveAll();
      setHtml(slot, suiteSolutionHtml(all));
      return;
    }
    const apply = e.target.closest('[data-apply]');
    if (apply) {
      const sol = solved.get(Number(apply.dataset.apply));
      if (sol) solver.apply(sol.graph, `Applied the solution for “${sol.name}”. The tests run again.`);
      return;
    }
    if (e.target.closest('[data-apply-all]') && all) solver.apply(all.graph, 'Applied all the changes. The tests run again.');
  };
  solutionHandlers.set(el, onClick);
  el.addEventListener('click', onClick);
}

function renderSimulation(el, data, { onFinding }) {
  const { simulation, score, rules, scenario } = data;
  setHtml(el, html`${scoreBanner(score.total, scenario || 'Simulation at normal traffic', `Availability ${score.parts.availability}, speed ${score.parts.latency}, headroom ${score.parts.headroom}. 60 or more is a pass.`)}
    ${metricsGrid(simulation)}
    ${simulation.latency_hops.length ? html`<h3>Where the time goes</h3><ul class="list-plain">${simulation.latency_hops.map((h) => html`<li>${h.label}: ${fmtMs(h.latency_ms)}</li>`)}</ul>` : ''}
    ${findingsHtml(rules)}
    ${nodeTableHtml(simulation)}`);
  bindResults(el, { tests: [], onFinding });
}

function renderStress(el, data, { cursor = null, onPlay, onShowTest, onFinding }) {
  const { verdict, ramp, targets, tests, rules } = data;
  const cls = verdict.status === 'pass' ? 'chip-pass' : verdict.status === 'warn' ? 'chip-warn' : 'chip-fail';
  setHtml(el, html`
    <div class="score-banner"><span class="${cls}">${verdict.status === 'pass' ? 'HOLDS UP' : verdict.status === 'warn' ? 'SHAKY' : 'BREAKS EARLY'}</span><div><strong>Load ramp</strong><p>${verdict.headline}</p></div></div>
    <div class="row" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
      <h3 style="margin:0">Traffic from 0.5× to 10×</h3>
      <button type="button" class="btn btn-sm" data-play>${cursor === null ? '▶ Play on canvas' : '↺ Replay'}</button>
    </div>
    ${rampChart({ points: ramp, targetMs: targets.latency_ms, cursor })}
    ${tests.length ? html`<h3>Focus tests</h3>${tests.map((t, i) => html`<div class="test-row ${t.status === 'pass' ? 'pass' : t.status === 'skipped' ? '' : 'fail'}">
      <div class="test-top"><strong>${t.name}</strong><span class="${t.status === 'pass' ? 'chip-pass' : t.status === 'warn' ? 'chip-warn' : t.status === 'skipped' ? 'chip-skip' : 'chip-fail'}">${t.status.toUpperCase()}</span></div>
      ${t.description ? html`<div class="test-meta">${t.description}</div>` : ''}
      <p class="test-note">${t.summary}</p>
      ${t.advice ? html`<p class="test-note">→ ${t.advice}</p>` : ''}
      ${t.load ? html`<button type="button" class="link-btn" data-test="${i}">Show on canvas</button>` : ''}
    </div>`)}` : html`<p class="muted small">Pick focus tests on the left to try specific failures.</p>`}
    ${rules ? findingsHtml(rules) : ''}`);
  el.querySelector('[data-play]')?.addEventListener('click', () => onPlay?.());
  el.querySelectorAll('[data-test]').forEach((b) => b.addEventListener('click', () => onShowTest?.(tests[Number(b.dataset.test)])));
  bindResults(el, { tests: [], onFinding });
}

function bindResults(el, { tests, onShow, onFinding }) {
  el.querySelectorAll('[data-show]').forEach((b) => b.addEventListener('click', () => onShow?.(tests[Number(b.dataset.show)])));
  el.querySelectorAll('.finding[data-ids]').forEach((card) => card.addEventListener('click', () => {
    const ids = card.dataset.ids.split(',').filter(Boolean);
    if (ids.length) onFinding?.(ids);
  }));
}

// ================================================================ workspace: run
const TICK_MS = 250;
const SPEEDS = [1, 2, 5, 10];
const CHART_SECONDS = 120;
const REPLICA_TYPES = new Set(['LoadBalancer', 'ReverseProxy', 'RateLimiter', 'APIGateway', 'Server', 'Service', 'WebSocketServer', 'Cache', 'Worker', ...KAFKA_CONSUMERS, 'SchemaRegistry']);
const FAILURE_WORD = { down: 'down', 'lose-one': 'one copy lost', slow: 'slower than normal', poison: 'stuck on a poison message', schema: 'breaking schema deployed' };
const STATE_WORD = { idle: 'Ready', live: 'Live', paused: 'Paused', stopped: 'Stopped' };

function failureWord(node, f) {
  if (f.kind === 'slow') return `${f.mult}× slower`;
  if (f.kind === 'lose-one' && node?.type === 'KafkaTopic') return 'one broker lost';
  if (f.kind === 'lose-one' && KAFKA_CONSUMERS.has(node?.type)) return 'one consumer crashed';
  return FAILURE_WORD[f.kind];
}

const levelFromSlider = (pos) => 0.1 * 100 ** (pos / 100);
const sliderFromLevel = (level) => Math.round(50 * Math.log10(level * 10));
const countWord = (n, one, many) => `${fmtNum(n)} ${Number(n) === 1 ? one : many}`;

// Replaces an element's content only when it changed, so a button under the pointer
// survives the frequent updates of a running simulation.
const lastHtml = new WeakMap();
function patch(el, content) {
  const next = String(content);
  if (lastHtml.get(el) === next) return;
  lastHtml.set(el, next);
  el.innerHTML = next;
}

// The one setting the + and − buttons change for each kind of part.
function knobFor(node) {
  if (isAutoscaled(node)) return { key: 'autoscale_max', min: Math.max(1, Number(param(node, 'autoscale_min')) || 1), max: 500, step: (v) => Math.max(1, Math.round(v * 0.25)), word: 'autoscale maximum' };
  const word = node.type === 'Cache' ? 'nodes' : node.type === 'ConsumerGroup' ? 'consumers' : KAFKA_CONSUMERS.has(node.type) ? 'instances' : 'copies';
  if (REPLICA_TYPES.has(node.type)) return { key: 'replicas', min: 1, max: 200, step: () => 1, word };
  if (node.type === 'Database' || node.type === 'SearchIndex') return { key: 'shards', min: 1, max: 64, step: () => 1, word: 'shards' };
  if (node.type === 'Queue') return { key: 'max_depth', min: 1000, max: 100_000_000, double: true, word: 'room' };
  if (node.type === 'KafkaTopic') {
    return { key: 'partitions', min: 1, max: 2000, step: (v) => Math.max(1, Math.round(v * 0.5)), word: 'partitions', growOnly: 'Kafka can add partitions to a topic but never remove them. Recreate the topic to have fewer.' };
  }
  return null;
}

// Moving dots for each arrow that carries traffic; red dots are requests that get dropped.
function particlesFrom(sim, graph) {
  const metrics = new Map(sim.node_metrics.map((m) => [m.id, m]));
  const types = new Map(graph.nodes.map((n) => [n.id, n.type]));
  const kinds = new Map(graph.edges.map((e) => [e.id, e.kind]));
  return sim.edge_flows.filter((f) => f.rps > 0.5).map((f) => {
    const target = metrics.get(f.target);
    const dots = Math.min(6, Math.max(1, Math.round(Math.log10(f.rps))));
    const waits = ['Queue', 'KafkaTopic'].includes(types.get(f.source));
    const drop = target?.status === 'down' ? 1 : target?.drop_rate || 0;
    const bad = !waits && drop > 0.02 ? Math.min(dots, Math.max(1, Math.round(dots * drop))) : 0;
    return { id: f.id, dots, bad, async: kinds.get(f.id) === 'async' || ['Queue', 'KafkaTopic'].includes(types.get(f.target)) || waits };
  });
}

class RunPanel {
  constructor(ctx, { panel, host }) {
    this.ctx = ctx;
    this.panel = panel;
    this.state = 'idle';
    this.holding = false;
    this.speed = 2;
    this.botsTouched = false;
    this.info = { latencyTarget: 300, spikes: { regular: 3, mega: 8 } };
    this.run = this.freshRun();
    this.target = null;
    this.shownEvent = 0;
    this.hud = document.createElement('button');
    this.hud.type = 'button';
    this.hud.className = 'cv-hud hidden';
    this.hud.title = 'Open the run controls';
    this.hud.addEventListener('click', () => ctx.showTab('run'));
    host.appendChild(this.hud);
    this.renderShell();
    this.bind();
    this.refreshDesign();
  }

  freshRun(prev = null) {
    const run = new LiveRun({ traffic: this.ctx.traffic(), overrides: this.ctx.overrides(), latencyTarget: this.info.latencyTarget, spikes: this.info.spikes });
    if (prev) {
      run.level = prev.level;
      run.abusive = prev.abusive;
      run.eventSeq = prev.eventSeq;
    }
    return run;
  }

  get active() {
    return this.state === 'live' || this.state === 'paused';
  }

  // ---------------------------------------------------------------- settings from the mode
  setInfo({ latencyTarget, spikes } = {}) {
    if (latencyTarget) this.info.latencyTarget = latencyTarget;
    if (spikes) this.info.spikes = { ...this.info.spikes, ...spikes };
    this.run.latencyTarget = this.info.latencyTarget;
    this.run.spikes = { ...this.info.spikes };
    this.renderTrafficButtons();
    this.renderStats(this.run.last);
  }

  syncTraffic() {
    this.run.traffic = this.ctx.traffic();
    this.run.baseOverrides = this.ctx.overrides();
    if (this.state === 'idle' && !this.botsTouched) this.run.abusive = Number(this.run.traffic.abusive_share) || 0;
    this.renderLevel();
    this.renderBots();
  }

  // ---------------------------------------------------------------- playback
  start() {
    const graph = this.ctx.graph();
    if (!graph.nodes.some((n) => n.type === 'User')) return toast('Add a Users box first: traffic starts there.', 'warn');
    if (!graph.edges.length) return toast('Connect your boxes with arrows first.', 'warn');
    if (this.state === 'stopped') this.reset();
    this.syncTraffic();
    this.state = 'live';
    patch(this.el.report, '');
    this.hold(true);
    clearInterval(this.timer);
    this.timer = setInterval(() => this.tick(), TICK_MS);
    this.tick();
    this.renderActions();
    this.ctx.setRunState('live');
    this.ctx.showTab('run', { open: false });
  }

  pause() {
    if (this.state !== 'live') return;
    clearInterval(this.timer);
    this.timer = null;
    this.state = 'paused';
    this.ctx.editor.setParticles(null);
    this.renderActions();
    this.paintHud();
    this.ctx.setRunState('paused');
  }

  stop() {
    if (!this.active) return;
    clearInterval(this.timer);
    this.timer = null;
    this.state = 'stopped';
    this.renderActions();
    this.renderReport();
    this.hold(false);
    this.ctx.setRunState('idle');
    this.ctx.showTab('run', { open: false });
  }

  reset() {
    clearInterval(this.timer);
    this.timer = null;
    const wasHolding = this.holding;
    this.run = this.freshRun(this.run);
    this.state = 'idle';
    patch(this.el.report, '');
    this.renderActions();
    this.renderLog(true);
    this.renderStats(null);
    this.renderCharts();
    this.renderProblems();
    this.renderPattern();
    this.paintParts(null);
    this.el.clock.textContent = clock(0);
    if (wasHolding) this.hold(false);
    this.ctx.setRunState('idle');
  }

  // Another view wants the canvas (for example a test result): pause and hand it over.
  yieldCanvas() {
    if (this.state === 'live') this.pause();
    if (this.holding) this.hold(false);
  }

  // Called when the canvas goes back to the live view while a run owns it.
  repaint() {
    if (this.run.last) this.ctx.editor.setOverlay(this.run.last);
  }

  hold(on) {
    this.holding = on;
    this.hud.classList.toggle('hidden', !on);
    this.ctx.holdOverlay(on);
    if (on && this.run.last) this.repaint();
  }

  setSpeed(speed) {
    this.speed = speed;
    this.el.speed.querySelectorAll('[data-speed]').forEach((b) => {
      const on = Number(b.dataset.speed) === speed;
      b.classList.toggle('active', on);
      b.setAttribute('aria-pressed', String(on));
    });
  }

  tick() {
    const graph = this.ctx.graph();
    if (!graph.nodes.some((n) => n.type === 'User')) {
      this.pause();
      toast('The Users box is gone, so the run is paused.', 'warn');
      return;
    }
    this.run.traffic = this.ctx.traffic();
    this.run.baseOverrides = this.ctx.overrides();
    let sim;
    try {
      sim = this.run.step(graph, (TICK_MS / 1000) * this.speed);
    } catch (err) {
      console.error(err);
      this.pause();
      toast('The simulation hit a problem and paused.', 'bad');
      return;
    }
    if (this.holding) {
      this.ctx.editor.setOverlay(sim);
      this.ctx.editor.setParticles(particlesFrom(sim, graph));
    }
    this.paintAll(sim);
  }

  // While paused, edits show their effect at the current traffic without moving time.
  peek() {
    if (this.state !== 'paused' || !this.holding) return;
    try {
      const sim = this.run.peek(this.ctx.graph());
      this.ctx.editor.setOverlay(sim);
      this.renderStats(sim);
      this.paintParts(sim);
      this.paintHud(sim);
    } catch (err) {
      console.error(err);
    }
  }

  paintAll(sim) {
    this.el.clock.textContent = clock(this.run.t);
    this.paintHud(sim);
    this.renderStats(sim);
    this.renderCharts();
    this.renderProblems();
    this.renderBreak();
    this.paintParts(sim);
    this.renderLog();
  }

  // After a button press: show its effect now instead of waiting for the next tick.
  afterControl() {
    this.renderProblems();
    this.renderBreak();
    this.renderLog();
    this.renderPattern();
    this.renderLevel();
    if (this.state === 'paused') this.peek();
  }

  // ---------------------------------------------------------------- layout
  renderShell() {
    setHtml(this.panel, html`<div class="run">
      <p class="panel-intro">Send live traffic through your design. Change the traffic, break parts and add capacity while it runs.</p>
      <div class="run-head">
        <span class="run-state" data-el="state" data-state="idle">Ready</span>
        <span class="run-clock" data-el="clock" title="Simulated time">0:00</span>
        <span class="grow"></span>
        <span class="muted small">Speed</span>
        <div class="seg" role="group" aria-label="Simulation speed" data-el="speed">${SPEEDS.map((s) => html`<button type="button" data-speed="${s}" class="${s === this.speed ? 'active' : ''}" aria-pressed="${s === this.speed}">${s}×</button>`)}</div>
      </div>
      <div class="run-actions" data-el="actions"></div>
      <div data-el="report"></div>
      <div class="stat-grid run-stats" data-el="stats">${['arriving', 'served', 'errors', 'p95', 'busiest', 'queued'].map((key) => html`<div class="stat-box"><span data-label="${key}">${{ arriving: 'Arriving', served: 'Served', errors: 'Failing', p95: 'Slowest 5% (p95)', busiest: 'Busiest part', queued: 'Waiting in queues' }[key]}</span><strong data-stat="${key}">–</strong><small data-sub="${key}"></small></div>`)}</div>
      <div class="run-charts" data-el="charts"></div>
      <details class="run-sec" open>
        <summary>Traffic</summary>
        <div class="run-body">
          <label class="run-slider"><span>Traffic level</span><output data-el="levelOut"></output>
            <input type="range" min="0" max="100" step="1" value="${sliderFromLevel(this.run.level)}" data-el="level" aria-label="Traffic level"></label>
          <div class="run-btns" data-el="trafficBtns"></div>
          <div class="seg seg-wide" role="group" aria-label="Traffic pattern" data-el="pattern">
            <button type="button" data-pattern="steady">Steady</button>
            <button type="button" data-pattern="ramp" title="From half to ten times normal over two minutes">Ramp up</button>
            <button type="button" data-pattern="wave" title="Quiet at night, busiest at midday, every three minutes">Day and night</button>
          </div>
          <label class="run-slider"><span>Bots and abusive clients</span><output data-el="botsOut"></output>
            <input type="range" min="0" max="90" step="5" value="0" data-el="bots" aria-label="Share of bot traffic"></label>
        </div>
      </details>
      <details class="run-sec" open>
        <summary>Break something</summary>
        <div class="run-body">
          <p class="muted small">Pick a part, or click a box on the canvas, then choose what goes wrong.</p>
          <select class="input" data-el="target" aria-label="Part to break"></select>
          <div class="run-btns" data-el="breakBtns"></div>
          <div class="run-btns"><button type="button" class="btn btn-sm btn-ghost" data-act="random">Break something at random</button></div>
          <div data-el="problems"></div>
          <div data-el="recovering"></div>
        </div>
      </details>
      <details class="run-sec" open>
        <summary>Parts and capacity</summary>
        <div class="run-body">
          <p class="muted small">Add or remove capacity while traffic flows. Changes apply straight away and can be undone.</p>
          <div class="run-parts" data-el="parts"></div>
        </div>
      </details>
      <details class="run-sec" open>
        <summary>What happened</summary>
        <ol class="run-log" data-el="log"></ol>
      </details>
    </div>`);
    this.el = Object.fromEntries([...this.panel.querySelectorAll('[data-el]')].map((x) => [x.dataset.el, x]));
    this.renderActions();
    this.renderTrafficButtons();
    this.renderPattern();
    this.renderLevel();
    this.renderBots();
    this.renderStats(null);
    this.renderCharts();
    this.renderProblems();
    this.renderLog(true);
  }

  bind() {
    this.panel.addEventListener('click', (e) => {
      const speed = e.target.closest('[data-speed]');
      if (speed) return this.setSpeed(Number(speed.dataset.speed));
      const pattern = e.target.closest('[data-pattern]');
      if (pattern) {
        this.run.setPattern(pattern.dataset.pattern);
        return this.afterControl();
      }
      const traffic = e.target.closest('[data-traffic]')?.dataset.traffic;
      if (traffic) {
        if (traffic === 'normal') this.run.calm();
        else this.run.spike(traffic === 'mega' ? 'mega' : 'regular');
        this.el.level.value = String(sliderFromLevel(this.run.level));
        return this.afterControl();
      }
      const kind = e.target.closest('[data-break]')?.dataset.break;
      if (kind) return this.breakTarget(kind);
      const fix = e.target.closest('[data-fix]')?.dataset.fix;
      if (fix) {
        this.run.fix(fix, this.ctx.graph());
        return this.afterControl();
      }
      const knob = e.target.closest('[data-knob]');
      if (knob) return this.turnKnob(knob.dataset.id, Number(knob.dataset.knob));
      const open = e.target.closest('[data-open]')?.dataset.open;
      if (open) {
        this.ctx.editor.select({ kind: 'node', id: open });
        this.ctx.editor.flash([open]);
        return this.ctx.showTab('inspect');
      }
      const act = e.target.closest('[data-act]')?.dataset.act;
      if (act === 'start' || act === 'resume') this.start();
      else if (act === 'pause') this.pause();
      else if (act === 'stop') this.stop();
      else if (act === 'reset') this.reset();
      else if (act === 'again') {
        this.reset();
        this.start();
      } else if (act === 'random') {
        if (!this.run.randomFailure(this.ctx.graph())) toast('There is nothing left to break.', 'warn');
        this.afterControl();
      } else if (act === 'fix-all') {
        this.run.fixAll(this.ctx.graph());
        this.afterControl();
      }
    });
    this.el.level.addEventListener('input', () => {
      this.run.level = levelFromSlider(Number(this.el.level.value));
      this.renderLevel();
    });
    this.el.level.addEventListener('change', () => {
      this.run.setLevel(levelFromSlider(Number(this.el.level.value)), this.ctx.graph());
      this.afterControl();
    });
    this.el.bots.addEventListener('input', () => {
      this.botsTouched = true;
      this.run.abusive = Number(this.el.bots.value) / 100;
      this.renderBots();
    });
    this.el.bots.addEventListener('change', () => {
      this.run.setAbusive(Number(this.el.bots.value) / 100);
      this.afterControl();
    });
    this.el.target.addEventListener('change', () => {
      this.target = this.el.target.value || null;
      this.renderBreak();
    });
    this.ctx.editor.addEventListener('select', (e) => {
      const sel = e.detail.sel;
      if (sel?.kind !== 'node' || this.ctx.editor.node(sel.id)?.type === 'User') return;
      this.target = sel.id;
      this.renderTargets();
      this.renderBreak();
    });
    this.ctx.editor.addEventListener('change', (e) => {
      this.refreshDesign();
      if (e.detail.reason === 'load' && this.active) this.run.log('info', 'A different design was loaded.');
      if (this.state === 'paused') this.peek();
    });
  }

  renderActions() {
    const buttons = {
      idle: [['start', '▶ Start', true]],
      live: [['pause', '❚❚ Pause', true], ['stop', '■ Stop']],
      paused: [['resume', '▶ Resume', true], ['stop', '■ Stop'], ['reset', '↺ Start over']],
      stopped: [['again', '▶ Run again', true], ['reset', '↺ Clear']],
    }[this.state];
    patch(this.el.actions, html`${buttons.map(([act, label, primary]) => html`<button type="button" class="btn btn-sm ${primary ? 'btn-success' : ''}" data-act="${act}">${label}</button>`)}`);
    this.el.state.dataset.state = this.state;
    this.el.state.textContent = STATE_WORD[this.state];
    this.hud.dataset.state = this.state;
  }

  renderTrafficButtons() {
    const { regular, mega } = this.info.spikes;
    patch(this.el.trafficBtns, html`<button type="button" class="btn btn-sm" data-traffic="normal">Back to normal</button>
      <button type="button" class="btn btn-sm" data-traffic="spike" title="For 30 seconds">Spike ${regular}×</button>
      <button type="button" class="btn btn-sm" data-traffic="mega" title="For 30 seconds">Mega spike ${mega}×</button>`);
  }

  renderPattern() {
    this.el.pattern.querySelectorAll('[data-pattern]').forEach((b) => {
      const on = b.dataset.pattern === this.run.pattern;
      b.classList.toggle('active', on);
      b.setAttribute('aria-pressed', String(on));
    });
  }

  renderLevel() {
    const normal = this.run.normalRps(this.ctx.graph());
    this.el.levelOut.textContent = `${times(this.run.level)} · ${fmtNum(normal * this.run.level)}/s`;
    if (document.activeElement !== this.el.level) this.el.level.value = String(sliderFromLevel(this.run.level));
  }

  renderBots() {
    this.el.botsOut.textContent = `${Math.round(this.run.abusive * 100)}%`;
    if (document.activeElement !== this.el.bots) this.el.bots.value = String(Math.round(this.run.abusive * 100));
  }

  renderStats(sim) {
    const set = (key, value, sub = '', tone = '') => {
      const strong = this.el.stats.querySelector(`[data-stat="${key}"]`);
      strong.textContent = value;
      strong.className = tone;
      this.el.stats.querySelector(`[data-sub="${key}"]`).textContent = sub;
    };
    if (!sim) {
      for (const key of ['arriving', 'served', 'errors', 'p95', 'busiest', 'queued']) set(key, '–');
      return;
    }
    const run = this.run;
    set('arriving', `${fmtNum(sim.incoming_rps)}/s`, `${times(run.multiplier())} normal`);
    set('served', `${fmtNum(sim.throughput_rps)}/s`, sim.blocked_rps > 1 ? `${fmtNum(sim.blocked_rps)}/s bots blocked` : '');
    const failing = Math.max(0, sim.legit_rps - sim.throughput_rps);
    set('errors', fmtPct(sim.error_rate, 1), failing >= 1 ? `${fmtNum(failing)}/s fail` : 'none', sim.error_rate > 0.05 ? 'bad' : sim.error_rate > 0.01 ? 'warn' : 'good');
    const p95Tone = sim.error_rate > 0.05 ? '' : sim.latency_ms.p95 > run.latencyTarget ? 'warn' : 'good';
    set('p95', fmtMs(sim.latency_ms.p95), sim.error_rate > 0.05 ? 'failures return fast' : `target ${run.latencyTarget} ms`, p95Tone);
    const busiest = sim.node_metrics.filter((m) => m.status !== 'source').sort((a, b) => b.utilization - a.utilization)[0];
    set('busiest', busiest ? (busiest.status === 'down' ? 'Down' : fmtLoad(busiest.utilization)) : '–', busiest?.label || '', busiest && (busiest.utilization > 1 || busiest.status === 'down') ? 'bad' : busiest?.utilization >= 0.85 ? 'warn' : '');
    const waiting = [...run.depth.values()].reduce((s, d) => s + d, 0);
    const label = this.el.stats.querySelector('[data-label="queued"]');
    if (sim.kafka) {
      const lag = [...run.lag.values()].reduce((s, d) => s + d, 0);
      const age = Math.max(0, ...run.age.values());
      label.textContent = run.depth.size ? 'Waiting (queues and lag)' : 'Consumer lag';
      set('queued', fmtNum(waiting + lag), lag >= 1 ? `messages · ${fmtDuration(age)} behind` : 'messages · caught up', lag >= 1 || waiting >= 1 ? 'warn' : 'good');
    } else {
      label.textContent = 'Waiting in queues';
      set('queued', run.depth.size ? fmtNum(waiting) : '–', run.depth.size ? 'messages' : 'no queues', waiting >= 1 ? 'warn' : '');
    }
  }

  renderCharts() {
    const h = this.run.history;
    if (h.length < 2) return patch(this.el.charts, html`<p class="muted small">Charts appear once the run starts.</p>`);
    setHtml(this.el.charts, liveCharts(h, { targetMs: this.run.latencyTarget, window: CHART_SECONDS, kafka: h.at(-1).lag !== undefined }));
  }

  paintHud(sim = this.run.last) {
    if (!sim) return;
    const tone = sim.error_rate > 0.05 ? 'bad' : sim.error_rate > 0.01 || sim.latency_ms.p95 > this.run.latencyTarget ? 'warn' : 'good';
    this.hud.dataset.tone = tone;
    this.hud.dataset.state = this.state;
    setHtml(this.hud, html`<span class="dot" aria-hidden="true"></span><strong>${this.state === 'paused' ? 'Paused' : 'Live'} ${clock(this.run.t)}</strong>
      <span>${fmtNum(sim.incoming_rps)}/s · ${times(this.run.multiplier())}</span>
      <span class="${tone}">${fmtPct(sim.success_rate, sim.success_rate < 0.995 ? 1 : 0)} served</span>
      <span>p95 ${fmtMs(sim.latency_ms.p95)}</span>`);
  }

  // ---------------------------------------------------------------- breaking parts
  breakables() {
    return this.ctx.editor.nodes.filter((n) => n.type !== 'User');
  }

  renderTargets() {
    const nodes = this.breakables();
    if (this.target && !nodes.some((n) => n.id === this.target)) this.target = null;
    patch(this.el.target, html`<option value="">Pick a part…</option>${nodes.map((n) => html`<option value="${n.id}" ${n.id === this.target ? 'selected' : ''}>${n.label} (${COMPONENTS[n.type].label})</option>`)}`);
    this.el.target.value = this.target || '';
  }

  renderBreak() {
    const node = this.target && this.ctx.editor.node(this.target);
    if (!node) return patch(this.el.breakBtns, html`<span class="muted small">No part picked yet.</span>`);
    const broken = this.run.failures.has(node.id);
    const waitingToStart = isAutoscaled(node) && !this.run.scale.has(node.id);
    patch(this.el.breakBtns, html`${failureOptions(node, this.ctx.graph()).map((o) => {
      const off = (broken && o.kind !== 'empty') || (o.kind === 'lose-one' && waitingToStart);
      const why = broken ? 'Already broken. Fix it first.' : 'Start the run first.';
      return html`<button type="button" class="btn btn-sm btn-danger" data-break="${o.kind}" ${off ? html`disabled title="${why}"` : ''}>${o.label}</button>`;
    })}`);
  }

  breakTarget(kind) {
    const node = this.target && this.ctx.editor.node(this.target);
    if (!node) return toast('Pick a part first.', 'warn');
    if (!this.run.fail(node, kind, this.ctx.graph())) {
      toast(this.run.failures.has(node.id) ? `${node.label}: already broken. Fix it first.` : 'Start the run first.', 'warn');
      return;
    }
    if (this.state === 'idle') toast('Press Start to see what happens.', 'info');
    this.afterControl();
  }

  renderProblems() {
    const run = this.run;
    const rows = [...run.failures].map(([id, f]) => html`<li><span class="tone-dot bad"></span><span><strong>${f.label}</strong>: ${failureWord(this.ctx.editor.node(id), f)} <span class="muted small">since ${clock(f.since)}</span></span><button type="button" class="btn btn-sm" data-fix="${id}">Fix</button></li>`);
    patch(this.el.problems, rows.length
      ? html`<h4 class="run-sub">Broken now</h4><ul class="run-problems">${rows}</ul>${run.failures.size > 1 ? html`<div class="run-btns"><button type="button" class="btn btn-sm" data-act="fix-all">Fix everything</button></div>` : ''}`
      : html`<p class="muted small">Nothing is broken.</p>`);

    const label = (id) => this.ctx.editor.node(id)?.label || id;
    const warm = [...run.warm].map(([id, w]) => html`<li><span class="tone-dot warn"></span><span><strong>${label(id)}</strong>: refilling, ${Math.round(Math.min(1, w.from + (run.t - w.since) / WARM_SECONDS) * 100)}% warm</span></li>`);
    const booting = [...run.scale].filter(([, s]) => s.starting.length).map(([id, s]) => {
      const count = s.starting.reduce((sum, x) => sum + x.count, 0);
      const soonest = Math.max(0, Math.ceil(Math.min(...s.starting.map((x) => x.readyAt)) - run.t));
      return html`<li><span class="tone-dot info"></span><span><strong>${label(id)}</strong>: ${count} starting, ready in ${soonest} s</span></li>`;
    });
    patch(this.el.recovering, warm.length || booting.length ? html`<h4 class="run-sub">Recovering</h4><ul class="run-problems">${warm}${booting}</ul>` : '');
  }

  // ---------------------------------------------------------------- capacity
  refreshDesign() {
    this.renderTargets();
    this.renderBreak();
    const nodes = this.breakables();
    const ro = this.ctx.editor.readOnly;
    patch(this.el.parts, nodes.length ? html`${nodes.map((n) => {
      const comp = COMPONENTS[n.type];
      const knob = knobFor(n);
      return html`<div class="run-part cat-${comp.category}" data-part="${n.id}">
        <span class="pal-badge" aria-hidden="true">${comp.abbr}</span>
        <div class="rp-main">
          <div class="rp-top"><strong title="${n.label}">${n.label}</strong><span data-load>–</span></div>
          <div class="rp-bar"><i data-bar></i></div>
          <div class="rp-sub" data-sub></div>
        </div>
        <div class="rp-knobs">
          ${knob && !ro ? html`<button type="button" data-knob="-1" data-id="${n.id}" title="Less: ${knob.word}" aria-label="Less capacity for ${n.label}">−</button><button type="button" data-knob="1" data-id="${n.id}" title="More: ${knob.word}" aria-label="More capacity for ${n.label}">+</button>` : ''}
          <button type="button" data-open="${n.id}" title="All settings" aria-label="All settings for ${n.label}">⚙</button>
        </div>
      </div>`;
    })}` : html`<p class="muted small">Add parts to the canvas to see them here.</p>`);
    this.paintParts(this.run.last);
  }

  partText(node, m) {
    const p = (k) => param(node, k);
    const run = this.run;
    let text;
    if (isAutoscaled(node)) {
      const s = run.scale.get(node.id);
      const starting = s ? s.starting.reduce((sum, x) => sum + x.count, 0) : 0;
      text = s ? `${s.running} running${starting ? ` · ${starting} starting` : ''} · max ${p('autoscale_max')}` : `autoscale ${p('autoscale_min')}–${p('autoscale_max')}`;
    } else if (node.type === 'Cache') text = countWord(p('replicas'), 'node', 'nodes');
    else if (node.type === 'ConsumerGroup') text = countWord(p('replicas'), 'consumer', 'consumers');
    else if (KAFKA_CONSUMERS.has(node.type)) text = countWord(p('replicas'), 'instance', 'instances');
    else if (REPLICA_TYPES.has(node.type)) text = countWord(p('replicas'), 'copy', 'copies');
    else if (node.type === 'KafkaTopic') text = `${countWord(p('partitions'), 'partition', 'partitions')} · ${m?.kafka ? `${m.kafka.brokers_alive} of ${p('brokers')}` : p('brokers')} brokers`;
    else if (node.type === 'Database') text = `${countWord(p('shards'), 'shard', 'shards')} · ${countWord(p('read_replicas'), 'replica', 'replicas')}`;
    else if (node.type === 'SearchIndex') text = `${countWord(p('shards'), 'shard', 'shards')} · ${countWord(p('replicas'), 'extra copy', 'extra copies')}`;
    else if (node.type === 'Queue') text = `${fmtNum(run.depth.get(node.id) || 0)} waiting · room for ${fmtNum(p('max_depth'))}`;
    else if (node.type === 'ThirdPartyAPI') text = `allows ${fmtNum(p('capacity'))}/s`;
    else if (node.type === 'CDN') text = `hit ${Math.round(p('cache_hit_ratio') * 100)}%`;
    else text = COMPONENTS[node.type].label;
    if (m && m.demand_rps > 0 && node.type !== 'Queue') text += ` · ${fmtNum(m.demand_rps)}/s in`;
    const lag = run.lag.get(node.id) || 0;
    if (lag >= 1) text += ` · ${fmtNum(lag)} behind`;
    const f = run.failures.get(node.id);
    if (f) text += ` · ${failureWord(node, f)}`;
    return text;
  }

  paintParts(sim) {
    const metrics = new Map((sim?.node_metrics || []).map((m) => [m.id, m]));
    for (const row of this.el.parts.querySelectorAll('[data-part]')) {
      const node = this.ctx.editor.node(row.dataset.part);
      if (!node) continue;
      const m = metrics.get(node.id);
      const status = m?.status || 'idle';
      const bar = row.querySelector('[data-bar]');
      bar.style.width = `${m && status !== 'down' ? Math.min(100, m.utilization * 100) : status === 'down' ? 100 : 0}%`;
      bar.className = `st-${status}`;
      const load = row.querySelector('[data-load]');
      load.textContent = !m ? '–' : status === 'down' ? 'Down' : status === 'idle' ? 'idle' : fmtLoad(m.utilization);
      load.className = status === 'overloaded' || status === 'down' ? 'bad' : status === 'hot' || status === 'warm' ? 'warn' : '';
      row.querySelector('[data-sub]').textContent = this.partText(node, m);
    }
  }

  turnKnob(id, dir) {
    const editor = this.ctx.editor;
    const node = editor.node(id);
    const knob = node && knobFor(node);
    if (!knob || editor.readOnly) return;
    if (dir < 0 && knob.growOnly) return toast(knob.growOnly, 'warn');
    const before = Number(editor.param(id, knob.key)) || knob.min;
    const next = knob.double ? (dir > 0 ? before * 2 : Math.round(before / 2)) : before + dir * knob.step(before);
    const value = Math.min(knob.max, Math.max(knob.min, next));
    if (value === before) return toast(dir > 0 ? 'That is the most this part allows.' : 'That is the least this part allows.', 'warn');
    editor.endEdit();
    editor.setParam(id, knob.key, value);
    editor.endEdit();
    if (this.active || this.state === 'stopped') {
      this.run.log('info', `You changed ${node.label}: ${knob.word} ${fmtNum(before)} → ${fmtNum(value)}.`);
      this.renderLog();
    }
  }

  // ---------------------------------------------------------------- log and report
  renderLog(reset = false) {
    const log = this.el.log;
    if (reset) {
      this.shownEvent = 0;
      setHtml(log, html`<li class="muted small run-log-empty">Press Start, then try a spike or break a part.</li>`);
    }
    const fresh = this.run.events.filter((e) => e.id > this.shownEvent);
    if (!fresh.length) return;
    log.querySelector('.run-log-empty')?.remove();
    for (const e of fresh) {
      const li = document.createElement('li');
      setHtml(li, html`<span class="t">${clock(e.t)}</span><span class="tone-dot ${e.tone}"></span><span>${e.text}</span>`);
      log.prepend(li);
    }
    this.shownEvent = fresh.at(-1).id;
    while (log.children.length > 150) log.lastElementChild.remove();
  }

  nextStep(label) {
    const node = this.ctx.editor.nodes.find((n) => n.label === label);
    if (node && KAFKA_CONSUMERS.has(node.type)) return `Next: give ${label} more consumers (up to its partitions, or add partitions too), so it keeps up and catches up faster after an outage. Then run again.`;
    if (node?.type === 'KafkaTopic') return `Next: give ${label} more partitions and brokers, or batch and compress more. Then run again.`;
    return `Next: give ${label} more capacity, or take load off it with a cache, a queue or a CDN. Then run again.`;
  }

  renderReport() {
    const r = this.run.summary();
    if (!r.duration) return patch(this.el.report, '');
    const [tone, word] = { held: ['good', 'Held up'], struggled: ['warn', 'Struggled'], failed: ['bad', 'Fell over'] }[r.verdict];
    patch(this.el.report, html`<div class="run-report">
      <div class="score-banner"><div class="big ${tone}">${fmtPct(r.successRate, r.successRate < 0.999 ? 1 : 0)}</div><div><strong>${word}</strong><p>of ${fmtNum(r.requests)} requests were served in ${clock(r.duration)} of simulated time.</p></div></div>
      <ul class="list-plain">
        <li>Busiest moment: <strong>${times(r.peakMult)}</strong> normal traffic, ${fmtNum(r.peakRps)} requests a second.</li>
        <li>${r.breakAt ? html`Errors first passed 1% at <strong>${times(r.breakAt.mult)}</strong> normal, ${clock(r.breakAt.t)} into the run.` : 'Errors stayed under 1% the whole time.'}</li>
        <li>The slowest 5% peaked at <strong>${fmtMs(r.worstP95)}</strong>${r.secondsOverTarget > 0 ? `, over the ${this.run.latencyTarget} ms target for ${clock(r.secondsOverTarget)}` : ''}.</li>
        ${r.busiest.length ? html`<li>Struggled most: ${r.busiest.map((b, i) => html`${i ? ', ' : ''}<strong>${b.label}</strong> (${clock(b.seconds)})`)}.</li>` : ''}
        ${r.failures ? html`<li>You broke something ${r.failures === 1 ? 'once' : `${r.failures} times`}.</li>` : ''}
        ${r.kafka ? html`<li>${r.kafka.maxLag >= 1 ? html`Consumer lag peaked at <strong>${fmtNum(r.kafka.maxLag)}</strong> messages, with data up to <strong>${fmtDuration(r.kafka.worstAge)}</strong> old${r.kafka.lagNow >= 1 ? html`; ${fmtNum(r.kafka.lagNow)} are still waiting` : ', and consumers caught up'}.` : 'Consumers kept up with every topic the whole time.'}</li>` : ''}
        ${r.kafka?.expired >= 1 ? html`<li class="bad">${fmtNum(r.kafka.expired)} messages passed the topic's retention and were deleted before anyone read them.</li>` : ''}
      </ul>
      ${r.busiest.length ? html`<p class="muted small">${this.nextStep(r.busiest[0].label)}</p>` : ''}
    </div>`);
  }
}

// ================================================================ workspace: modes: practice
function usersNode(traffic, extra = {}) {
  return { id: 'users', type: 'User', label: 'Users', x: 40, y: 200, config: { basic: { users: traffic.users, requests_per_user: traffic.requests_per_user } }, ...extra };
}

// Read share of the main database in the reference design.
function mainReadShare(problem) {
  const db = problem.stages?.at(-1)?.graph?.nodes?.find((n) => n.type === 'Database');
  return db ? param(db, 'read_ratio') : null;
}

function briefHtml(problem, { hintsShown = 0, showRequired = false } = {}) {
  const reads = mainReadShare(problem);
  return html`
    <div class="brief">
      <div class="meta"><span class="diff diff-${problem.difficulty.toLowerCase()}">${problem.difficulty}</span><span class="pill">${problem.category}</span></div>
      <h2>${problem.title}</h2>
      <p>${problem.statement}</p>
      <h3>What it must do</h3>
      <ul>${problem.functional.map((t) => html`<li>${t}</li>`)}</ul>
      <h3>How well it must work</h3>
      <ul>${problem.non_functional.map((t) => html`<li>${t}</li>`)}</ul>
      <h3>Numbers</h3>
      <div class="section-card kv">
        <span>Users at peak</span><strong>${fmtNum(problem.scale.users)}</strong>
        <span>Requests per second</span><strong>${fmtNum(problem.scale.peak_rps)}</strong>
        <span>Target speed (p95)</span><strong>${problem.scale.latency_target_ms} ms</strong>
        ${problem.scale.freshness_target_ms ? html`<span>Data freshness</span><strong>under ${fmtDuration(problem.scale.freshness_target_ms / 1000)}</strong>` : ''}
        <span>Normal spike</span><strong>${problem.scale.regular_spike}×</strong>
        <span>Biggest spike</span><strong>${problem.scale.mega_spike}×</strong>
        ${reads !== null ? html`<span>Database reads</span><strong>${Math.round(reads * 100)}% of queries</strong>` : ''}
      </div>
      ${reads !== null ? html`<p class="muted small">New databases you add start with this read share. You can change it under Advanced.</p>` : ''}
      ${problem.scale.freshness_target_ms ? html`<p class="muted small">Freshness is how old the newest data your consumers have handled may get. Consumers that fall behind fail it.</p>` : ''}
      ${problem.constraints.length ? html`<h3>Limits</h3><ul>${problem.constraints.map((t) => html`<li>${t}</li>`)}</ul>` : ''}
      <h3>Hints</h3>
      ${problem.hints.slice(0, hintsShown).map((h, i) => html`<div class="hint-box"><strong>Hint ${i + 1}.</strong> ${h}</div>`)}
      ${hintsShown < problem.hints.length ? html`<button class="btn btn-sm" type="button" data-hint>Show hint ${hintsShown + 1} of ${problem.hints.length}</button>` : html`<p class="muted small">No more hints.</p>`}
      <h3>Parts you will probably need</h3>
      ${showRequired
        ? html`<ul>${problem.required.map((r) => html`<li><strong>${r.type}</strong>: ${r.why}</li>`)}</ul>`
        : html`<button class="btn btn-sm btn-ghost" type="button" data-required>Show me</button>`}
      <h3>Tests</h3>
      <ul>${problem.tests.map((t) => html`<li>${t.name} <span class="muted small">(${t.tier === 'system' ? 'on Submit' : 'on Run'})</span>${t.requires?.length ? html`<br><span class="muted small">Must keep: ${t.requires.map((r) => REQUIREMENT_LABELS[r] || r).join('; ')}</span>` : ''}</li>`)}</ul>
      ${problem.concepts.length ? html`<h3>Learn the ideas</h3><ul>${problem.concepts.map((c) => html`<li><a href="/roadmap/${c.id}">${c.title}</a></li>`)}</ul>` : ''}
    </div>`;
}

async function initPracticeMode(ctx) {
  const problem = await api(`/api/problems/${encodeURIComponent(ctx.id)}`);
  const traffic = problemTraffic(problem, 1);
  let hintsShown = 0;
  let showRequired = false;

  ctx.setTitle(`Practice: ${problem.title}`);
  ctx.setTraffic(traffic);
  ctx.setRunInfo({ latencyTarget: problem.scale.latency_target_ms, spikes: { regular: problem.scale.regular_spike, mega: problem.scale.mega_spike } });
  const reads = mainReadShare(problem);
  if (reads !== null) ctx.editor.newDefaults = { Database: { read_ratio: reads } };
  ctx.restoreOr({ nodes: [usersNode(traffic)], edges: [] });

  const renderLeft = () => {
    setHtml(ctx.left, html`${briefHtml(problem, { hintsShown, showRequired })}
      <div class="section-card" style="margin-top:14px"><strong>Stuck?</strong><p class="muted small" style="margin:4px 0 8px">See how the design grows, step by step.</p><a class="btn btn-sm" href="/learn/${problem.id}">Open the reference design</a></div>`);
  };

  ctx.left.addEventListener('click', (e) => {
    if (e.target.closest('[data-hint]')) { hintsShown++; renderLeft(); }
    if (e.target.closest('[data-required]')) { showRequired = true; renderLeft(); }
  });

  const showTest = (t) => {
    const def = problem.tests.find((x) => x.id === t.id);
    const result = runTestCase(ctx.graph(), problem, def);
    ctx.showSim(result.sim, `${t.name} (${result.score})`);
  };

  const run = async (btn, submit) => {
    busy(btn, true, submit ? 'Scoring…' : 'Running…');
    try {
      const data = await api(`/api/practice/${problem.id}/${submit ? 'submit' : 'run'}`, { method: 'POST', body: { graph: ctx.editor.toGraph() } });
      const tiers = [...new Set(data.tests.map((t) => t.tier))];
      renderSuite(ctx.results, data, {
        title: submit ? 'Submitted' : `${data.summary.passed} of ${data.summary.total} basic tests passed`,
        intro: submit ? '' : 'Run checks normal load, a spike and one failure. Submit adds bigger tests and gives a score.',
        onShow: showTest,
        onFinding: (ids) => ctx.editor.flash(ids),
        solver: {
          solve: (id) => solveTest(ctx.graph(), problem, problem.tests.find((t) => t.id === id)),
          solveAll: () => solveSuite(ctx.graph(), problem, { tiers }),
          apply: (solution, message) => {
            ctx.editor.snapshot();
            ctx.editor.load(solution, { keepHistory: true, fit: false });
            toast(message, 'ok');
            run(null, submit);
          },
        },
      });
      ctx.showTab('results');
      if (submit) toast(`Score ${data.submission.total} (${data.submission.grade})`, data.submission.total >= 60 ? 'ok' : 'warn');
    } catch (err) {
      toast(errorText(err), 'bad');
    } finally {
      busy(btn, false);
    }
  };

  ctx.setActions([
    { id: 'run', label: 'Run tests', onClick: (b) => run(b, false), title: 'Normal load, a spike and one failure' },
    { id: 'submit', label: 'Submit', primary: true, onClick: (b) => run(b, true), title: 'All tests plus design and security review' },
  ]);
  ctx.setTitle(`Practice: ${problem.title}`);
  renderLeft();
  setHtml(ctx.results, html`<h3>No results yet</h3><p class="panel-intro">Build your design, then press <strong>Run tests</strong>. Results show here.</p>`);
}

// ================================================================ workspace: modes: learn
function typeCounts(graph) {
  const counts = new Map();
  for (const n of graph.nodes) counts.set(n.type, (counts.get(n.type) || 0) + 1);
  return counts;
}

function changesFrom(prev, next) {
  if (!prev) return [];
  const a = typeCounts(prev);
  const b = typeCounts(next);
  const out = [];
  for (const [type, n] of b) if (n > (a.get(type) || 0)) out.push(`+ ${COMPONENTS[type].label}`);
  for (const [type, n] of a) if (n > (b.get(type) || 0)) out.push(`− ${COMPONENTS[type].label}`);
  return out;
}

async function initLearnMode(ctx) {
  const problem = await api(`/api/problems/${encodeURIComponent(ctx.id)}`);
  const stageKey = `kafkalab:learn-stage:${problem.id}`;
  let current = Math.min(Number(localStorage.getItem(stageKey)) || 0, problem.stages.length - 1);
  let edited = false;

  ctx.setAutosave(false);
  ctx.setTraffic(problemTraffic(problem, 1));
  ctx.setRunInfo({ latencyTarget: problem.scale.latency_target_ms, spikes: { regular: problem.scale.regular_spike, mega: problem.scale.mega_spike } });
  ctx.editor.addEventListener('change', (e) => {
    if (['add', 'remove', 'connect', 'edit', 'move'].includes(e.detail.reason)) edited = true;
  });

  const loadStage = (i) => {
    current = i;
    localStorage.setItem(stageKey, String(i));
    ctx.editor.load(problem.stages[i].graph);
    edited = false;
    renderLeft();
    setHtml(ctx.results, html`<h3>Stage ${i + 1} loaded</h3><p class="panel-intro">Press <strong>Run all tests</strong> to see which tests this stage passes.</p>`);
  };

  const renderLeft = () => {
    const stage = problem.stages[current];
    const changes = changesFrom(problem.stages[current - 1]?.graph, stage.graph);
    setHtml(ctx.left, html`<div class="brief">
      <div class="meta"><span class="diff diff-${problem.difficulty.toLowerCase()}">${problem.difficulty}</span><span class="pill">${problem.stages.length} stages</span></div>
      <h2>${problem.title}</h2>
      <p>${problem.summary}</p>
      <ol class="stage-list">${problem.stages.map((s, i) => html`<li><button type="button" class="${i === current ? 'active' : ''}" data-stage="${i}"><span class="stage-n">${i + 1}</span>${s.title}</button></li>`)}</ol>
      <div class="section-card">
        <p class="eyebrow">Stage ${current + 1}</p>
        <h3 style="margin-top:0">${stage.title}</h3>
        <p><strong>Goal:</strong> ${stage.goal}</p>
        ${changes.length ? html`<div class="changes">${changes.map((c) => html`<span class="pill">${c}</span>`)}</div>` : ''}
        <ul>${stage.insights.map((t) => html`<li>${t}</li>`)}</ul>
        <div class="insp-actions">
          <button class="btn btn-sm" type="button" data-reload>Reload this stage</button>
          ${current < problem.stages.length - 1 ? html`<button class="btn btn-sm btn-primary" type="button" data-next>Next stage →</button>` : ''}
        </div>
      </div>
      <h3>The brief</h3>
      <p>${problem.statement}</p>
      <div class="section-card"><strong>Ready to try?</strong><p class="muted small" style="margin:4px 0 8px">Design it yourself from an empty canvas.</p><a class="btn btn-sm" href="/practice/${problem.id}">Practice this problem</a></div>
    </div>`);
  };

  ctx.left.addEventListener('click', async (e) => {
    const stageBtn = e.target.closest('[data-stage]');
    const go = async (i) => {
      if (edited) {
        const ok = await dialog({ title: 'Load another stage?', body: html`<p>Your changes on the canvas will be replaced.</p>`, actions: [{ label: 'Cancel', value: false }, { label: 'Load stage', value: true, primary: true }] });
        if (!ok) return;
      }
      loadStage(i);
    };
    if (stageBtn) await go(Number(stageBtn.dataset.stage));
    if (e.target.closest('[data-next]')) await go(current + 1);
    if (e.target.closest('[data-reload]')) await go(current);
  });

  const runAll = () => {
    const graph = ctx.graph();
    if (!graph.nodes.length) return toast('Add some components first.', 'warn');
    const suite = runSuite(graph, problem, { tiers: ['component', 'system'] });
    const rules = analyzeDesign(graph, { problem, sim: suite.baseline });
    renderSuite(ctx.results, { summary: suite.summary, tests: suite.results.map(({ sim, ...t }) => t), rules, simulation: suite.baseline }, {
      title: `Stage ${current + 1}: ${suite.summary.passed} of ${suite.summary.total} tests pass`,
      intro: 'These tests ran in your browser using the same engine as the server.',
      onShow: (t) => {
        const result = runTestCase(graph, problem, problem.tests.find((x) => x.id === t.id));
        ctx.showSim(result.sim, `${t.name} (${result.score})`);
      },
      onFinding: (ids) => ctx.editor.flash(ids),
      solver: {
        solve: (id) => solveTest(ctx.graph(), problem, problem.tests.find((x) => x.id === id)),
        solveAll: () => solveSuite(ctx.graph(), problem, { tiers: ['component', 'system'] }),
        apply: (solution, message) => {
          ctx.editor.snapshot();
          ctx.editor.load(solution, { keepHistory: true, fit: false });
          edited = true;
          toast(message, 'ok');
          runAll();
        },
      },
    });
    ctx.showTab('results');
  };

  ctx.setActions([{ id: 'run', label: 'Run all tests', primary: true, onClick: runAll, title: 'Every test, run in your browser' }]);
  ctx.setTitle(`Learn: ${problem.title}`);
  loadStage(current);
}

// ================================================================ workspace: modes: build
const BRIEF_KEY = 'kafkalab:build-brief';

const STARTER = {
  nodes: [
    { id: 'users', type: 'User', label: 'Users', x: 40, y: 140, config: { basic: { users: 2000, requests_per_user: 1 } } },
    { id: 'lb', type: 'LoadBalancer', label: 'Load Balancer', x: 250, y: 140 },
    { id: 'app', type: 'Server', label: 'App Server', x: 460, y: 140, config: { advanced: { replicas: 2 } } },
    { id: 'db', type: 'Database', label: 'Database', x: 670, y: 140 },
  ],
  edges: [
    { source: 'users', target: 'lb' }, { source: 'lb', target: 'app' }, { source: 'app', target: 'db' },
  ],
};

async function initBuildMode(ctx) {
  let brief = { name: 'My system', rps: '', latency: 300, focus: [] };
  try { brief = { ...brief, ...JSON.parse(localStorage.getItem(BRIEF_KEY) || '{}') }; } catch { /* ignore */ }
  const [catalog, problems] = await Promise.all([api('/api/stress/catalog'), api('/api/problems')]);
  let lastStress = null;
  let playTimer = null;

  const trafficNow = () => (Number(brief.rps) > 0 ? { requests_per_second: Number(brief.rps) } : {});
  const saveBrief = () => localStorage.setItem(BRIEF_KEY, JSON.stringify(brief));

  ctx.restoreOr(STARTER);
  ctx.setTraffic(trafficNow());
  ctx.setRunInfo({ latencyTarget: Number(brief.latency) || 300 });

  const typesInDesign = () => new Set(ctx.editor.nodes.map((n) => n.type));

  const renderLeft = () => {
    const types = typesInDesign();
    setHtml(ctx.left, html`<div class="brief">
      <h2>Build and stress test</h2>
      <p>Design any system, then push traffic through it. Watch which part breaks first.</p>
      <div class="form" style="gap:10px">
        <label class="field"><span>Name</span><input class="input" data-brief="name" value="${brief.name}" maxlength="80"></label>
        <label class="field"><span>Normal traffic (requests per second)</span><input class="input" type="number" min="0" data-brief="rps" value="${brief.rps}" placeholder="use the Users box"><small>Leave empty to use the numbers in your Users box.</small></label>
        <label class="field"><span>Target speed (p95, ms)</span><input class="input" type="number" min="10" data-brief="latency" value="${brief.latency}"></label>
      </div>
      <h3>Focus tests</h3>
      <p class="muted small">Pick failures to try. Tests for parts you do not have are skipped.</p>
      ${catalog.areas.map((a) => {
        const present = a.types.some((t) => types.has(t));
        return html`<details class="focus-area" ${present ? 'open' : ''}><summary>${a.label}<span class="muted small">${present ? '' : 'not in design'}</span></summary>
          <div class="tests">${a.tests.map((t) => html`<label class="check"><input type="checkbox" data-focus="${t.id}" ${brief.focus.includes(t.id) ? 'checked' : ''}><span><strong>${t.name}</strong><br><span class="muted">${t.description}</span></span></label>`)}</div>
        </details>`;
      })}
      <h3>Start from a template</h3>
      <div style="display:flex;gap:8px"><select class="input" data-template><option value="">Pick a system…</option>${problems.items.map((p) => html`<option value="${p.id}">${p.title}</option>`)}</select><button class="btn btn-sm" type="button" data-load-template>Load</button></div>
      <h3>Keep a design</h3>
      <p class="muted small">Use the download button in the toolbar to save your design as a file, and the upload button to open it again.</p>
    </div>`);
  };

  ctx.left.addEventListener('input', (e) => {
    const key = e.target.dataset.brief;
    if (key) {
      brief[key] = e.target.value;
      saveBrief();
      if (key === 'rps') ctx.setTraffic(trafficNow());
      if (key === 'latency') ctx.setRunInfo({ latencyTarget: Number(brief.latency) || 300 });
    }
  });
  ctx.left.addEventListener('change', (e) => {
    const id = e.target.dataset.focus;
    if (id) {
      brief.focus = e.target.checked ? [...new Set([...brief.focus, id])] : brief.focus.filter((x) => x !== id);
      saveBrief();
    }
  });
  ctx.left.addEventListener('click', async (e) => {
    if (e.target.closest('[data-load-template]')) {
      const id = ctx.left.querySelector('[data-template]').value;
      if (!id) return toast('Pick a system first.', 'warn');
      const ok = await dialog({ title: 'Load this template?', body: html`<p>It replaces the canvas. You can undo it.</p>`, actions: [{ label: 'Cancel', value: false }, { label: 'Load', value: true, primary: true }] });
      if (!ok) return;
      const p = await api(`/api/problems/${id}`);
      const graph = p.stages[p.stages.length - 1].graph;
      ctx.editor.snapshot();
      ctx.editor.load(graph, { keepHistory: true });
      brief.name = p.title;
      brief.rps = String(p.scale.peak_rps);
      brief.latency = p.scale.latency_target_ms;
      saveBrief();
      ctx.setTraffic(trafficNow());
      ctx.setRunInfo({ latencyTarget: brief.latency, spikes: { regular: p.scale.regular_spike, mega: p.scale.mega_spike } });
      renderLeft();
    }
  });
  ctx.editor.addEventListener('change', (e) => {
    if (['add', 'remove', 'load'].includes(e.detail.reason)) renderLeft();
  });

  const stopPlay = () => {
    clearInterval(playTimer);
    playTimer = null;
  };

  const drawStress = (cursor = null) => {
    renderStress(ctx.results, lastStress, {
      cursor,
      onPlay: play,
      onShowTest: (t) => { stopPlay(); ctx.showHeat(t.load, t.name); },
      onFinding: (ids) => ctx.editor.flash(ids),
    });
  };

  function play() {
    if (!lastStress) return;
    stopPlay();
    let i = 0;
    const frame = () => {
      const pt = lastStress.ramp[i];
      ctx.showHeat(pt.load, `${pt.x}× traffic · ${Math.round(pt.rps).toLocaleString()} req/s · ${(pt.error * 100).toFixed(1)}% errors`);
      drawStress(i);
      i++;
      if (i >= lastStress.ramp.length) stopPlay();
    };
    frame();
    playTimer = setInterval(frame, 900);
  }

  const simulateNow = async (btn) => {
    busy(btn, true, 'Simulating…');
    try {
      const data = await api('/api/simulate', { method: 'POST', body: { graph: ctx.editor.toGraph(), traffic: trafficNow(), latency_target_ms: Number(brief.latency) || 300 } });
      renderSimulation(ctx.results, data, { onFinding: (ids) => ctx.editor.flash(ids) });
      ctx.showTab('results');
    } catch (err) {
      toast(errorText(err), 'bad');
    } finally { busy(btn, false); }
  };

  const stressNow = async (btn) => {
    busy(btn, true, 'Testing…');
    stopPlay();
    try {
      lastStress = await api('/api/stress-test', {
        method: 'POST',
        body: { graph: ctx.editor.toGraph(), traffic: trafficNow(), latency_target_ms: Number(brief.latency) || 300, focus: brief.focus },
      });
      drawStress();
      ctx.showTab('results');
      play();
    } catch (err) {
      toast(errorText(err), 'bad');
    } finally { busy(btn, false); }
  };

  ctx.setActions([
    { id: 'simulate', label: 'Simulate', onClick: simulateNow, title: 'One run at normal traffic' },
    { id: 'stress', label: 'Stress test', primary: true, onClick: stressNow, title: 'Ramp traffic up to 10× and run focus tests' },
  ]);
  ctx.setTitle('Build and stress test');
  setHtml(ctx.results, html`<h3>No results yet</h3><p class="panel-intro">Press <strong>Stress test</strong> to ramp traffic from half to ten times normal. The canvas shows each step.</p>
    <p class="muted small">Components available: ${Object.keys(COMPONENTS).length}. Drag them from the left side of the canvas.</p>`);
  renderLeft();
}

// ================================================================ workspace: modes: fix
async function initFixMode(ctx) {
  const inc = await api(`/api/incidents/${encodeURIComponent(ctx.id)}`);
  const stateKey = `kafkalab:fix:${inc.id}`;
  let st = { step: 'diagnose', rootCause: null, hints: [], debrief: null };
  try { st = { ...st, ...JSON.parse(localStorage.getItem(stateKey) || '{}') }; } catch { /* ignore */ }
  let check = null;
  let baseline = null;
  let solutionGraph = null;
  const save = () => localStorage.setItem(stateKey, JSON.stringify(st));
  const fetchSolution = async () => {
    const res = await api(`/api/incidents/${inc.id}/solution`, { method: 'POST', body: {} });
    solutionGraph = res.graph;
    return res;
  };

  ctx.setTraffic(inc.traffic, inc.overrides || {});
  ctx.setRunInfo({ latencyTarget: inc.goals?.find((g) => g.type === 'latency_below')?.value || 300 });
  ctx.restoreOr(inc.graph);

  const goalRow = (g, i) => {
    const res = check?.goals[i];
    return html`<div class="goal ${res ? (res.pass ? 'pass' : 'fail') : ''}"><span class="g-icon">${res ? (res.pass ? '✓' : '✗') : i + 1}</span><span>${g.text}${res ? html`<small>${res.detail}</small>` : ''}</span></div>`;
  };

  const stepClass = (name) => {
    const order = ['diagnose', 'fix', 'debrief', 'done'];
    const at = order.indexOf(st.step);
    const me = order.indexOf(name);
    return me < at ? 'step done' : me === at ? 'step active' : 'step';
  };

  const renderLeft = () => {
    const rc = st.rootCause;
    setHtml(ctx.left, html`<div class="brief">
      <div class="meta"><span class="diff diff-${inc.difficulty}">${inc.difficulty}</span><span class="pill">${inc.category}</span><span class="muted small">${inc.product}</span></div>
      <h2>${inc.title}</h2>
      <p>${inc.headline}</p>
      <h3>What people see</h3>
      <ul>${inc.symptoms.map((s) => html`<li>${s}</li>`)}</ul>
      <h3>Goals</h3>
      <div class="section-card">${inc.goals.map(goalRow)}
        ${check && !check.constraint.ok ? html`<div class="goal fail"><span class="g-icon">!</span><span>${check.constraint.problems.join(' ')}</span></div>` : ''}
        <p class="muted small" style="margin:8px 0 0">${inc.constraints?.note || ''}</p>
      </div>
      <ol class="steps">
        <li class="${stepClass('diagnose')}">
          <div class="step-head"><span class="stage-n">1</span>Find the cause</div>
          <div class="step-body">
            <p>${inc.root_cause.question}</p>
            ${inc.root_cause.options.map((o) => {
              const cls = rc ? (o.id === rc.answer ? 'choice correct' : o.id === rc.picked ? 'choice wrong' : 'choice') : 'choice';
              return html`<button type="button" class="${cls}" data-rc="${o.id}" ${rc ? 'disabled' : ''}>${o.label} <span class="muted small">${o.type}</span></button>`;
            })}
            ${rc ? html`<p class="${rc.correct ? 'good' : 'warn'}"><strong>${rc.correct ? 'Right.' : 'Not quite.'}</strong> ${rc.explanation}</p>` : ''}
          </div>
        </li>
        <li class="${stepClass('fix')}">
          <div class="step-head"><span class="stage-n">2</span>Fix the design</div>
          ${st.step === 'fix' || st.step === 'debrief' || st.step === 'done' ? html`<div class="step-body">
            <p>Change settings or add parts on the canvas. Then press <strong>Check my fix</strong>.</p>
            ${st.hints.map((h, i) => html`<div class="hint-box"><strong>Hint ${i + 1}.</strong> ${h}</div>`)}
            ${st.hints.length < inc.hint_count && st.step === 'fix' ? html`<button class="btn btn-sm btn-ghost" type="button" data-hint>Get a hint</button>` : ''}
            ${st.solution ? html`<div class="solution">
              <p class="sol-title">The solution</p>
              <p>${st.solution.cause}</p>
              <ul class="sol-why">${st.solution.changes.map((c) => html`<li>${c}</li>`)}</ul>
              <div class="insp-actions"><button class="btn btn-sm btn-primary" type="button" data-apply-solution>Apply it to the canvas</button><span class="muted small">You can undo it.</span></div>
            </div>` : st.step === 'fix' ? html`<button class="btn btn-sm btn-ghost" type="button" data-solution>Show the solution</button>` : ''}
          </div>` : ''}
        </li>
        <li class="${stepClass('debrief')}">
          <div class="step-head"><span class="stage-n">3</span>One last question</div>
          ${st.step === 'debrief' || st.step === 'done' ? html`<div class="step-body">
            <p>${inc.debrief.question}</p>
            ${inc.debrief.options.map((o, i) => {
              const d = st.debrief;
              const cls = d ? (i === d.answer ? 'choice correct' : i === d.picked ? 'choice wrong' : 'choice') : 'choice';
              return html`<button type="button" class="${cls}" data-debrief="${i}" ${d ? 'disabled' : ''}>${o}</button>`;
            })}
            ${st.debrief ? html`<p>${st.debrief.explanation}</p>` : ''}
          </div>` : ''}
        </li>
      </ol>
      ${st.step === 'done' ? html`<div class="section-card"><p class="eyebrow">Solved</p><p><strong>Lesson:</strong> ${st.lesson}</p><a class="btn btn-sm btn-primary" href="/fix">Next incident</a></div>` : ''}
      <button class="btn btn-sm btn-ghost" type="button" data-reset>Reset to the broken system</button>
    </div>`);
  };

  const renderResults = () => {
    const c = check || baseline;
    if (!c) return;
    setHtml(ctx.results, html`
      <h3>${check?.solved ? 'Every goal passes' : check ? 'Not fixed yet' : 'Before your fix'}</h3>
      ${metricsGrid(c.simulation)}
      <h3>Goals</h3>
      ${c.goals.map((g) => html`<div class="goal ${g.pass ? 'pass' : 'fail'}"><span class="g-icon">${g.pass ? '✓' : '✗'}</span><span>${g.text}<small>${g.detail}</small></span></div>`)}
      ${!c.constraint.ok ? html`<div class="goal fail"><span class="g-icon">!</span><span>${c.constraint.problems.join(' ')}</span></div>` : ''}
      ${nodeTableHtml(c.simulation)}
      <p class="muted small">Errors ${fmtPct(c.metrics.error_rate, 1)} · p95 ${fmtMs(c.metrics.latency_ms.p95)}</p>`);
  };

  ctx.left.addEventListener('click', async (e) => {
    const rcBtn = e.target.closest('[data-rc]');
    if (rcBtn && !st.rootCause) {
      try {
        const res = await api(`/api/incidents/${inc.id}/root-cause`, { method: 'POST', body: { answer: rcBtn.dataset.rc } });
        st.rootCause = { ...res, picked: rcBtn.dataset.rc };
        st.step = 'fix';
        save();
        renderLeft();
        ctx.editor.flash([res.answer]);
      } catch (err) { toast(errorText(err), 'bad'); }
    }
    if (e.target.closest('[data-hint]')) {
      try {
        const res = await api(`/api/incidents/${inc.id}/hint`, { method: 'POST', body: { index: st.hints.length } });
        st.hints.push(res.hint);
        save();
        renderLeft();
      } catch (err) { toast(errorText(err), 'bad'); }
    }
    if (e.target.closest('[data-solution]')) {
      const ok = await dialog({ title: 'See the solution?', body: html`<p>This shows the answer. If you would rather work it out yourself, try a hint first.</p>`, actions: [{ label: 'Cancel', value: false }, { label: 'Show the solution', value: true, primary: true }] });
      if (!ok) return;
      try {
        const res = await fetchSolution();
        st.solution = { cause: res.cause, changes: res.changes };
        save();
        renderLeft();
      } catch (err) { toast(errorText(err), 'bad'); }
    }
    if (e.target.closest('[data-apply-solution]')) {
      try {
        if (!solutionGraph) await fetchSolution();
        ctx.editor.snapshot();
        ctx.editor.load(solutionGraph, { keepHistory: true });
        toast('Solution applied. Press Check my fix to see every goal pass.', 'ok');
      } catch (err) { toast(errorText(err), 'bad'); }
    }
    const dBtn = e.target.closest('[data-debrief]');
    if (dBtn && !st.debrief) {
      try {
        const res = await api(`/api/incidents/${inc.id}/debrief`, { method: 'POST', body: { answer: Number(dBtn.dataset.debrief), graph: ctx.editor.toGraph() } });
        st.debrief = { ...res, picked: Number(dBtn.dataset.debrief) };
        st.lesson = res.lesson;
        st.step = 'done';
        save();
        renderLeft();
        toast(res.correct ? 'Correct! Incident solved.' : 'Incident solved. Read the explanation for that last one.', 'ok');
      } catch (err) { toast(errorText(err), 'bad'); }
    }
    if (e.target.closest('[data-reset]')) {
      const ok = await dialog({ title: 'Reset this incident?', body: html`<p>The canvas goes back to the broken system and your steps start over.</p>`, actions: [{ label: 'Cancel', value: false }, { label: 'Reset', value: true, danger: true }] });
      if (!ok) return;
      st = { step: 'diagnose', rootCause: null, hints: [], debrief: null };
      save();
      ctx.clearLocal();
      ctx.editor.load(inc.graph);
      check = null;
      renderLeft();
      renderResults();
    }
  });

  const runCheck = async (btn) => {
    busy(btn, true, 'Checking…');
    try {
      check = await api(`/api/incidents/${inc.id}/check`, { method: 'POST', body: { graph: ctx.editor.toGraph() } });
      if (check.solved && st.step === 'fix') {
        st.step = 'debrief';
        save();
        toast('All goals pass! Answer the last question to finish.', 'ok');
      } else if (check.solved && st.step === 'diagnose') {
        toast('All goals pass. Answer step 1 first to finish properly.', 'ok');
      } else if (!check.solved) {
        toast('Not fixed yet. Look at the goals that still fail.', 'warn');
      }
      renderLeft();
      renderResults();
      ctx.showTab('results');
    } catch (err) {
      toast(errorText(err), 'bad');
    } finally { busy(btn, false); }
  };

  ctx.setActions([{ id: 'check', label: 'Check my fix', primary: true, onClick: runCheck }]);
  ctx.setTitle(inc.title);
  renderLeft();
  try {
    baseline = await api(`/api/incidents/${inc.id}/check`, { method: 'POST', body: { graph: inc.graph } });
    renderResults();
  } catch { /* the results tab simply stays empty */ }
}

// ================================================================ workspace: modes: interview
const ROUNDS = ['Requirements', 'Design', 'Questions', 'Trade-offs'];

function notFound(ctx) {
  ctx.setReadOnly(true);
  ctx.setTitle('Mock interview');
  setHtml(ctx.left, html`<div class="brief"><p class="eyebrow">Mock interview</p><h2>Interview not found</h2>
    <p>This interview is not saved in this browser. Only your latest interview is kept, and only in the browser where you started it.</p></div>`);
  ctx.roundPanel.classList.remove('hidden');
  setHtml(ctx.roundPanel, html`<div class="round-inner"><p class="eyebrow">Nothing here</p><h2>Start a new interview</h2>
    <p class="lead">Pick a problem and a time limit to begin.</p><a class="btn btn-primary" href="/interview">Go to mock interviews</a></div>`);
}

async function initInterviewMode(ctx) {
  const session = loadInterview(ctx.id);
  ctx.setPreviewEnabled(false);
  ctx.setHintsVisible(false);
  if (!session) return notFound(ctx);
  if (session.report) {
    location.href = `/evaluations/${session.id}`;
    return;
  }
  const p = await api(`/api/interview/problems/${encodeURIComponent(session.problem_id)}`);
  session.draft = session.draft || {};
  if (session.round >= 3 && !session.design) session.round = 2;
  const save = () => saveInterview(session);

  ctx.setTitle(`Interview: ${p.title}`);
  ctx.restoreOr({ nodes: [usersNode({ users: p.scale.users, requests_per_user: +(p.scale.peak_rps / Math.max(1, p.scale.users)).toFixed(3) })], edges: [] });

  // ------------------------------------------------------------ timer
  const end = Date.parse(session.started_at) + session.duration_min * 60000;
  const tick = () => {
    const el = document.getElementById('iv-timer');
    if (!el) return;
    const left = end - Date.now();
    const total = session.duration_min * 60000;
    if (left <= 0) {
      el.textContent = 'Time is up';
      el.className = 'timer danger';
      return;
    }
    const m = Math.floor(left / 60000);
    const s = Math.floor((left % 60000) / 1000);
    el.textContent = `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    el.className = `timer${left < total * 0.1 ? ' danger' : left < total * 0.3 ? ' warn' : ''}`;
  };
  setInterval(tick, 1000);

  // ------------------------------------------------------------ left panel
  const renderLeft = () => {
    setHtml(ctx.left, html`<div class="brief">
      <p class="eyebrow">Mock interview</p>
      <div class="timer" id="iv-timer">--:--</div>
      <p class="muted small">Time left of ${session.duration_min} minutes. Nothing stops when time runs out, but the report notes it.</p>
      <ol class="round-steps">${ROUNDS.map((r, i) => html`<li class="${session.round === i + 1 ? 'active' : session.round > i + 1 ? 'done' : ''}"><span class="stage-n">${session.round > i + 1 ? '✓' : i + 1}</span>${r}</li>`)}</ol>
      <h2>${p.title}</h2>
      <p>${p.statement}</p>
      <h3>Numbers</h3>
      <div class="section-card kv"><span>Users</span><strong>${fmtNum(p.scale.users)}</strong><span>Requests per second</span><strong>${fmtNum(p.scale.peak_rps)}</strong><span>Target speed (p95)</span><strong>${p.scale.latency_target_ms} ms</strong></div>
      ${p.constraints.length ? html`<h3>Limits</h3><ul>${p.constraints.map((c) => html`<li>${c}</li>`)}</ul>` : ''}
      ${session.round === 2 ? html`<div class="hint-box"><strong>Round 2.</strong> Build your design on the canvas. When you are happy, press <strong>Submit design</strong>. It will be load-tested.</div>` : ''}
    </div>`);
    tick();
  };

  // ------------------------------------------------------------ rounds
  const panel = ctx.roundPanel;
  const showPanel = (content) => {
    panel.classList.remove('hidden');
    setHtml(panel, html`<div class="round-inner">${content}</div>`);
  };
  const waiting = (label) => showPanel(html`<p class="eyebrow">Please wait</p><h2>${label}</h2><p class="lead"><span class="spinner"></span> This takes a few seconds.</p>`);
  const advance = (round) => {
    session.round = round;
    save();
    renderLeft();
  };
  const continueButton = (label) => {
    const btn = document.createElement('button');
    btn.className = 'btn btn-primary';
    btn.type = 'button';
    btn.textContent = label;
    btn.addEventListener('click', showRound);
    panel.querySelector('.round-inner').append(btn);
  };

  const scoreLine = (label, score) => html`<div class="dim"><div class="dim-head"><span>${label}</span><span class="${score >= 75 ? 'good' : score >= 50 ? 'warn' : 'bad'}">${score}</span></div><div class="bar"><span style="width:${score}%"></span></div></div>`;

  const round1 = () => {
    ctx.setActions([]);
    showPanel(html`
      <p class="eyebrow">Round 1 of 4</p>
      <h2>Requirements</h2>
      <p class="lead">Before drawing anything, write what the system must do and how well it must work. One point per line.</p>
      <div class="form">
        <label class="field"><span>What it must do (functional)</span><textarea class="textarea" id="iv-fr" placeholder="- Users can ...">${session.draft.fr || ''}</textarea></label>
        <label class="field"><span>How well it must work (non-functional)</span><textarea class="textarea" id="iv-nfr" placeholder="- 95% of requests answer in under 200 ms&#10;- Available 99.9% of the time&#10;- Handles 10,000 requests per second">${session.draft.nfr || ''}</textarea><small>Use numbers: speed in ms, traffic per second, uptime in %.</small></label>
        <div><button class="btn btn-primary" type="button" id="iv-r1">Submit requirements</button></div>
      </div>`);
    const fr = document.getElementById('iv-fr');
    const nfr = document.getElementById('iv-nfr');
    fr.addEventListener('input', () => { session.draft.fr = fr.value; save(); });
    nfr.addEventListener('input', () => { session.draft.nfr = nfr.value; save(); });
    document.getElementById('iv-r1').addEventListener('click', async (e) => {
      busy(e.target, true, 'Submitting…');
      const body = { problem_id: p.id, functional: fr.value, non_functional: nfr.value };
      try {
        waiting('Reading your requirements');
        const result = await api('/api/interview/requirements', { method: 'POST', body });
        session.requirements = { functional: body.functional, non_functional: body.non_functional, result };
        advance(2);
        showPanel(html`
          <p class="eyebrow">Round 1 feedback</p>
          <h2>Requirements</h2>
          ${scoreLine('What it must do', result.functional.score)}
          ${scoreLine('How well it must work', result.non_functional.score)}
          ${result.feedback ? html`<div class="feedback"><h3>Feedback</h3><p>${result.feedback}</p></div>` : ''}
          <div class="feedback">
            <h3>You covered</h3>
            <ul>${[...result.functional.covered, ...result.non_functional.covered].map((t) => html`<li class="covered">${t}</li>`)}</ul>
            ${result.functional.missed.length || result.non_functional.missed.length ? html`<h3>You missed</h3><ul>${[...result.functional.missed, ...result.non_functional.missed].map((t) => html`<li class="missed">${t}</li>`)}</ul>` : ''}
            ${result.tips.length ? html`<h3>Tips</h3><ul>${result.tips.map((t) => html`<li>${t}</li>`)}</ul>` : ''}
          </div>`);
        continueButton('Continue to the design round →');
      } catch (err) {
        toast(errorText(err), 'bad');
        round1();
      }
    });
  };

  const round2 = () => {
    panel.classList.add('hidden');
    ctx.setActions([{
      id: 'submit-design', label: 'Submit design', primary: true,
      onClick: async (btn) => {
        const ok = await dialog({ title: 'Submit your design?', body: html`<p>It will be load-tested and you cannot change it afterwards.</p>`, actions: [{ label: 'Keep editing', value: false }, { label: 'Submit', value: true, primary: true }] });
        if (!ok) return;
        busy(btn, true, 'Submitting…');
        try {
          waiting('Load-testing your design');
          const res = await api('/api/interview/design', { method: 'POST', body: { problem_id: p.id, graph: ctx.editor.toGraph(), seed: session.id } });
          session.design = res.design;
          session.mcqs = { items: res.mcqs };
          session.tradeoffs = { items: res.tradeoffs };
          advance(3);
          toast(`Your design passed ${res.design.passed} of ${res.design.total} tests.`, res.design.passed === res.design.total ? 'ok' : 'warn');
          showRound();
        } catch (err) {
          toast(errorText(err), 'bad');
          panel.classList.add('hidden');
          busy(btn, false);
        }
      },
    }]);
  };

  const round3 = () => {
    ctx.setActions([]);
    const items = session.mcqs.items;
    showPanel(html`
      <p class="eyebrow">Round 3 of 4</p>
      <h2>Quick questions</h2>
      <p class="lead">Five questions picked from the parts you used. Your design passed ${session.design.passed} of ${session.design.total} tests.</p>
      <form id="iv-mcq">${items.map((q, qi) => html`<div class="quiz-q"><p>${qi + 1}. ${q.q}</p>${q.options.map((o, oi) => html`<label class="option" data-q="${qi}" data-o="${oi}"><input type="radio" name="q${qi}" value="${oi}"><span>${o}</span></label>`)}<div class="explain hidden" data-explain="${qi}"></div></div>`)}
      <button class="btn btn-primary" type="submit">Submit answers</button></form>`);
    const form = document.getElementById('iv-mcq');
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const answers = items.map((_, i) => {
        const checked = form.querySelector(`input[name="q${i}"]:checked`);
        return checked ? Number(checked.value) : -1;
      });
      if (answers.includes(-1)) {
        const ok = await dialog({ title: 'Some questions are blank', body: html`<p>Blank answers count as wrong. Submit anyway?</p>`, actions: [{ label: 'Go back', value: false }, { label: 'Submit', value: true, primary: true }] });
        if (!ok) return;
      }
      try {
        const res = await api('/api/interview/mcqs', { method: 'POST', body: { problem_id: p.id, graph: session.design.graph, seed: session.id, answers } });
        session.mcqs.answers = answers;
        advance(4);
        res.results.forEach((r, i) => {
          form.querySelectorAll(`[data-q="${i}"]`).forEach((opt) => {
            const o = Number(opt.dataset.o);
            if (o === r.answer) opt.classList.add('correct');
            else if (o === answers[i]) opt.classList.add('wrong');
            opt.querySelector('input').disabled = true;
          });
          const ex = form.querySelector(`[data-explain="${i}"]`);
          ex.textContent = r.explain;
          ex.classList.remove('hidden');
        });
        form.querySelector('button[type="submit"]').remove();
        form.insertAdjacentHTML('beforeend', String(html`<div class="feedback"><strong>${res.correct} of ${res.total} correct.</strong></div>`));
        continueButton('Continue to trade-offs →');
      } catch (err) { toast(errorText(err), 'bad'); }
    });
  };

  const round4 = () => {
    ctx.setActions([]);
    const items = session.tradeoffs.items;
    session.draft.trade = session.draft.trade || [];
    showPanel(html`
      <p class="eyebrow">Round 4 of 4</p>
      <h2>Trade-offs</h2>
      <p class="lead">Three questions about your own design. Explain your choice, what it costs, and what you would do instead.</p>
      ${items.map((q, i) => html`<div class="form" style="margin-bottom:18px"><label class="field"><span>${i + 1}. ${q.question}</span><textarea class="textarea" data-trade="${i}">${session.draft.trade[i] || ''}</textarea></label></div>`)}
      <button class="btn btn-primary" type="button" id="iv-r4">Submit answers</button>`);
    panel.querySelectorAll('[data-trade]').forEach((t) => t.addEventListener('input', () => { session.draft.trade[Number(t.dataset.trade)] = t.value; save(); }));
    document.getElementById('iv-r4').addEventListener('click', async (e) => {
      const answers = items.map((_, i) => panel.querySelector(`[data-trade="${i}"]`).value);
      busy(e.target, true, 'Submitting…');
      try {
        waiting('Reading your answers');
        const { items: graded } = await api('/api/interview/tradeoffs', { method: 'POST', body: { graph: session.design.graph, answers } });
        session.tradeoffs.answers = answers;
        session.tradeoffs.graded = graded;
        advance(5);
        showPanel(html`
          <p class="eyebrow">Round 4 feedback</p>
          <h2>Trade-offs</h2>
          ${graded.map((g, i) => html`<div class="feedback">
            <h3>${i + 1}. ${items[i].question}</h3>
            ${scoreLine('Score', Math.round(g.score * 10))}
            ${g.feedback ? html`<p>${g.feedback}</p>` : ''}
            ${g.covered.length ? html`<p class="covered">Covered: ${g.covered.join(', ')}</p>` : ''}
            ${g.missed.length ? html`<p class="missed">Missing: ${g.missed.join(', ')}</p>` : ''}
            <details><summary>A strong answer</summary><p>${g.model_answer}</p></details>
          </div>`)}
          <button class="btn btn-primary" type="button" id="iv-finish">Finish and see my report</button>`);
        document.getElementById('iv-finish').addEventListener('click', finish);
      } catch (err) {
        toast(errorText(err), 'bad');
        round4();
      }
    });
  };

  const round5 = () => {
    ctx.setActions([]);
    showPanel(html`<p class="eyebrow">All rounds done</p><h2>Ready for your report</h2><p class="lead">You finished every round.</p><button class="btn btn-primary" type="button" id="iv-finish">Finish and see my report</button>`);
    document.getElementById('iv-finish').addEventListener('click', finish);
  };

  async function finish(e) {
    busy(e.target, true, 'Building report…');
    try {
      const report = await api('/api/interview/report', {
        method: 'POST',
        body: {
          problem_id: p.id, seed: session.id, duration_min: session.duration_min, started_at: session.started_at,
          graph: session.design.graph,
          requirements: session.requirements?.result,
          mcq_answers: session.mcqs.answers,
          tradeoff_answers: session.tradeoffs.answers,
          tradeoffs: session.tradeoffs.graded,
        },
      });
      saveInterview({ id: session.id, problem_id: session.problem_id, duration_min: session.duration_min, started_at: session.started_at, round: 5, report });
      ctx.clearLocal();
      location.href = `/evaluations/${session.id}`;
    } catch (err) {
      toast(errorText(err), 'bad');
      busy(e.target, false);
    }
  }

  function showRound() {
    ctx.setReadOnly(session.round !== 2);
    if (session.round === 1) return round1();
    if (session.round === 2) return round2();
    if (session.round === 3) return round3();
    if (session.round === 4) return round4();
    return round5();
  }

  renderLeft();
  showRound();
}

// ================================================================ workspace: main
// The design workspace, used by Learn, Practice, Build, Design & Fix and mock interviews.
async function initWorkspace() {
  const ICONS = {
    undo: '<svg viewBox="0 0 24 24"><path d="M9 14 4 9l5-5"/><path d="M4 9h10.5a5.5 5.5 0 0 1 0 11H11"/></svg>',
    redo: '<svg viewBox="0 0 24 24"><path d="m15 14 5-5-5-5"/><path d="M20 9H9.5a5.5 5.5 0 0 0 0 11H13"/></svg>',
    arrange: '<svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="8.5" y="14" width="7" height="7" rx="1.5"/></svg>',
    export: '<svg viewBox="0 0 24 24"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg>',
    import: '<svg viewBox="0 0 24 24"><path d="M12 15V3"/><path d="m7 8 5-5 5 5"/><path d="M5 21h14"/></svg>',
    clear: '<svg viewBox="0 0 24 24"><path d="M4 7h16"/><path d="M10 11v6M14 11v6"/><path d="M6 7l1 13h10l1-13"/><path d="M9 7V4h6v3"/></svg>',
    panel: '<svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16"/></svg>',
    play: '<svg viewBox="0 0 24 24" class="solid"><path d="M7 4.5v15l13-7.5z"/></svg>',
    pause: '<svg viewBox="0 0 24 24" class="solid"><rect x="6" y="4.5" width="4" height="15" rx="1"/><rect x="14" y="4.5" width="4" height="15" rx="1"/></svg>',
    stop: '<svg viewBox="0 0 24 24" class="solid"><rect x="5.5" y="5.5" width="13" height="13" rx="2"/></svg>',
  };

  const data = pageData();
  const mode = data.mode;
  const ws = $('#ws');
  const editor = new Editor($('#canvas'));
  let preview = null;
  let pinned = false;
  let traffic = {};
  let overrides = {};
  let previewEnabled = true;
  let autosave = true;
  let runner = null;
  let overlayHeld = false;
  let runState = 'idle';
  const saveKey = `kafkalab:canvas:${mode}:${data.id || 'main'}`;

  // ---------------------------------------------------------------- palette
  function buildPalette() {
    const parts = [];
    for (const cat of CATEGORIES) {
      const items = Object.entries(COMPONENTS).filter(([, c]) => c.category === cat.id);
      if (!items.length) continue;
      parts.push(html`<div class="pal-group">${cat.label}</div>`);
      for (const [type, c] of items) {
        parts.push(html`<button type="button" class="pal-item cat-${c.category}" draggable="true" data-type="${type}" title="${c.description}"><span class="pal-badge">${c.abbr}</span>${c.label}</button>`);
      }
    }
    const pal = $('#palette');
    setHtml(pal, parts);
    pal.addEventListener('dragstart', (e) => {
      const item = e.target.closest('.pal-item');
      if (!item) return;
      e.dataTransfer.setData('application/x-hld-type', item.dataset.type);
      e.dataTransfer.effectAllowed = 'copy';
    });
    pal.addEventListener('click', (e) => {
      const item = e.target.closest('.pal-item');
      if (item) editor.addNodeAtCenter(item.dataset.type);
    });
  }

  // ---------------------------------------------------------------- tabs
  const narrow = () => window.matchMedia('(max-width: 980px)').matches;

  function showTab(name, { open = true } = {}) {
    $$('.tab').forEach((t) => {
      const on = t.dataset.tab === name;
      t.classList.toggle('active', on);
      t.setAttribute('aria-selected', String(on));
      if (on) t.querySelector('.dot')?.remove();
    });
    $$('.panel').forEach((p) => p.classList.toggle('hidden', p.id !== `panel-${name}`));
    if (open && narrow()) {
      ws.classList.add('show-right');
      ws.classList.remove('show-left');
    }
  }

  new ResizeObserver(() => ws.style.setProperty('--ws-toolbar-h', `${$('#ws-toolbar').offsetHeight}px`)).observe($('#ws-toolbar'));
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && narrow() && !document.querySelector('.modal-backdrop') && (ws.classList.contains('show-left') || ws.classList.contains('show-right'))) {
      ws.classList.remove('show-left', 'show-right');
    }
  });

  function markTab(name) {
    const tab = $(`.tab[data-tab="${name}"]`);
    if (tab && !tab.classList.contains('active') && !tab.querySelector('.dot')) tab.insertAdjacentHTML('beforeend', '<span class="dot"></span>');
  }

  $$('.tab').forEach((t) => t.addEventListener('click', () => showTab(t.dataset.tab)));

  // ---------------------------------------------------------------- preview
  function graphNow() {
    return normalizeGraph(editor.toGraph()).graph;
  }

  function runPreview() {
    if (!previewEnabled) return;
    const graph = graphNow();
    try {
      preview = graph.nodes.length ? simulate(graph, { traffic, overrides }) : null;
    } catch (err) {
      console.error(err);
      preview = null;
    }
    if (!pinned && !overlayHeld) {
      if (preview) editor.setOverlay(preview);
      else editor.clearOverlay();
    }
    renderHints($('#panel-hints'), liveHints(editor.toGraph()), preview, { onHint: (ids) => editor.flash(ids) });
    inspector.refreshMetrics();
  }
  const previewSoon = debounce(runPreview, 220);

  function showSim(sim, label) {
    runner?.yieldCanvas();
    pinned = true;
    editor.setOverlay(sim, { banner: { html: html`Showing: <strong>${label}</strong>`, action: 'Back to live view' } });
  }

  function showHeat(load, label) {
    runner?.yieldCanvas();
    pinned = true;
    editor.setHeat(load, { banner: { html: html`Showing: <strong>${label}</strong>`, action: 'Back to live view' } });
  }

  function backToLive() {
    pinned = false;
    if (overlayHeld) return runner?.repaint();
    if (preview && previewEnabled) editor.setOverlay(preview);
    else editor.clearOverlay();
  }
  editor.addEventListener('banner-action', backToLive);

  // ---------------------------------------------------------------- toolbar
  let modeActions = [];
  let readOnly = false;

  function runButtons() {
    if (!runner || !previewEnabled) return '';
    const stop = html`<button class="btn btn-sm" type="button" data-tool="run-stop" title="Stop and see how it went">${raw(ICONS.stop)}Stop</button>`;
    if (runState === 'live') return html`<button class="btn btn-sm btn-success" type="button" data-tool="run-pause" title="Pause the live simulation">${raw(ICONS.pause)}Pause</button>${stop}`;
    if (runState === 'paused') return html`<button class="btn btn-sm btn-success" type="button" data-tool="run-resume" title="Carry on from here">${raw(ICONS.play)}Resume</button>${stop}`;
    return html`<button class="btn btn-sm btn-success" type="button" data-tool="run-start" title="Send live traffic through your design. Change traffic, break parts and add capacity while it runs.">${raw(ICONS.play)}Run simulation</button>`;
  }

  function renderToolbar(title = '') {
    const tb = $('#ws-toolbar');
    setHtml(tb, html`
    <button class="tool panel-toggle" type="button" data-tool="left" title="Show the brief">${raw(ICONS.panel)}<span>Brief</span></button>
    <span class="ws-title" id="ws-title">${title}</span>
    <span class="sep"></span>
    ${readOnly ? '' : html`
    <button class="tool" type="button" data-tool="undo" title="Undo (Ctrl/⌘ Z)" disabled>${raw(ICONS.undo)}</button>
    <button class="tool" type="button" data-tool="redo" title="Redo (Ctrl/⌘ ⇧ Z)" disabled>${raw(ICONS.redo)}</button>
    <button class="tool" type="button" data-tool="arrange" title="Tidy up the layout">${raw(ICONS.arrange)}<span>Tidy</span></button>`}
    <button class="tool" type="button" data-tool="export" title="Download the design as a file">${raw(ICONS.export)}</button>
    ${readOnly ? '' : html`
    <button class="tool" type="button" data-tool="import" title="Open a design file">${raw(ICONS.import)}</button>
    <button class="tool" type="button" data-tool="clear" title="Clear the canvas">${raw(ICONS.clear)}</button>`}
    <span class="grow"></span>
    <span class="save-state" id="save-state"></span>
    ${runButtons()}
    ${modeActions.map((a) => html`<button class="btn btn-sm ${a.primary ? 'btn-primary' : ''}" type="button" data-action="${a.id}" title="${a.title || ''}">${a.icon ? raw(ICONS[a.icon]) : ''}${a.label}</button>`)}
    <button class="tool panel-toggle" type="button" data-tool="right" title="Show details">${raw(ICONS.panel)}<span>Details</span></button>`);
  }

  $('#ws-toolbar').addEventListener('click', async (e) => {
    const tool = e.target.closest('[data-tool]')?.dataset.tool;
    const action = e.target.closest('[data-action]')?.dataset.action;
    if (action) {
      const a = modeActions.find((x) => x.id === action);
      if (a) a.onClick(e.target.closest('button'));
      return;
    }
    if (!tool) return;
    if (tool === 'run-start' || tool === 'run-resume') runner?.start();
    if (tool === 'run-pause') runner?.pause();
    if (tool === 'run-stop') runner?.stop();
    if (tool === 'undo') editor.undo();
    if (tool === 'redo') editor.redo();
    if (tool === 'arrange') editor.autoArrange();
    if (tool === 'left') { ws.classList.toggle('show-left'); ws.classList.remove('show-right'); }
    if (tool === 'right') { ws.classList.toggle('show-right'); ws.classList.remove('show-left'); }
    if (tool === 'export') exportDesign();
    if (tool === 'import') importDesign();
    if (tool === 'clear') {
      const ok = await dialog({ title: 'Clear the canvas?', body: html`<p>This removes every part except Users, and all arrows. You can undo it.</p>`, actions: [{ label: 'Cancel', value: false }, { label: 'Clear', value: true, danger: true }] });
      if (ok) {
        editor.snapshot();
        editor.nodes = editor.nodes.filter((n) => n.type === 'User');
        editor.edges = [];
        editor.select(null);
        editor.changed('remove');
      }
    }
  });

  editor.addEventListener('history', (e) => {
    const u = $('[data-tool="undo"]');
    const r = $('[data-tool="redo"]');
    if (u) u.disabled = !e.detail.canUndo;
    if (r) r.disabled = !e.detail.canRedo;
  });

  function exportDesign() {
    const blob = new Blob([JSON.stringify({ app: 'kafka-lab', mode, graph: editor.toGraph() }, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `kafka-lab-${mode}${data.id ? `-${data.id}` : ''}.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  }

  function importDesign() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'application/json,.json';
    input.onchange = async () => {
      const file = input.files?.[0];
      if (!file) return;
      try {
        const parsed = JSON.parse(await file.text());
        const { graph, issues } = normalizeGraph(parsed.graph || parsed);
        if (!graph.nodes.length) throw new Error('That file has no components in it.');
        editor.load(graph);
        toast(issues.length ? `Opened with ${issues.length} problem(s) skipped.` : 'Design opened.', issues.length ? 'warn' : 'ok');
      } catch (err) {
        toast(`Could not open that file: ${errorText(err)}`, 'bad');
      }
    };
    input.click();
  }

  // ---------------------------------------------------------------- saving
  const saveLocal = debounce(() => {
    if (!autosave) return;
    try {
      localStorage.setItem(saveKey, JSON.stringify(editor.toGraph()));
      const s = $('#save-state');
      if (s) s.textContent = 'Draft kept in this browser';
    } catch { /* storage full or blocked */ }
  }, 500);

  editor.addEventListener('change', () => {
    if (pinned) backToLive();
    previewSoon();
    saveLocal();
  });
  editor.addEventListener('notice', (e) => toast(e.detail.message, 'warn'));
  editor.addEventListener('edit', () => {
    showTab('inspect');
    setTimeout(() => $('#f-label')?.focus(), 30);
  });
  editor.addEventListener('select', (e) => {
    if (e.detail.sel) showTab('inspect', { open: ws.classList.contains('show-right') });
  });

  const inspector = new Inspector($('#panel-inspect'), editor, { metrics: () => (pinned ? null : preview) });

  // ---------------------------------------------------------------- context for modes
  const ctx = {
    mode, id: data.id, editor, inspector,
    left: $('#ws-left'),
    results: $('#panel-results'),
    hintsPanel: $('#panel-hints'),
    roundPanel: $('#round-panel'),
    showTab, markTab, showSim, showHeat, backToLive,
    graph: graphNow,
    preview: () => preview,
    traffic: () => traffic,
    overrides: () => overrides,
    // The live run owns the canvas colours while it holds them; the normal preview waits.
    holdOverlay(on) {
      overlayHeld = !!on;
      if (overlayHeld) {
        pinned = false;
        editor.setBanner(null);
      } else {
        editor.setParticles(null);
        backToLive();
      }
    },
    setRunState(state) {
      runState = state;
      ctx.setActions(modeActions);
    },
    setRunInfo(info) {
      runner?.setInfo(info);
    },
    setTitle(text) {
      const t = $('#ws-title');
      if (t) t.textContent = text;
    },
    setActions(actions) {
      modeActions = actions;
      const title = $('#ws-title')?.textContent || '';
      renderToolbar(title);
      editor.emit('history', { canUndo: editor.history.length > 0, canRedo: editor.future.length > 0 });
    },
    setReadOnly(on) {
      readOnly = !!on;
      editor.setReadOnly(readOnly);
      inspector.setReadOnly(readOnly);
      $('#palette').inert = readOnly;
      ctx.setActions(modeActions);
    },
    setTraffic(t, o = {}) {
      traffic = t || {};
      overrides = o || {};
      runner?.syncTraffic();
      previewSoon();
    },
    setPreviewEnabled(on) {
      previewEnabled = on;
      $('.tab[data-tab="run"]')?.classList.toggle('hidden', !on);
      if (!on) {
        runner?.reset();
        preview = null;
        editor.clearOverlay();
      } else previewSoon();
      ctx.setActions(modeActions);
    },
    setHintsVisible(on) {
      $('.tab[data-tab="hints"]').classList.toggle('hidden', !on);
    },
    setAutosave(on) {
      autosave = on;
    },
    clearLocal() {
      localStorage.removeItem(saveKey);
    },
    restoreOr(fallback) {
      let graph = fallback;
      try {
        const saved = JSON.parse(localStorage.getItem(saveKey) || 'null');
        if (saved?.nodes?.length) graph = saved;
      } catch { /* ignore broken saves */ }
      editor.load(graph);
      return graph !== fallback;
    },
  };

  if ($('#panel-run')) runner = new RunPanel(ctx, { panel: $('#panel-run'), host: $('#canvas') });

  buildPalette();
  renderToolbar();

  try {
    const MODES = { practice: initPracticeMode, learn: initLearnMode, build: initBuildMode, fix: initFixMode, interview: initInterviewMode };
    await MODES[mode](ctx);
  } catch (err) {
    console.error(err);
    setHtml($('#ws-left'), html`<div class="section-card"><strong>Could not load this page.</strong><p class="muted small">${errorText(err)}</p><a class="btn btn-sm" href="/">Go home</a></div>`);
  }
  runPreview();
}

// ================================================================ pages: lists
// Filters on the problem and incident lists.
function initLists() {
  let level = 'All';
  let query = '';

  function applyFilters() {
    for (const card of $$('#list .card')) {
      const okLevel = level === 'All' || (level === 'Kafka' ? card.dataset.track === 'kafka' : card.dataset.level.toLowerCase() === level.toLowerCase());
      const okQuery = !query || card.dataset.title.includes(query);
      card.classList.toggle('hidden', !(okLevel && okQuery));
    }
  }

  $('#filters')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.filter');
    if (!btn) return;
    level = btn.dataset.level;
    $$('.filter').forEach((b) => b.classList.toggle('active', b === btn));
    applyFilters();
  });
  $('#search')?.addEventListener('input', (e) => {
    query = e.target.value.trim().toLowerCase();
    applyFilters();
  });
}

// ================================================================ pages: topic
// The quiz on a roadmap topic page.
function initTopic() {
  const { topic: topicId } = pageData();
  const body = $('#quiz-body');

  async function load() {
    const topic = await api(`/api/roadmap/${encodeURIComponent(topicId)}`);
    setHtml(body, html`<form id="quiz-form">
    ${topic.quiz.map((q, qi) => html`<div class="quiz-q"><p>${qi + 1}. ${q.q}</p>
      ${q.options.map((o, oi) => html`<label class="option" data-q="${qi}" data-o="${oi}"><input type="radio" name="q${qi}" value="${oi}"><span>${o}</span></label>`)}
      <div class="explain hidden" data-explain="${qi}"></div></div>`)}
    <div style="display:flex;gap:10px;align-items:center"><button class="btn btn-primary" type="submit">Check my answers</button><span class="muted small" id="quiz-result"></span></div>
  </form>`);
    const form = $('#quiz-form');
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const answers = topic.quiz.map((_, i) => {
        const checked = form.querySelector(`input[name="q${i}"]:checked`);
        return checked ? Number(checked.value) : -1;
      });
      if (answers.includes(-1)) return toast('Answer every question first.', 'warn');
      try {
        const res = await api(`/api/roadmap/${encodeURIComponent(topicId)}/quiz`, { method: 'POST', body: { answers } });
        res.results.forEach((r, i) => {
          form.querySelectorAll(`[data-q="${i}"]`).forEach((opt) => {
            const o = Number(opt.dataset.o);
            opt.classList.remove('correct', 'wrong');
            if (o === r.answer) opt.classList.add('correct');
            else if (o === answers[i]) opt.classList.add('wrong');
          });
          const ex = form.querySelector(`[data-explain="${i}"]`);
          ex.textContent = r.explain;
          ex.classList.remove('hidden');
        });
        $('#quiz-result').textContent = res.passed ? `${res.score} of ${res.total}. Nice, topic passed!` : `${res.score} of ${res.total}. Get 2 right to pass. Try again.`;
        form.querySelector('button[type="submit"]').textContent = 'Check again';
      } catch (err) {
        toast(errorText(err), 'bad');
      }
    });
  }

  load().catch((err) => setHtml(body, html`<p class="bad">${errorText(err)}</p>`));
}

// ================================================================ pages: report
// The mock interview report.
function initReport() {
  const { id } = pageData();
  const root = $('#report');

  const cls = (s) => (s >= 75 ? 'good' : s >= 55 ? 'warn' : 'bad');
  const listOr = (items, itemClass, empty) => (items.length
    ? html`<ul>${items.map((t) => html`<li class="${itemClass}">${t}</li>`)}</ul>`
    : html`<p class="muted small">${empty}</p>`);

  function ring(score) {
    const r = 62;
    const c = 2 * Math.PI * r;
    return html`<svg class="score-ring" viewBox="0 0 150 150" role="img" aria-label="Overall score ${score} out of 100">
    <circle class="track" cx="75" cy="75" r="${r}"/>
    <circle class="arc" cx="75" cy="75" r="${r}" stroke-dasharray="${c.toFixed(1)}" stroke-dashoffset="${(c * (1 - score / 100)).toFixed(1)}"/>
    <text x="75" y="87">${score}</text>
  </svg>`;
  }

  function show(rep) {
    const r = rep.rounds;
    setHtml(root, html`
    <div class="report-head">
      ${ring(rep.overall)}
      <div>
        <p class="eyebrow">Interview report</p>
        <h1 style="margin-bottom:6px">${rep.readiness}</h1>
        <p class="lead" style="margin-bottom:6px">${rep.problem.title} · <span class="diff diff-${rep.problem.difficulty.toLowerCase()}">${rep.problem.difficulty}</span></p>
        <p class="muted small">Took ${rep.elapsed_min} of ${rep.duration_min} minutes${rep.over_time ? ' (over time)' : ''} · ${fmtDateTime(rep.finished_at)}</p>
      </div>
    </div>

    <div class="split">
      <div class="card">
        <h2 class="card-title">Your five scores</h2>
        ${rep.dimensions.map((d) => html`<div class="dim"><div class="dim-head"><span>${d.label}</span><span class="${cls(d.score)}">${d.score}</span></div><div class="bar"><span style="width:${d.score}%"></span></div><p>${d.description} Weight ${Math.round(d.weight * 100)}%.</p></div>`)}
      </div>
      <div class="card">
        <h2 class="card-title">Strong points</h2>
        ${rep.strengths.length ? html`<ul class="checks">${rep.strengths.map((s) => html`<li>${s}</li>`)}</ul>` : html`<p class="empty">None above 75 yet.</p>`}
        <h2 class="card-title" style="margin-top:18px">What to work on</h2>
        <ul>${rep.improvements.map((s) => html`<li>${s}</li>`)}</ul>
        <div class="insp-actions"><a class="btn btn-primary btn-sm" href="/interview">Start another interview</a><a class="btn btn-sm" href="/practice/${rep.problem.id}">Practice this problem</a></div>
      </div>
    </div>

    ${r.requirements ? html`<div class="card" style="margin-top:24px">
      <h2 class="card-title">Round 1: Requirements</h2>
      <p>What it must do: <strong class="${cls(r.requirements.functional.score)}">${r.requirements.functional.score}</strong> · How well it must work: <strong class="${cls(r.requirements.non_functional.score)}">${r.requirements.non_functional.score}</strong></p>
      ${r.requirements.feedback ? html`<p>${r.requirements.feedback}</p>` : ''}
      <div class="split"><div><h3>Covered</h3>${listOr([...r.requirements.functional.covered, ...r.requirements.non_functional.covered], 'covered', 'Nothing yet.')}</div>
      <div><h3>Missed</h3>${listOr([...r.requirements.functional.missed, ...r.requirements.non_functional.missed], 'missed', 'Nothing missed.')}</div></div>
    </div>` : ''}

    ${r.design ? html`<div class="card" style="margin-top:24px">
      <h2 class="card-title">Round 2: Design</h2>
      <p>Passed <strong>${r.design.passed} of ${r.design.total}</strong> tests · test score ${r.design.test_score} · design review ${r.design.rule_score} · security ${r.design.security_score}</p>
      <div class="report-design" id="report-design"></div>
      ${r.design.failing_tests.length ? html`<h3>Failed tests</h3><ul>${r.design.failing_tests.map((t) => html`<li>${t.name}: ${fmtPct(t.error_rate, 1)} errors (score ${t.score})</li>`)}</ul>` : ''}
      ${r.design.missing_components.length ? html`<h3>Parts you could add</h3><p>${r.design.missing_components.join(', ')}</p>` : ''}
      ${r.design.top_findings.length ? html`<h3>Review notes</h3><ul>${r.design.top_findings.map((f) => html`<li><strong>${f.title}.</strong> ${f.recommendation}</li>`)}</ul>` : ''}
    </div>` : ''}

    ${r.mcqs ? html`<div class="card" style="margin-top:24px">
      <h2 class="card-title">Round 3: Questions <span class="muted small">${r.mcqs.correct} of ${r.mcqs.total} correct</span></h2>
      ${r.mcqs.items.map((q, i) => html`<div class="review-q"><p><strong>${i + 1}. ${q.q}</strong></p>
        <p class="${q.chosen === q.answer ? 'covered' : 'missed'}">${q.chosen === q.answer ? 'Correct' : `Your answer: ${q.options[q.chosen] ?? 'blank'}`}</p>
        ${q.chosen !== q.answer ? html`<p class="covered">Right answer: ${q.options[q.answer]}</p>` : ''}
        <div class="explain">${q.explain}</div></div>`)}
    </div>` : ''}

    ${r.tradeoffs ? html`<div class="card" style="margin-top:24px">
      <h2 class="card-title">Round 4: Trade-offs</h2>
      ${r.tradeoffs.map((t, i) => html`<div class="review-q">
        <p><strong>${i + 1}. ${t.question}</strong> <span class="${cls(t.score * 10)}">${t.score}/10</span></p>
        <div class="answer-box">${t.answer || '(no answer)'}</div>
        ${t.feedback ? html`<p>${t.feedback}</p>` : ''}
        ${t.covered.length ? html`<p class="covered">Covered: ${t.covered.join(', ')}</p>` : ''}
        ${t.missed.length ? html`<p class="missed">Missing: ${t.missed.join(', ')}</p>` : ''}
        <details><summary>A strong answer</summary><p>${t.model_answer}</p></details>
      </div>`)}
    </div>` : ''}`);

    const host = document.getElementById('report-design');
    if (host && r.design?.graph) {
      const viewer = new Editor(host, { readOnly: true });
      viewer.load(r.design.graph);
    }
  }

  const report = loadInterview(id)?.report;
  if (report) show(report);
  else setHtml(root, html`<h1>Report not found</h1><p class="lead">This report is not saved in this browser. Only your latest interview is kept, and only in the browser where you took it.</p><a class="btn btn-primary" href="/interview">Back to mock interviews</a>`);
}

// ================================================================ pages: interview home
// The mock interview start page.
function initInterviewHome() {
  const current = loadInterview();
  const problemTitle = (id) => $(`#iv-problem option[value="${CSS.escape(id)}"]`)?.textContent.replace(/\s*\([^)]*\)$/, '') || id;

  if (current) {
    const rep = current.report;
    setHtml($('#iv-current'), rep
      ? html`<ul class="history"><li><a href="/evaluations/${current.id}">
        <span>${rep.problem.title}<br><span class="muted small">${fmtDateTime(rep.finished_at)} · ${rep.readiness}</span></span>
        <strong class="${rep.overall >= 65 ? 'good' : 'warn'}">${rep.overall}</strong></a></li></ul>`
      : html`<ul class="history"><li><a href="/interview/${current.id}">
        <span>${problemTitle(current.problem_id)}<br><span class="muted small">Started ${fmtDateTime(current.started_at)} · round ${Math.min(current.round, 4)} of 4</span></span>
        <strong>Continue →</strong></a></li></ul>`);
  }

  $('#start-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    if (current && !current.report) {
      const ok = await dialog({
        title: 'Start a new interview?',
        body: html`<p>Your unfinished interview will be lost.</p>`,
        actions: [{ label: 'Cancel', value: false }, { label: 'Start new', value: true, danger: true }],
      });
      if (!ok) return;
    }
    const session = startInterview($('#iv-problem').value, Number($('#iv-duration').value));
    location.href = `/interview/${session.id}`;
  });
}

// ================================================================ start
const PAGES = { lists: initLists, topic: initTopic, report: initReport, 'interview-home': initInterviewHome, workspace: initWorkspace };

setupBackdrop();
setupNav();
PAGES[document.body.dataset.page]?.();
