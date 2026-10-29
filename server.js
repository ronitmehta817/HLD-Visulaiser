// Kafka Lab server: HTTP helpers, AI grading, HTML views, API routes and startup. Run with `node server.js`.

import { readFile, stat } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  checkIncident, debriefView, getIncident, getProblem, getTopic, INCIDENTS, incidentSummary, PHASES, pickMcqs, pickTradeoffs, PROBLEMS, problemSummary, publicIncident, publicProblem, publicTopic, quizView, roadmapOverview, TOPICS, TRACKS,
} from './content.js';
import {
  analyzeDesign, autoLayout, combineSubmission, describeChanges, failureOverrides, normalizeGraph, problemTraffic, publicCatalog, runStressTest, runSuite, scoreSimulation, simulate, stressCatalog,
} from './public/engine.js';

// ================================================================ http
class HttpError extends Error {
  constructor(status, message, extra = {}) {
    super(message);
    this.status = status;
    this.extra = extra;
  }
}

class Router {
  constructor() {
    this.routes = [];
  }

  add(method, pattern, handler) {
    const keys = [];
    const re = new RegExp(`^${pattern.replace(/:(\w+)/g, (_, k) => { keys.push(k); return '([^/]+)'; })}/?$`);
    this.routes.push({ method, re, keys, handler });
  }

  get(p, h) { this.add('GET', p, h); }
  post(p, h) { this.add('POST', p, h); }
  put(p, h) { this.add('PUT', p, h); }
  delete(p, h) { this.add('DELETE', p, h); }

  match(method, pathname) {
    let otherMethod = false;
    for (const r of this.routes) {
      const m = r.re.exec(pathname);
      if (!m) continue;
      if (r.method !== method && !(method === 'HEAD' && r.method === 'GET')) {
        otherMethod = true;
        continue;
      }
      let params;
      try {
        params = Object.fromEntries(r.keys.map((k, i) => [k, decodeURIComponent(m[i + 1])]));
      } catch {
        return null;
      }
      return { handler: r.handler, params };
    }
    return otherMethod ? { methodNotAllowed: true } : null;
  }
}

const SECURITY_HEADERS = {
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'same-origin',
  'X-Frame-Options': 'DENY',
  'Content-Security-Policy': `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; img-src 'self' data:; connect-src 'self'; font-src 'self' https://fonts.gstatic.com; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'`,
};

function sendJson(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, { ...SECURITY_HEADERS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': Buffer.byteLength(data) });
  res.end(data);
}

function sendHtml(res, status, html) {
  res.writeHead(status, { ...SECURITY_HEADERS, 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'Content-Length': Buffer.byteLength(html) });
  res.end(html);
}

function readJson(req, limit = 2_000_000) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new HttpError(413, 'Request body is too large.'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        resolve(parsed && typeof parsed === 'object' ? parsed : {});
      } catch {
        reject(new HttpError(400, 'Request body must be valid JSON.'));
      }
    });
    req.on('error', reject);
  });
}

const MIME_TYPES = {
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.json': 'application/json; charset=utf-8',
  '.woff2': 'font/woff2',
};

async function serveStatic(req, res, root, relPath) {
  const full = path.normalize(path.join(root, relPath));
  if (!full.startsWith(root + path.sep)) return false;
  const type = MIME_TYPES[path.extname(full)];
  if (!type) return false;
  let info;
  try {
    info = await stat(full);
  } catch {
    return false;
  }
  if (!info.isFile()) return false;
  const etag = `"${info.size.toString(16)}-${Math.floor(info.mtimeMs).toString(16)}"`;
  const headers = { 'X-Content-Type-Options': 'nosniff', 'Content-Type': type, 'Cache-Control': 'no-cache', ETag: etag };
  if (req.headers['if-none-match'] === etag) {
    res.writeHead(304, headers);
    res.end();
    return true;
  }
  const data = await readFile(full);
  res.writeHead(200, { ...headers, 'Content-Length': data.length });
  res.end(req.method === 'HEAD' ? undefined : data);
  return true;
}

const htmlResponse = (markup, status = 200) => ({ __html: markup, status });

// ================================================================ ai
// Interview grading. A deterministic keyword rubric always runs. When a model is configured,
// either OPENAI_API_KEY for OpenAI or OPENAI_BASE_URL for any OpenAI-compatible server (local
// ones often need no key), it adds written feedback and its own scores. Any model failure
// falls back to the rubric.

const normalize = (text) => String(text || '').toLowerCase().replace(/\s+/g, ' ');
const wordCount = (text) => normalize(text).split(' ').filter(Boolean).length;
const QUANTIFIED = /\d[\d,.]*\s*(ms|milliseconds?|s\b|seconds?|rps|qps|tps|requests?|users?|%|gb|tb|mb|k\b|m\b|million|thousand|nines?)/i;
const TRADEOFF_WORDS = /\b(trade-?offs?|cost|costs|but|however|downside|versus|vs\.?|at the expense|in exchange)\b/;

function coverage(items, text) {
  const t = normalize(text);
  return items.map((item) => ({ text: item.text, covered: item.keywords.some((k) => t.includes(k.toLowerCase())) }));
}

function gradeRequirements(problem, functional, nonFunctional) {
  const fr = coverage(problem.functional, functional);
  const nfr = coverage(problem.non_functional, nonFunctional);
  const quantified = QUANTIFIED.test(nonFunctional || '');
  const frScore = Math.round((100 * fr.filter((x) => x.covered).length) / fr.length);
  const nfrScore = Math.round((85 * nfr.filter((x) => x.covered).length) / nfr.length + (quantified ? 15 : 0));
  const tips = [];
  if (wordCount(functional) < 15) tips.push('List each user-facing capability on its own line; interviewers want to see scope agreed before design.');
  if (!quantified) tips.push('Put numbers on non-functional requirements: requests per second, latency targets in ms, availability as nines.');
  const missedFr = fr.filter((x) => !x.covered).map((x) => x.text);
  const missedNfr = nfr.filter((x) => !x.covered).map((x) => x.text);
  return {
    functional: { score: frScore, covered: fr.filter((x) => x.covered).map((x) => x.text), missed: missedFr },
    non_functional: { score: nfrScore, covered: nfr.filter((x) => x.covered).map((x) => x.text), missed: missedNfr, quantified },
    tips,
    feedback: null,
    source: 'rubric',
  };
}

function gradeTradeoff(question, answer) {
  const t = normalize(answer);
  const words = wordCount(answer);
  const hits = question.concepts.filter((c) => c.keywords.some((k) => t.includes(k.toLowerCase())));
  let score = (hits.length / question.concepts.length) * 10;
  if (words < 25) score *= 0.6;
  if (TRADEOFF_WORDS.test(t)) score += 1;
  score = Math.max(0, Math.min(10, Math.round(score * 10) / 10));
  return {
    score,
    covered: hits.map((c) => c.label),
    missed: question.concepts.filter((c) => !hits.includes(c)).map((c) => c.label),
    words,
    model_answer: question.model,
    feedback: null,
    source: 'rubric',
  };
}

function createAi(env = process.env) {
  const apiKey = env.OPENAI_API_KEY || '';
  const baseUrl = (env.OPENAI_BASE_URL || 'https://api.openai.com/v1').replace(/\/$/, '');
  const model = env.OPENAI_MODEL || 'gpt-4o-mini';
  const enabled = !!(apiKey || env.OPENAI_BASE_URL);

  async function chatJson(system, user) {
    const headers = { 'Content-Type': 'application/json' };
    if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
    const res = await fetch(`${baseUrl}/chat/completions`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model,
        temperature: 0.2,
        response_format: { type: 'json_object' },
        messages: [{ role: 'system', content: system }, { role: 'user', content: user }],
      }),
      signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) throw new Error(`LLM request failed with ${res.status}`);
    const data = await res.json();
    return JSON.parse(data.choices?.[0]?.message?.content || '{}');
  }

  async function requirementsFeedback(problem, functional, nonFunctional) {
    const base = gradeRequirements(problem, functional, nonFunctional);
    if (!enabled) return base;
    try {
      const out = await chatJson(
        'You grade the requirements round of a system design interview. Reply with JSON: {"feedback": string (3-5 sentences, direct and specific), "functional_score": 0-100, "non_functional_score": 0-100}.',
        JSON.stringify({
          problem: problem.statement,
          reference_functional: problem.functional.map((r) => r.text),
          reference_non_functional: problem.non_functional.map((r) => r.text),
          candidate_functional: functional,
          candidate_non_functional: nonFunctional,
        }),
      );
      const clamp = (v, fallback) => (Number.isFinite(Number(v)) ? Math.max(0, Math.min(100, Math.round(Number(v)))) : fallback);
      return {
        ...base,
        functional: { ...base.functional, score: clamp(out.functional_score, base.functional.score) },
        non_functional: { ...base.non_functional, score: clamp(out.non_functional_score, base.non_functional.score) },
        feedback: typeof out.feedback === 'string' ? out.feedback : null,
        source: 'llm',
      };
    } catch (err) {
      return { ...base, llm_error: err.message };
    }
  }

  async function tradeoffFeedback(questions, answers) {
    const graded = questions.map((q, i) => gradeTradeoff(q, answers[i] || ''));
    if (!enabled) return graded;
    try {
      const out = await chatJson(
        'You grade trade-off answers in a system design interview. Reply with JSON: {"answers": [{"score": 0-10, "feedback": string (2-3 sentences)}]} in the same order as the questions.',
        JSON.stringify(questions.map((q, i) => ({ question: q.question, reference_answer: q.model, candidate_answer: answers[i] || '' }))),
      );
      return graded.map((g, i) => {
        const a = out.answers?.[i];
        const score = Number(a?.score);
        return {
          ...g,
          score: Number.isFinite(score) ? Math.max(0, Math.min(10, Math.round(score * 10) / 10)) : g.score,
          feedback: typeof a?.feedback === 'string' ? a.feedback : null,
          source: a ? 'llm' : 'rubric',
        };
      });
    } catch (err) {
      return graded.map((g) => ({ ...g, llm_error: err.message }));
    }
  }

  return {
    enabled,
    provider: enabled ? `the built-in checklist plus AI feedback from ${model}` : 'the built-in checklist',
    requirementsFeedback,
    tradeoffFeedback,
  };
}

// ================================================================ views: html
// Tagged template that escapes every interpolation unless it is wrapped in raw() or is
// itself the output of html``. Arrays are joined, null/undefined/false render nothing.

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

const escape = (value) => String(value ?? '').replace(/[&<>"']/g, (c) => ESC[c]);

class Raw {
  constructor(value) { this.value = value; }
  toString() { return this.value; }
}

const raw = (value) => new Raw(String(value));

function render(value) {
  if (value === null || value === undefined || value === false) return '';
  if (value instanceof Raw) return value.value;
  if (Array.isArray(value)) return value.map(render).join('');
  return escape(value);
}

function html(strings, ...values) {
  let out = '';
  strings.forEach((s, i) => {
    out += s;
    if (i < values.length) out += render(values[i]);
  });
  return new Raw(out);
}

// JSON for a <script type="application/json"> block: never lets the payload close the tag.
const jsonBlock = (data) => raw(JSON.stringify(data).replace(/</g, '\\u003c').replace(/>/g, '\\u003e').replace(/&/g, '\\u0026'));

// ================================================================ views: layout
const NAV = [
  { id: 'learn', href: '/learn', label: 'Learn' },
  { id: 'practice', href: '/practice', label: 'Practice' },
  { id: 'build', href: '/build', label: 'Build' },
  { id: 'fix', href: '/fix', label: 'Design & Fix' },
  { id: 'interview', href: '/interview', label: 'Interview' },
  { id: 'roadmap', href: '/roadmap', label: 'Roadmap' },
];

const FAVICON = `data:image/svg+xml,${encodeURIComponent("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 32 32\"><rect width=\"32\" height=\"32\" rx=\"8\" fill=\"#010204\"/><rect x=\"5\" y=\"5\" width=\"9\" height=\"9\" rx=\"2.5\" fill=\"#00ffe0\"/><rect x=\"18\" y=\"5\" width=\"9\" height=\"9\" rx=\"2.5\" fill=\"#ffa800\"/><rect x=\"11.5\" y=\"18\" width=\"9\" height=\"9\" rx=\"2.5\" fill=\"#ecad2c\"/><path d=\"M9.5 14v2.5h13V14M16 16.5V18\" fill=\"none\" stroke=\"#8a8f96\" stroke-width=\"1.5\"/></svg>")}`;

const LOGO = html`<svg class="logo-mark" viewBox="0 0 32 32" aria-hidden="true"><rect x="3" y="3" width="10" height="10" rx="3"/><rect x="19" y="3" width="10" height="10" rx="3" class="lm-2"/><rect x="11" y="19" width="10" height="10" rx="3" class="lm-3"/><path d="M8 13v3h16v-3M16 16v3" fill="none"/></svg>`;

function page({ title, active = '', body, pageId = '', data = null, bodyClass = '', description = 'Learn system design and Apache Kafka: build on a canvas, simulate real traffic and find out what breaks.' }) {
  return String(html`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title ? `${title} · Kafka Lab` : 'Kafka Lab · System Design and Kafka Simulator'}</title>
<meta name="description" content="${description}">
<link rel="icon" href="${FAVICON}" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Manrope:wght@200..800&family=Noto+Serif+Display:ital,wdth,wght@1,75,300..500&display=swap">
<link rel="stylesheet" href="/app.css">
</head>
<body class="${bodyClass}" data-page="${pageId}">
${pageId === 'workspace' ? '' : html`<canvas class="backdrop" id="backdrop" aria-hidden="true"></canvas><div class="grain" aria-hidden="true"></div>`}
<header class="topbar">
  <a class="brand" href="/" aria-label="Kafka Lab home">${LOGO}<span>Kafka Lab</span></a>
  <button class="icon-btn nav-toggle" id="nav-toggle" type="button" aria-label="Open menu" aria-expanded="false">
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16"/></svg>
  </button>
  <nav class="nav" id="main-nav" aria-label="Main">
    ${NAV.map((n) => html`<a href="${n.href}" class="${n.id === active ? 'active' : ''}"${n.id === active ? html` aria-current="page"` : ''}>${n.label}</a>`)}
  </nav>
</header>
<main id="main">${body}</main>
${data ? html`<script type="application/json" id="page-data">${jsonBlock(data)}</script>` : ''}
<script type="module" src="/app.js"></script>
</body>
</html>`);
}

// ================================================================ views: pages
const kafkaCount = (items) => items.filter((x) => x.track === 'kafka').length;

const diffClass = (d) => `diff diff-${String(d).toLowerCase()}`;

const HERO_SVG = html`
<svg class="hero-diagram" viewBox="0 0 560 330" role="img" aria-label="Animated diagram of requests flowing through a load balancer to servers, a cache and a database">
  <defs>
    <path id="hp-1" d="M70 165 C120 165 130 165 170 165"/>
    <path id="hp-2a" d="M230 165 C265 165 270 90 305 90"/>
    <path id="hp-2b" d="M230 165 C265 165 270 165 305 165"/>
    <path id="hp-2c" d="M230 165 C265 165 270 240 305 240"/>
    <path id="hp-3a" d="M365 90 C400 90 405 110 440 110"/>
    <path id="hp-3b" d="M365 165 C400 165 405 110 440 110"/>
    <path id="hp-3c" d="M365 240 C400 240 405 220 440 220"/>
  </defs>
  <g class="hd-links">
    <use href="#hp-1"/><use href="#hp-2a"/><use href="#hp-2b"/><use href="#hp-2c"/><use href="#hp-3a"/><use href="#hp-3b"/><use href="#hp-3c"/>
    <path d="M365 165 C400 165 405 220 440 220"/>
  </g>
  <g class="hd-packets">
    <circle r="4"><animateMotion dur="1.2s" repeatCount="indefinite"><mpath href="#hp-1"/></animateMotion></circle>
    <circle r="4"><animateMotion dur="1.2s" begin="-0.4s" repeatCount="indefinite"><mpath href="#hp-1"/></animateMotion></circle>
    <circle r="4"><animateMotion dur="1.2s" begin="-0.8s" repeatCount="indefinite"><mpath href="#hp-1"/></animateMotion></circle>
    <circle r="3.5"><animateMotion dur="1.4s" repeatCount="indefinite"><mpath href="#hp-2a"/></animateMotion></circle>
    <circle r="3.5"><animateMotion dur="1.4s" begin="-0.45s" repeatCount="indefinite"><mpath href="#hp-2b"/></animateMotion></circle>
    <circle r="3.5"><animateMotion dur="1.4s" begin="-0.9s" repeatCount="indefinite"><mpath href="#hp-2c"/></animateMotion></circle>
    <circle r="3"><animateMotion dur="1.6s" begin="-0.2s" repeatCount="indefinite"><mpath href="#hp-3a"/></animateMotion></circle>
    <circle r="3"><animateMotion dur="1.6s" begin="-0.7s" repeatCount="indefinite"><mpath href="#hp-3b"/></animateMotion></circle>
    <circle r="3" class="hd-hot"><animateMotion dur="0.9s" repeatCount="indefinite"><mpath href="#hp-3c"/></animateMotion></circle>
    <circle r="3" class="hd-hot"><animateMotion dur="0.9s" begin="-0.3s" repeatCount="indefinite"><mpath href="#hp-3c"/></animateMotion></circle>
    <circle r="3" class="hd-hot"><animateMotion dur="0.9s" begin="-0.6s" repeatCount="indefinite"><mpath href="#hp-3c"/></animateMotion></circle>
  </g>
  <g class="hd-nodes">
    <g transform="translate(14 140)"><rect width="56" height="50" rx="10"/><text x="28" y="30">Users</text></g>
    <g transform="translate(170 140)"><rect width="60" height="50" rx="10"/><text x="30" y="30">LB</text></g>
    <g transform="translate(305 68)"><rect width="60" height="44" rx="10"/><text x="30" y="27">App</text></g>
    <g transform="translate(305 143)"><rect width="60" height="44" rx="10"/><text x="30" y="27">App</text></g>
    <g transform="translate(305 218)"><rect width="60" height="44" rx="10"/><text x="30" y="27">App</text></g>
    <g transform="translate(440 88)" class="hd-node-data"><rect width="100" height="44" rx="10"/><text x="50" y="27">Cache 94%</text></g>
    <g transform="translate(440 198)" class="hd-node-hot"><rect width="100" height="44" rx="10"/><text x="50" y="27">DB 118%</text></g>
  </g>
  <g class="hd-callout" transform="translate(392 270)">
    <rect width="160" height="44" rx="10"/>
    <text x="12" y="19" class="hd-c1">Bottleneck found</text>
    <text x="12" y="35" class="hd-c2">writes exceed the primary</text>
  </g>
</svg>`;

function landing() {
  const modes = [
    { href: '/learn', title: 'Learn', text: 'See how real systems grow, one step at a time, and why each part was added. Kafka systems included.', tag: `${PROBLEMS.length} systems · ${kafkaCount(PROBLEMS)} Kafka` },
    { href: '/practice', title: 'Practice', text: 'Read a short brief, build the system, then test it with normal traffic, spikes, failures and bad messages.', tag: 'Tests included' },
    { href: '/build', title: 'Build & stress test', text: 'Build anything you like and push traffic from half to ten times normal. Lose a broker, send a poison message.', tag: 'Load ramp' },
    { href: '/fix', title: 'Design & Fix', text: 'A live system is broken: consumer lag, lost messages, rebalance storms. Find the cause and fix it.', tag: `${INCIDENTS.length} incidents · ${kafkaCount(INCIDENTS)} Kafka` },
    { href: '/interview', title: 'Mock interview', text: 'Four timed rounds like a real interview, then a report with your scores.', tag: '4 rounds' },
    { href: '/roadmap', title: 'Roadmap', text: `${TOPICS.length} short lessons: system design first, then everything about Kafka. Each has a quiz and a hands-on exercise.`, tag: `${TOPICS.length} lessons` },
  ];
  const failures = [
    { title: 'Single point of failure', text: 'Only one copy of something. When it stops, everything behind it stops too.' },
    { title: 'Database overload', text: 'More queries arrive than the database can answer, so requests wait and then fail.' },
    { title: 'Consumer lag', text: 'Consumers read slower than producers write. Data gets older, and past the retention it is deleted unread.' },
    { title: 'Hot partition', text: 'One busy key sends a big share of messages to one partition and one consumer.' },
    { title: 'Lost on broker failure', text: 'One copy, or acks=1, and a broker crash takes confirmed messages with it.' },
    { title: 'Poison message', text: 'One message that always fails blocks its partition until something moves it aside.' },
  ];
  const signals = [
    ['Scope', 'Did you agree what the system must do?'],
    ['Requirements', 'Did you put numbers on speed, traffic and uptime?'],
    ['Components', 'Did you pick the right parts?'],
    ['Architecture', 'Does the design survive heavy traffic and failures?'],
    ['Trade-offs', 'Can you explain other options and their costs?'],
  ];
  const body = html`
<section class="hero">
  <div class="container hero-inner">
    <div class="hero-copy">
      <p class="eyebrow">System design and Kafka simulator</p>
      <h1>Build it. Load it.<br>See where it <span class="accent">breaks.</span></h1>
      <p class="lead">Build a system from parts like servers, caches, databases and Kafka topics. Send pretend traffic through it. See which part breaks first, and why. Then fix it, just like in a system design interview.</p>
      <div class="hero-actions">
        <a class="btn btn-primary btn-lg" href="/practice/url_shortener">Try your first design</a>
        <a class="btn btn-ghost btn-lg" href="/learn/clickstream_analytics">Start with Kafka</a>
        <a class="btn btn-ghost btn-lg" href="/roadmap">Start learning</a>
      </div>
      <p class="muted small">Runs on your computer. No sign-in, no account, nothing to set up.</p>
    </div>
    <div class="hero-visual">${HERO_SVG}</div>
  </div>
</section>
<section class="section">
  <div class="container">
    <h2 class="section-title">Six ways to <span class="accent">practise</span></h2>
    <div class="grid grid-3">
      ${modes.map((m) => html`<a class="card card-link mode-card" href="${m.href}"><span class="pill">${m.tag}</span><h3>${m.title}</h3><p>${m.text}</p></a>`)}
    </div>
  </div>
</section>
<section class="section section-alt">
  <div class="container split">
    <div>
      <h2 class="section-title">How the simulator <span class="accent">works</span></h2>
      <p class="lead small-lead">Each part can handle a certain number of requests per second. Traffic flows along your arrows. Caches and CDNs answer some requests early. When a part gets more than it can handle, requests fail and everything slows down.</p>
      <ul class="checks">
        <li>Database copies help with reads. Only splitting the data (sharding) helps with writes.</li>
        <li>Retries add load. Timeouts stop long waits. Circuit breakers fail fast.</li>
        <li>Queues soak up bursts, until the workers fall too far behind.</li>
        <li>Kafka topics keep every message for days; a group can only use as many consumers as there are partitions.</li>
        <li>Failure tests switch parts off to see if the rest keeps working, and check that no message is lost or processed twice.</li>
      </ul>
    </div>
    <div class="grid grid-2 failure-grid">
      ${failures.map((f) => html`<div class="card failure-card"><h4>${f.title}</h4><p>${f.text}</p></div>`)}
    </div>
  </div>
</section>
<section class="section">
  <div class="container">
    <h2 class="section-title">Five interview <span class="accent">signals</span></h2>
    <p class="lead small-lead center">Mock interviews score you on what interviewers actually look for.</p>
    <div class="signals">
      ${signals.map(([t, d], i) => html`<div class="signal"><span class="signal-n">${i + 1}</span><strong>${t}</strong><span>${d}</span></div>`)}
    </div>
  </div>
</section>
<section class="section cta">
  <div class="container center">
    <h2>Your system is only as strong as its <span class="accent">busiest component.</span></h2>
    <p class="lead">Find it before your users do.</p>
    <div class="hero-actions center"><a class="btn btn-primary btn-lg" href="/practice">Browse problems</a><a class="btn btn-ghost btn-lg" href="/fix">Fix an incident</a></div>
  </div>
</section>
<footer class="footer"><div class="container footer-inner"><span>Kafka Lab · runs locally · built for learning</span><span><a href="/learn">Learn</a> · <a href="/roadmap">Roadmap</a></span></div></footer>`;
  return page({ title: '', active: '', body, pageId: 'landing' });
}

function problemList(mode) {
  const isLearn = mode === 'learn';
  const levels = ['All', 'Beginner', 'Intermediate', 'Advanced', 'Kafka'];
  const body = html`
<div class="container page">
  <div class="page-head">
    <div>
      <p class="eyebrow">${isLearn ? 'Learn' : 'Practice'}</p>
      <h1>${isLearn ? html`See how real systems <span class="accent">grow</span>` : html`Build a system, then <span class="accent">test it</span>`}</h1>
      <p class="lead">${isLearn
        ? 'Each system starts as one server and grows step by step. Open any step, run the tests, and read why the next part was added.'
        : 'Read the brief and build your design on the canvas. Run tests checks normal traffic, a spike and one failure. Submit runs bigger tests and gives you a score.'}</p>
    </div>
  </div>
  <div class="filters" id="filters">
    ${levels.map((l, i) => html`<button type="button" class="filter${i === 0 ? ' active' : ''}" data-level="${l}">${l}</button>`)}
    <input class="input search" id="search" type="search" placeholder="Search problems" aria-label="Search problems">
  </div>
  <div class="grid grid-3" id="list">
    ${PROBLEMS.map((p) => html`
    <a class="card card-link problem-card" href="/${mode}/${p.id}" data-level="${p.difficulty}" data-track="${p.track}" data-title="${p.title.toLowerCase()} ${p.category.toLowerCase()}">
      <div class="card-top"><span class="${diffClass(p.difficulty)}">${p.difficulty}</span><span class="muted small">${p.category}</span></div>
      <h3>${p.title}</h3>
      <p>${p.summary}</p>
      <div class="card-meta"><span>${p.scale.peak_rps.toLocaleString()} rps peak</span><span>${isLearn ? `${p.stages.length} stages` : `${p.tests.length} test cases`}</span></div>
    </a>`)}
  </div>
</div>`;
  return page({ title: isLearn ? 'Learn' : 'Practice', active: mode, body, pageId: 'lists' });
}

function incidentList() {
  const body = html`
<div class="container page">
  <div class="page-head">
    <div>
      <p class="eyebrow">Design & Fix</p>
      <h1>Something is <span class="accent">broken.</span> Fix it.</h1>
      <p class="lead">Each incident opens a broken system. Find the cause, change the design until every goal passes, then answer one last question.</p>
    </div>
  </div>
  <div class="filters" id="filters">
    ${['All', 'easy', 'intermediate', 'Kafka'].map((l, i) => html`<button type="button" class="filter${i === 0 ? ' active' : ''}" data-level="${l}">${l === 'All' ? 'All' : l[0].toUpperCase() + l.slice(1)}</button>`)}
    <input class="input search" id="search" type="search" placeholder="Search incidents" aria-label="Search incidents">
  </div>
  <div class="grid grid-3" id="list">
    ${INCIDENTS.map((inc) => html`
    <a class="card card-link incident-card" href="/fix/${inc.id}" data-level="${inc.difficulty}" data-track="${inc.track}" data-title="${inc.title.toLowerCase()} ${inc.category}">
      <div class="card-top"><span class="${diffClass(inc.difficulty)}">${inc.difficulty}</span><span class="pill">${inc.category}</span></div>
      <h3>${inc.title}</h3>
      <p>${inc.headline}</p>
    </a>`)}
  </div>
</div>`;
  return page({ title: 'Design & Fix', active: 'fix', body, pageId: 'lists' });
}

function workspace({ mode, id = null, title }) {
  const live = mode !== 'interview';
  const body = html`
<div class="ws" id="ws" data-mode="${mode}">
  <aside class="ws-left" id="ws-left" aria-label="Brief"><div class="ws-loading">Loading…</div></aside>
  <section class="ws-center" aria-label="Design canvas">
    <div class="ws-toolbar" id="ws-toolbar"></div>
    <div class="ws-stage">
      <div class="palette" id="palette" aria-label="Components"></div>
      <div class="canvas-host" id="canvas"></div>
      <div class="round-panel hidden" id="round-panel"></div>
    </div>
  </section>
  <aside class="ws-right" id="ws-right" aria-label="Details">
    <div class="tabs" role="tablist">
      <button class="tab active" type="button" role="tab" data-tab="inspect" aria-selected="true">Inspect</button>
      <button class="tab" type="button" role="tab" data-tab="results" aria-selected="false">Results</button>
      <button class="tab" type="button" role="tab" data-tab="hints" aria-selected="false">Live hints</button>
      ${live ? html`<button class="tab" type="button" role="tab" data-tab="run" aria-selected="false">Run</button>` : ''}
    </div>
    <div class="panel" id="panel-inspect" role="tabpanel"></div>
    <div class="panel hidden" id="panel-results" role="tabpanel"></div>
    <div class="panel hidden" id="panel-hints" role="tabpanel"></div>
    ${live ? html`<div class="panel hidden" id="panel-run" role="tabpanel"></div>` : ''}
  </aside>
</div>`;
  return page({ title, active: mode === 'fix' ? 'fix' : mode, body, bodyClass: 'workspace-page', pageId: 'workspace', data: { mode, id } });
}

function interviewHome({ grading, aiEnabled }) {
  const body = html`
<div class="container page">
  <div class="page-head">
    <div>
      <p class="eyebrow">Mock interview</p>
      <h1>Four rounds, <span class="accent">one report</span></h1>
      <p class="lead">Pick a problem and a time limit. You write the requirements, build the design (we load-test it), answer quick questions, and explain your choices. Then you get a report.</p>
    </div>
  </div>
  <div class="split split-wide">
    <div class="card">
      <h2 class="card-title">Start an interview</h2>
      <form class="form" id="start-form">
        <label class="field"><span>Problem</span><select class="input" id="iv-problem" required>
          ${PROBLEMS.map((p) => html`<option value="${p.id}">${p.title} (${p.difficulty})</option>`)}
        </select></label>
        <label class="field"><span>Time limit</span>
          <select class="input" id="iv-duration"><option value="30">30 minutes</option><option value="45">45 minutes</option><option value="60" selected>60 minutes</option><option value="90">90 minutes</option></select>
        </label>
        <ol class="rounds">
          <li><strong>Requirements.</strong> What it must do, and how fast and reliable it must be.</li>
          <li><strong>Design.</strong> Build it on the canvas. We test it with heavy traffic and failures.</li>
          <li><strong>Quick questions.</strong> Five questions about the parts you used.</li>
          <li><strong>Trade-offs.</strong> Three questions about your choices.</li>
        </ol>
        <p class="muted small">Answers are graded by ${grading}.${aiEnabled ? '' : ' For written AI feedback too, add an OpenAI key or a free local model. The README shows how.'}</p>
        <button class="btn btn-primary" type="submit" id="iv-start">Start interview</button>
      </form>
    </div>
    <div class="card">
      <h2 class="card-title">Your interview</h2>
      <div id="iv-current"><p class="empty">No interview yet.</p></div>
      <p class="muted small">Only your latest interview is kept, and only in this browser. Starting a new one replaces it.</p>
    </div>
  </div>
</div>`;
  return page({ title: 'Mock interview', active: 'interview', body, pageId: 'interview-home' });
}

function roadmap() {
  const body = html`
<div class="container page">
  <div class="page-head">
    <div>
      <p class="eyebrow">Roadmap</p>
      <h1>From building blocks to production, then <span class="accent">Kafka</span></h1>
      <p class="lead">${TOPICS.length} short lessons in ${PHASES.length} steps. Each one explains an idea, asks three quiz questions, and links to a hands-on exercise.</p>
    </div>
  </div>
  ${TRACKS.map((track) => html`
  <section class="track" id="track-${track.id}">
    <div class="track-head"><p class="eyebrow">${track.id === 'kafka' ? 'Track 2' : 'Track 1'}</p><h2>${track.title}</h2><p class="muted">${track.summary}</p></div>
    <ol class="phases">
      ${PHASES.filter((ph) => ph.track === track.id).map((ph) => html`
      <li class="phase">
        <div class="phase-head"><span class="phase-n">${ph.number}</span><div><h2>${ph.title}</h2><p class="muted">${ph.summary}</p></div></div>
        <div class="grid grid-4">
          ${TOPICS.filter((t) => t.phase === ph.id).map((t) => html`
          <a class="card card-link topic-card" href="/roadmap/${t.id}">
            <h3>${t.title}</h3><p>${t.summary}</p>
            <div class="card-meta"><span>${t.minutes} min</span></div>
          </a>`)}
        </div>
      </li>`)}
    </ol>
  </section>`)}
</div>`;
  return page({ title: 'Roadmap', active: 'roadmap', body, pageId: 'roadmap' });
}

function topicPage(topic) {
  const phase = PHASES.find((p) => p.id === topic.phase);
  const index = TOPICS.indexOf(topic);
  const prev = TOPICS[index - 1];
  const next = TOPICS[index + 1];
  const tryHref = topic.try.kind === 'problem' ? `/practice/${topic.try.id}` : `/fix/${topic.try.id}`;
  const body = html`
<div class="container page topic">
  <nav class="crumbs"><a href="/roadmap">Roadmap</a><span>/</span><span>Phase ${phase.number}: ${phase.title}</span></nav>
  <article class="topic-body">
    <h1>${topic.title}</h1>
    <p class="lead">${topic.summary}</p>
    ${topic.body.map((b) => (b.p ? html`<p>${b.p}</p>` : html`<ul>${b.ul.map((li) => html`<li>${li}</li>`)}</ul>`))}
    <div class="card key-points"><h3>Key points</h3><ul>${topic.key_points.map((k) => html`<li>${k}</li>`)}</ul></div>
    <section class="quiz card" id="quiz" data-topic="${topic.id}">
      <h2 class="card-title">Check yourself</h2>
      <div id="quiz-body"><p class="muted">Loading quiz…</p></div>
    </section>
    <div class="card try-card"><div><p class="eyebrow">Try it</p><h3>${topic.try.label}</h3></div><a class="btn btn-primary" href="${tryHref}">Open in the simulator</a></div>
    <div class="topic-nav">
      ${prev ? html`<a class="btn btn-ghost" href="/roadmap/${prev.id}">← ${prev.title}</a>` : html`<span></span>`}
      ${next ? html`<a class="btn btn-ghost" href="/roadmap/${next.id}">${next.title} →</a>` : html`<span></span>`}
    </div>
  </article>
</div>`;
  return page({ title: topic.title, active: 'roadmap', body, pageId: 'topic', data: { topic: topic.id } });
}

function report(id) {
  const body = html`<div class="container page" id="report"><p class="muted">Loading report…</p></div>`;
  return page({ title: 'Interview report', active: 'interview', body, pageId: 'report', data: { id } });
}

function errorPage(status, message) {
  const body = html`
<div class="container page center error-page">
  <p class="eyebrow">Error ${status}</p>
  <h1>${status === 404 ? html`Nothing to see <span class="accent">here</span>` : html`Something went <span class="accent">wrong</span>`}</h1>
  <p class="lead">${message}</p>
  <a class="btn btn-primary" href="/">Back to the start</a>
</div>`;
  return page({ title: `Error ${status}`, body, pageId: 'error' });
}

// ================================================================ routes: pages
function registerPages(router, app) {
  router.get('/', () => htmlResponse(landing()));
  router.get('/learn', () => htmlResponse(problemList('learn')));
  router.get('/practice', () => htmlResponse(problemList('practice')));
  router.get('/fix', () => htmlResponse(incidentList()));
  router.get('/build', () => htmlResponse(workspace({ mode: 'build', title: 'Build & stress test' })));
  router.get('/interview', () => htmlResponse(interviewHome({ grading: app.ai.provider, aiEnabled: app.ai.enabled })));
  router.get('/roadmap', () => htmlResponse(roadmap()));

  router.get('/learn/:id', ({ params }) => {
    const p = getProblem(params.id);
    if (!p) throw new HttpError(404, 'That architecture does not exist.');
    return htmlResponse(workspace({ mode: 'learn', id: p.id, title: `Learn: ${p.title}` }));
  });

  router.get('/practice/:id', ({ params }) => {
    const p = getProblem(params.id);
    if (!p) throw new HttpError(404, 'That problem does not exist.');
    return htmlResponse(workspace({ mode: 'practice', id: p.id, title: `Practice: ${p.title}` }));
  });

  router.get('/fix/:id', ({ params }) => {
    const inc = getIncident(params.id);
    if (!inc) throw new HttpError(404, 'That incident does not exist.');
    return htmlResponse(workspace({ mode: 'fix', id: inc.id, title: inc.title }));
  });

  router.get('/interview/:id', ({ params }) => htmlResponse(workspace({ mode: 'interview', id: params.id, title: 'Mock interview' })));

  router.get('/roadmap/:id', ({ params }) => {
    const t = getTopic(params.id);
    if (!t) throw new HttpError(404, 'That topic does not exist.');
    return htmlResponse(topicPage(t));
  });

  router.get('/evaluations/:id', ({ params }) => htmlResponse(report(params.id)));
}

// ================================================================ routes: content
function registerContent(router) {
  router.get('/api/catalog', () => publicCatalog());

  router.get('/api/problems', () => ({ items: PROBLEMS.map(problemSummary) }));

  router.get('/api/problems/:id', ({ params }) => {
    const p = getProblem(params.id);
    if (!p) throw new HttpError(404, 'Unknown problem.');
    return publicProblem(p);
  });

  router.get('/api/incidents', () => ({ items: INCIDENTS.map(incidentSummary) }));

  router.get('/api/incidents/:id', ({ params }) => {
    const inc = getIncident(params.id);
    if (!inc) throw new HttpError(404, 'Unknown incident.');
    return publicIncident(inc);
  });

  router.get('/api/roadmap', () => ({ phases: roadmapOverview() }));

  router.get('/api/roadmap/:id', ({ params }) => {
    const t = getTopic(params.id);
    if (!t) throw new HttpError(404, 'Unknown topic.');
    return publicTopic(t);
  });

  router.get('/api/stress/catalog', () => ({ areas: stressCatalog() }));
}

// ================================================================ routes: engine
const MAX_SPIKE = 50;

function readGraph(body) {
  const { graph, issues } = normalizeGraph(body?.graph ?? body);
  if (!graph.nodes.length) throw new HttpError(400, 'The design is empty. Add components to the canvas first.');
  return { graph, issues };
}

function problemOrNull(id) {
  if (!id) return null;
  const p = getProblem(id);
  if (!p) throw new HttpError(404, 'Unknown problem.');
  return p;
}

// Strips the per-test simulation payloads (large) but keeps one full simulation for the
// canvas overlay.
const slimTests = (results) => results.map(({ sim, ...rest }) => rest);

function registerEngine(router) {
  router.post('/api/validate', ({ body }) => {
    const { graph, issues } = readGraph(body);
    const problem = problemOrNull(body.problem_id);
    return { ...analyzeDesign(graph, { problem }), issues };
  });

  router.post('/api/simulate', ({ body }) => {
    const { graph, issues } = readGraph(body);
    const problem = problemOrNull(body.problem_id);
    const multiplier = Math.min(MAX_SPIKE, Math.max(0.1, Number(body.scenario?.multiplier) || 1));
    let traffic = problem ? problemTraffic(problem, 1) : {};
    if (body.traffic && Number(body.traffic.requests_per_second) > 0) traffic = { requests_per_second: Number(body.traffic.requests_per_second) };
    let overrides = {};
    let scenarioNote = null;
    if (body.scenario?.type === 'spike') {
      traffic = { ...traffic, multiplier };
      scenarioNote = `Traffic at ${multiplier}× the normal peak.`;
    } else if (body.scenario?.type === 'failure' && body.scenario.node_id) {
      const node = graph.nodes.find((n) => n.id === body.scenario.node_id);
      if (!node) throw new HttpError(400, 'The component to fail is not in the design.');
      overrides = failureOverrides(node);
      scenarioNote = `${node.label} failed.`;
    }
    const sim = simulate(graph, { traffic, overrides });
    return {
      simulation: sim,
      score: scoreSimulation(sim, { latencyTarget: problem?.scale.latency_target_ms || Number(body.latency_target_ms) || 300 }),
      rules: analyzeDesign(graph, { problem, sim }),
      scenario: scenarioNote,
      issues,
    };
  });

  router.post('/api/practice/:id/run', ({ params, body }) => {
    const problem = problemOrNull(params.id);
    const { graph, issues } = readGraph(body);
    const suite = runSuite(graph, problem, { tiers: ['component'] });
    const rules = analyzeDesign(graph, { problem, sim: suite.baseline });
    return { tests: slimTests(suite.results), summary: suite.summary, simulation: suite.baseline, rules, issues };
  });

  router.post('/api/practice/:id/submit', ({ params, body }) => {
    const problem = problemOrNull(params.id);
    const { graph, issues } = readGraph(body);
    const suite = runSuite(graph, problem, { tiers: ['component', 'system'] });
    const rules = analyzeDesign(graph, { problem, sim: suite.baseline });
    const result = combineSubmission({ testScore: suite.summary.score, ruleScore: rules.score.total, securityScore: rules.security.score });
    return {
      submission: { ...result, parts: { tests: suite.summary.score, design: rules.score.total, security: rules.security.score } },
      tests: slimTests(suite.results), summary: suite.summary, simulation: suite.baseline, rules, issues,
    };
  });

  router.post('/api/stress-test', ({ body }) => {
    const { graph } = readGraph(body);
    const rps = Number(body.traffic?.requests_per_second) || 0;
    const result = runStressTest(graph, {
      traffic: rps > 0 ? { requests_per_second: Math.min(rps, 10_000_000) } : {},
      latencyTarget: Math.max(10, Number(body.latency_target_ms) || 300),
      focus: Array.isArray(body.focus) ? body.focus.slice(0, 40).map(String) : [],
    });
    const rules = analyzeDesign(graph, { sim: simulate(graph, { traffic: rps > 0 ? { requests_per_second: rps } : {} }) });
    return { ...result, rules };
  });
}

// ================================================================ routes: incidents
function incidentOr404(id) {
  const inc = getIncident(id);
  if (!inc) throw new HttpError(404, 'Unknown incident.');
  return inc;
}

function registerIncidents(router) {
  router.post('/api/incidents/:id/root-cause', ({ params, body }) => {
    const inc = incidentOr404(params.id);
    const correct = String(body.answer || '') === inc.root_cause.answer;
    return { correct, answer: inc.root_cause.answer, explanation: inc.root_cause.explanation };
  });

  router.post('/api/incidents/:id/hint', ({ params, body }) => {
    const inc = incidentOr404(params.id);
    const index = Math.max(0, Math.min(inc.hints.length - 1, Number(body.index) || 0));
    return { index, hint: inc.hints[index], total: inc.hints.length };
  });

  router.post('/api/incidents/:id/check', ({ params, body }) => {
    const inc = incidentOr404(params.id);
    const { sim, ...rest } = checkIncident(inc, body.graph ?? body);
    return { ...rest, simulation: sim };
  });

  // The reference fix, for learners who ask to see it.
  router.post('/api/incidents/:id/solution', ({ params }) => {
    const inc = incidentOr404(params.id);
    const solved = inc.solution(structuredClone(inc.graph));
    return { cause: inc.root_cause.explanation, changes: describeChanges(inc.graph, solved), graph: autoLayout(solved) };
  });

  router.post('/api/incidents/:id/debrief', ({ params, body }) => {
    const inc = incidentOr404(params.id);
    const view = debriefView(inc);
    return {
      correct: Number(body.answer) === view.answer,
      answer: view.answer,
      explanation: view.explanation,
      lesson: inc.lesson,
      solved: checkIncident(inc, body.graph ?? {}).solved,
    };
  });
}

// ================================================================ routes: roadmap
const PASS = 2;

function registerRoadmap(router) {
  router.post('/api/roadmap/:id/quiz', ({ params, body }) => {
    const topic = getTopic(params.id);
    if (!topic) throw new HttpError(404, 'Unknown topic.');
    const answers = Array.isArray(body.answers) ? body.answers : [];
    const view = quizView(topic);
    const results = view.map((item, i) => ({ correct: Number(answers[i]) === item.answer, answer: item.answer, explain: item.explain }));
    const score = results.filter((r) => r.correct).length;
    return { score, total: view.length, passed: score >= PASS, results };
  });
}

// ================================================================ routes: interview
// Interviews are not stored: the browser keeps the session and sends back what each step
// needs. The design and the quick questions are re-derived from the graph and the
// interview id, so the report never depends on scores the browser claims.

const DURATIONS = [30, 45, 60, 90];
const DESIGN_CATEGORIES = new Set(['topology', 'reliability', 'scalability', 'async', 'data', 'structure']);
const TEXT_LIMIT = 6000;

const interviewProblem = (p) => ({
  id: p.id, title: p.title, difficulty: p.difficulty, statement: p.statement, constraints: p.constraints,
  scale: { users: p.scale.users, peak_rps: p.scale.peak_rps, latency_target_ms: p.scale.latency_target_ms },
});

const strList = (v) => (Array.isArray(v) ? v.filter((x) => typeof x === 'string').map((x) => x.slice(0, 300)).slice(0, 30) : []);
const clampNum = (v, lo, hi) => (Number.isFinite(Number(v)) ? Math.max(lo, Math.min(hi, Number(v))) : lo);
const text = (v, limit = 2000) => (typeof v === 'string' ? v.slice(0, limit) : null);

function problemOr400(id) {
  const p = getProblem(String(id || ''));
  if (!p) throw new HttpError(400, 'Choose a problem.');
  return p;
}

function readSeed(seed) {
  const s = String(seed || '');
  if (!/^[\w-]{6,64}$/.test(s)) throw new HttpError(400, 'The interview id is missing.');
  return s;
}

function readDesign(input) {
  const { graph } = normalizeGraph(input);
  if (graph.nodes.length < 2) throw new HttpError(400, 'Add your architecture to the canvas before submitting.');
  return graph;
}

function gradeDesign(graph, problem) {
  const suite = runSuite(graph, problem, { tiers: ['component', 'system'] });
  const rules = analyzeDesign(graph, { problem, sim: suite.baseline });
  const types = new Set(graph.nodes.map((n) => n.type));
  const required = problem.required.map((r) => r.type);
  const coverage = required.length ? (100 * required.filter((t) => types.has(t)).length) / required.length : 100;
  const designIssues = rules.findings.filter((f) => DESIGN_CATEGORIES.has(f.category) && f.severity !== 'info').length;
  return {
    graph,
    test_score: suite.summary.score, passed: suite.summary.passed, total: suite.summary.total,
    rule_score: rules.score.total, security_score: rules.security.score,
    components_score: Math.round(0.7 * coverage + 0.3 * Math.max(0, 100 - 10 * designIssues)),
    missing_components: problem.required.filter((r) => !types.has(r.type)).map((r) => r.type),
    failing_tests: suite.results.filter((r) => !r.pass).map((r) => ({ name: r.name, error_rate: r.metrics.error_rate, score: r.score })),
    top_findings: rules.findings.slice(0, 5).map((f) => ({ severity: f.severity, title: f.title, recommendation: f.recommendation })),
  };
}

const readAnswers = (list, n) => Array.from({ length: n }, (_, i) => (Number.isInteger(Number(list?.[i])) ? Number(list[i]) : -1));

function readRequirements(r) {
  if (!r || typeof r !== 'object') return null;
  const part = (p) => ({ score: Math.round(clampNum(p?.score, 0, 100)), covered: strList(p?.covered), missed: strList(p?.missed) });
  return {
    functional: part(r.functional),
    non_functional: { ...part(r.non_functional), quantified: !!r.non_functional?.quantified },
    feedback: text(r.feedback), tips: strList(r.tips), source: r.source === 'llm' ? 'llm' : 'rubric',
  };
}

function readTradeoffs(graded, items) {
  if (!Array.isArray(graded)) return null;
  return items.map((q, i) => ({
    score: clampNum(graded[i]?.score, 0, 10),
    covered: strList(graded[i]?.covered),
    missed: strList(graded[i]?.missed),
    feedback: text(graded[i]?.feedback),
    model_answer: q.model,
  }));
}

function readiness(score) {
  if (score >= 80) return 'Interview ready';
  if (score >= 65) return 'Almost there';
  if (score >= 45) return 'Keep practising';
  return 'Foundations first';
}

function buildReport({ id, problem, durationMin, startedAt, req, design, mcqItems, mcqAnswers, tradeItems, tradeAnswers, trade }) {
  const correct = mcqItems.filter((m, i) => mcqAnswers[i] === m.answer).length;
  const mcqPct = Math.round((100 * correct) / mcqItems.length);
  const tradePct = trade?.length ? Math.round((trade.reduce((s, r) => s + r.score, 0) / trade.length) * 10) : 0;
  const dims = [
    { id: 'scope', label: 'Scope', description: 'Clarified what the system must do.', weight: 0.15, score: req?.functional.score ?? 0 },
    { id: 'requirements', label: 'Requirements', description: 'Set measurable non-functional targets.', weight: 0.15, score: req?.non_functional.score ?? 0 },
    { id: 'components', label: 'Components', description: 'Chose the right building blocks.', weight: 0.2, score: design.components_score },
    { id: 'architecture', label: 'Architecture', description: 'Design holds up under load and failure.', weight: 0.3,
      score: Math.round(0.6 * design.test_score + 0.25 * design.rule_score + 0.15 * design.security_score) },
    { id: 'tradeoffs', label: 'Trade-offs', description: 'Reasoned about alternatives and risks.', weight: 0.2, score: Math.round(0.6 * tradePct + 0.4 * mcqPct) },
  ];
  const overall = Math.round(dims.reduce((s, d) => s + d.score * d.weight, 0));
  const elapsedMin = Math.max(0, Math.round((Date.now() - startedAt) / 60000));

  const strengths = dims.filter((d) => d.score >= 75).map((d) => `${d.label}: ${d.description.replace(/\.$/, '')} (${d.score}).`);
  const improvements = [];
  if (req?.functional.missed.length) improvements.push(`Scope: you did not mention ${req.functional.missed.slice(0, 3).map((t) => `"${t}"`).join(', ')}.`);
  if (req && !req.non_functional.quantified) improvements.push('Requirements: attach numbers to latency, throughput and availability targets.');
  if (design.missing_components.length) improvements.push(`Components: consider ${design.missing_components.join(', ')} for this problem.`);
  for (const t of design.failing_tests.slice(0, 3)) improvements.push(`Architecture: "${t.name}" failed (${(t.error_rate * 100).toFixed(1)}% errors).`);
  for (const t of (trade || []).filter((r) => r.score < 6).slice(0, 2)) if (t.missed.length) improvements.push(`Trade-offs: cover ${t.missed.slice(0, 2).join(' and ').toLowerCase()}.`);
  if (!improvements.length) improvements.push('Keep practising with harder problems and tighter time limits.');

  return {
    id,
    problem: { id: problem.id, title: problem.title, difficulty: problem.difficulty },
    overall, readiness: readiness(overall), dimensions: dims, strengths, improvements,
    duration_min: durationMin, elapsed_min: elapsedMin, over_time: elapsedMin > durationMin,
    rounds: {
      requirements: req,
      design: {
        test_score: design.test_score, rule_score: design.rule_score, security_score: design.security_score,
        passed: design.passed, total: design.total, failing_tests: design.failing_tests,
        missing_components: design.missing_components, top_findings: design.top_findings, graph: design.graph,
      },
      mcqs: { correct, total: mcqItems.length, items: mcqItems.map((m, i) => ({ q: m.q, options: m.options, answer: m.answer, chosen: mcqAnswers[i], explain: m.explain })) },
      tradeoffs: trade ? tradeItems.map((q, i) => ({ question: q.question, answer: tradeAnswers[i], ...trade[i] })) : null,
    },
    finished_at: new Date().toISOString(),
  };
}

function registerInterview(router, app) {
  const { ai } = app;

  router.get('/api/interview/problems/:id', ({ params }) => {
    const p = getProblem(params.id);
    if (!p) throw new HttpError(404, 'Unknown problem.');
    return interviewProblem(p);
  });

  router.post('/api/interview/requirements', async ({ body }) => {
    const problem = problemOr400(body.problem_id);
    const functional = String(body.functional || '').slice(0, TEXT_LIMIT);
    const nonFunctional = String(body.non_functional || '').slice(0, TEXT_LIMIT);
    if (functional.trim().length < 10) throw new HttpError(400, 'Write down the functional requirements first.');
    return ai.requirementsFeedback(problem, functional, nonFunctional);
  });

  router.post('/api/interview/design', ({ body }) => {
    const problem = problemOr400(body.problem_id);
    const seed = readSeed(body.seed);
    const design = gradeDesign(readDesign(body.graph), problem);
    return {
      design,
      mcqs: pickMcqs(design.graph, problem, seed).map(({ q, options, topic }) => ({ q, options, topic })),
      tradeoffs: pickTradeoffs(design.graph, 3).map(({ id, question }) => ({ id, question })),
    };
  });

  router.post('/api/interview/mcqs', ({ body }) => {
    const problem = problemOr400(body.problem_id);
    const items = pickMcqs(readDesign(body.graph), problem, readSeed(body.seed));
    const answers = readAnswers(body.answers, items.length);
    const results = items.map((m, i) => ({ correct: answers[i] === m.answer, answer: m.answer, explain: m.explain }));
    return { correct: results.filter((r) => r.correct).length, total: items.length, results };
  });

  router.post('/api/interview/tradeoffs', async ({ body }) => {
    const items = pickTradeoffs(readDesign(body.graph), 3);
    const answers = items.map((_, i) => String(body.answers?.[i] || '').slice(0, TEXT_LIMIT));
    return { items: await ai.tradeoffFeedback(items, answers) };
  });

  router.post('/api/interview/report', ({ body }) => {
    const problem = problemOr400(body.problem_id);
    const seed = readSeed(body.seed);
    const design = gradeDesign(readDesign(body.graph), problem);
    const mcqItems = pickMcqs(design.graph, problem, seed);
    const tradeItems = pickTradeoffs(design.graph, 3);
    const started = Date.parse(body.started_at);
    return buildReport({
      id: seed,
      problem,
      durationMin: DURATIONS.includes(Number(body.duration_min)) ? Number(body.duration_min) : 60,
      startedAt: Number.isFinite(started) ? Math.min(started, Date.now()) : Date.now(),
      req: readRequirements(body.requirements),
      design,
      mcqItems,
      mcqAnswers: readAnswers(body.mcq_answers, mcqItems.length),
      tradeItems,
      tradeAnswers: tradeItems.map((_, i) => String(body.tradeoff_answers?.[i] || '').slice(0, TEXT_LIMIT)),
      trade: readTradeoffs(body.tradeoffs, tradeItems),
    });
  });
}

// ================================================================ app
const ROOT = path.dirname(fileURLToPath(import.meta.url));

// Reads KEY=value lines from a .env file into process.env, if the file exists.
function loadEnv(file = path.join(ROOT, '.env')) {
  try {
    process.loadEnvFile(file);
    return true;
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    throw err;
  }
}

// The server keeps no state: every request carries what it needs.
function createApp({ env = process.env, quiet = false } = {}) {
  const app = { ai: createAi(env) };

  const router = new Router();
  for (const register of [registerPages, registerContent, registerEngine, registerIncidents, registerRoadmap, registerInterview]) {
    register(router, app);
  }

  async function handle(req, res) {
    const url = new URL(req.url, 'http://localhost');
    const pathname = url.pathname;
    try {
      if (req.method === 'GET' || req.method === 'HEAD') {
        if (/^\/(app\.js|app\.css|engine\.js)$/.test(pathname) && await serveStatic(req, res, path.join(ROOT, 'public'), pathname.slice(1))) return;
      }
      const match = router.match(req.method, pathname);
      if (!match) throw new HttpError(404, 'That page does not exist.');
      if (match.methodNotAllowed) throw new HttpError(405, 'Method not allowed.');

      const ctx = { req, res, url, app, params: match.params, query: url.searchParams, body: {} };
      if (['POST', 'PUT'].includes(req.method)) ctx.body = await readJson(req);

      const out = await match.handler(ctx);
      if (res.writableEnded) return;
      if (out && out.__html !== undefined) return sendHtml(res, out.status || 200, out.__html);
      if (out && out.__status) return sendJson(res, out.__status, out.body);
      return sendJson(res, 200, out ?? {});
    } catch (err) {
      const status = err.status || 500;
      if (status === 500 && !quiet) console.error(err);
      const message = status === 500 ? 'Something went wrong on the server.' : err.message;
      if (pathname.startsWith('/api/')) return sendJson(res, status, { error: message, ...(err.extra || {}) });
      return sendHtml(res, status, errorPage(status, message));
    }
  }

  return { handle, app };
}

// ================================================================ index
loadEnv();

const port = Number(process.env.PORT) || 3100;
const host = process.env.HOST || '127.0.0.1';
const { handle, app } = createApp();

const server = http.createServer((req, res) => {
  handle(req, res);
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Port ${port} is already in use. Try another one, for example: PORT=${port + 1} npm start`);
    process.exit(1);
  }
  throw err;
});

server.listen(port, host, () => {
  const shown = host === '127.0.0.1' ? 'localhost' : host;
  console.log(`Kafka Lab is running. Open http://${shown}:${port}`);
  console.log(`Interview answers are graded by ${app.ai.provider}.`);
});

const shutdown = () => {
  server.close(() => process.exit(0));
  server.closeAllConnections();
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
