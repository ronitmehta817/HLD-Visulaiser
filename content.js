// Kafka Lab content: practice problems, incidents, the roadmap and the interview question bank, Kafka included.

import {
  analyzeDesign, autoLayout, COMPONENTS, defaultsFor, failureOverrides, fmtDuration, GraphIndex, guaranteeViolations, normalizeGraph, param, paramSpec, simulate, stressCatalog, summarizeSim,
} from './public/engine.js';

// ================================================================ content: shuffle
// Deterministic option shuffling. Content is authored with the correct option first; the
// same seed always yields the same order, so display and grading agree without storing state.

export function seededPermutation(seed, n) {
  let h = 2166136261;
  for (const ch of String(seed)) {
    h ^= ch.charCodeAt(0);
    h = Math.imul(h, 16777619);
  }
  const idx = [...Array(n).keys()];
  for (let i = n - 1; i > 0; i--) {
    h = Math.imul(h ^ (h >>> 15), 2246822507) >>> 0;
    const j = h % (i + 1);
    [idx[i], idx[j]] = [idx[j], idx[i]];
  }
  return idx;
}

export function shuffleQuestion(seed, options, answer) {
  const perm = seededPermutation(seed, options.length);
  return { options: perm.map((i) => options[i]), answer: perm.indexOf(answer) };
}

// ================================================================ content: presets
// Settings and helpers shared by every problem file.
export const SECURE_DB = { encryption_at_rest: true, backups: true };
export const HA_DB = { read_replicas: 2, failover: true };
export const HA_CACHE = { replicas: 2, request_coalescing: true };
export const req = (text, keywords) => ({ text, keywords });

// ================================================================ content: builder
// Small DSL for authoring reference designs. Unknown types or parameters throw, so a typo
// in content fails at startup (and in the content tests) instead of silently using defaults.
export function design(build) {
  const nodes = [];
  const edges = [];
  const types = new Map();

  const node = (type, id, label, params = {}) => {
    if (!COMPONENTS[type]) throw new Error(`Unknown component type "${type}"`);
    if (types.has(id)) throw new Error(`Duplicate node id "${id}"`);
    const config = defaultsFor(type);
    for (const [key, value] of Object.entries(params)) {
      const spec = paramSpec(type, key);
      if (!spec) throw new Error(`Unknown parameter "${key}" for ${type} "${id}"`);
      config[spec.block][key] = value;
    }
    nodes.push({ id, type, label: label || COMPONENTS[type].label, x: 0, y: 0, config });
    types.set(id, type);
    return id;
  };

  const link = (source, target, opts = {}) => {
    if (!types.has(source) || !types.has(target)) throw new Error(`Edge references a missing node: ${source} -> ${target}`);
    edges.push({
      id: `${source}->${target}`,
      source, target,
      kind: opts.kind || (['Queue', 'KafkaTopic'].includes(types.get(target)) || (types.get(source) === 'Database' && types.get(target) === 'KafkaConnect') ? 'async' : 'sync'),
      ratio: opts.ratio ?? null,
      optional: !!opts.optional,
      read_share: opts.readShare ?? null,
    });
  };

  const chain = (...ids) => {
    for (let i = 0; i + 1 < ids.length; i++) link(ids[i], ids[i + 1]);
  };

  build({ node, link, chain });
  return autoLayout({ nodes, edges });
}

const READERS = ['ConsumerGroup', 'StreamProcessor', 'KafkaConnect', 'MirrorMaker'];

// Tests for systems built around Kafka. Spikes may leave consumers behind for a while (Kafka
// absorbs the burst), so only the steady tests hold consumers to the freshness target.
// `requires` adds delivery requirements to the normal, broker-loss and consumer-crash tests.
export function kafkaTests({ spike = 3, mega = 6, requires = {}, outageHours = 24, poison = true, schema = true, db = true, extra = [] } = {}) {
  const needs = (list) => (list?.length ? { requires: list } : {});
  return [
    { id: 'normal', name: 'Normal peak', tier: 'component', scenario: 'normal', weight: 1, ...needs(requires.normal),
      description: 'The stated peak with everything healthy. Producers must succeed and consumers must keep the data fresh.' },
    { id: 'spike', name: `Spike (${spike}×)`, tier: 'component', scenario: 'spike', multiplier: spike, ignore_freshness: true, weight: 1,
      description: `A sudden ${spike}× burst. Kafka must accept every write; consumers may fall behind for a while and catch up later.` },
    { id: 'broker-loss', name: 'Lose a broker', tier: 'component', scenario: 'failure', prefer: ['KafkaTopic'], weight: 1, ...needs(['no-loss', ...(requires.brokerLoss || [])]),
      description: 'One Kafka broker dies. Partitions must elect new leaders, writes must continue, and no acknowledged message may be lost.' },
    { id: 'consumer-crash', name: 'A consumer crashes', tier: 'component', scenario: 'failure', prefer: READERS, weight: 1, ...needs(requires.crash),
      description: 'One consumer instance dies and its group hands the partitions to the others, who must keep up.' },
    { id: 'mega-spike', name: `Mega spike (${mega}×)`, tier: 'system', scenario: 'spike', multiplier: mega, ignore_freshness: true, weight: 1,
      description: `A once-a-year event: ${mega}× the normal peak.` },
    ...(outageHours ? [{ id: 'consumer-outage', name: `Consumers stop for ${outageHours} h`, tier: 'system', scenario: 'kafka', weight: 1,
      typeOverrides: Object.fromEntries(READERS.map((t) => [t, { down: true }])), kafka: { horizon_hours: outageHours }, ignore_freshness: true, requires: ['no-loss'],
      description: `A bad deploy stops every consumer for ${outageHours} hours. When they come back, every message must still be in Kafka.` }] : []),
    ...(poison ? [{ id: 'poison-pill', name: 'Poison message', tier: 'system', scenario: 'kafka', weight: 1, kafka: { poison_pill: true }, ignore_freshness: true, requires: ['no-stuck'],
      description: 'A malformed message that always fails processing arrives. It must not block everything behind it.' }] : []),
    ...(schema ? [{ id: 'schema-change', name: 'Breaking schema change', tier: 'system', scenario: 'kafka', weight: 1, kafka: { schema_break: true }, ignore_freshness: true, requires: ['no-stuck'],
      description: 'A producer deploy changes the message format in a way existing consumers cannot read.' }] : []),
    ...(db ? [{ id: 'db-outage', name: 'Database primary fails', tier: 'system', scenario: 'failure', prefer: ['Database'], weight: 1,
      description: 'The busiest database primary dies.' }] : []),
    ...extra,
  ];
}

export function standardTests({ spike = 3, mega = 8, cache = true, lb = true, extra = [] } = {}) {
  return [
    { id: 'normal', name: 'Normal peak', tier: 'component', scenario: 'normal', weight: 1,
      description: 'The stated peak traffic, everything healthy.' },
    { id: 'spike', name: `Spike (${spike}×)`, tier: 'component', scenario: 'spike', multiplier: spike, weight: 1,
      description: `A sudden ${spike}× burst over the normal peak.` },
    { id: 'component-failure', name: 'Component failure', tier: 'component', scenario: 'failure', weight: 1,
      description: 'One component fails. Redundant components are chosen first, so a well-built design should shrug this off.' },
    { id: 'mega-spike', name: `Mega spike (${mega}×)`, tier: 'system', scenario: 'spike', multiplier: mega, weight: 1,
      description: `A once-a-year event: ${mega}× the normal peak.` },
    { id: 'db-outage', name: 'Database primary fails', tier: 'system', scenario: 'failure', prefer: ['Database'], weight: 1,
      description: 'The busiest database primary dies.' },
    ...(cache ? [{ id: 'cache-down', name: 'Cache node fails', tier: 'system', scenario: 'failure', prefer: ['Cache'], weight: 1,
      description: 'A cache node dies and its keys have to be refilled from the database.' }] : []),
    ...(lb ? [{ id: 'lb-failure', name: 'Load balancer fails', tier: 'system', scenario: 'failure', prefer: ['LoadBalancer'], weight: 1,
      description: 'A load balancer instance dies.' }] : []),
    ...extra,
  ];
}

// ================================================================ content: incident tools
// Small helpers for writing incident reference solutions.

export const setParam = (graph, id, key, value) => {
  const n = graph.nodes.find((x) => x.id === id);
  for (const block of ['basic', 'advanced', 'security']) {
    if (n.config[block] && key in n.config[block]) { n.config[block][key] = value; return graph; }
  }
  n.config.advanced[key] = value;
  return graph;
};
export const addNode = (graph, node) => { graph.nodes.push(node); return graph; };
export const addEdge = (graph, source, target, extra = {}) => { graph.edges.push({ id: `${source}->${target}`, source, target, kind: 'sync', ratio: null, ...extra }); return graph; };
export const removeEdge = (graph, source, target) => { graph.edges = graph.edges.filter((e) => !(e.source === source && e.target === target)); return graph; };
export const fresh = (type, id, label, config = {}) => ({ id, type, label, x: 0, y: 0, config: { basic: {}, advanced: {}, security: {}, ...config } });

// ================================================================ content: more problems
const workerCrash = (name, description) => ({ id: 'worker-outage', name, tier: 'system', scenario: 'failure', prefer: ['Worker'], weight: 1, description });
const providerDown = (id, name, description) => ({ id, name, tier: 'system', scenario: 'failure', prefer: ['ThirdPartyAPI'], weight: 1, description });
const providerSlow = (id, name, times, description) => ({ id, name, tier: 'system', scenario: 'failure', typeOverrides: { ThirdPartyAPI: { latencyMult: times } }, weight: 1, description });

export const MORE_PROBLEMS = [
  // ---------------------------------------------------------------------------
  {
    id: 'qa_forum',
    title: 'Q&A Forum',
    difficulty: 'Beginner',
    category: 'Social',
    summary: 'Serve question pages to millions of readers, most of whom arrive from a search engine.',
    statement: 'People ask programming questions, others answer, and everyone votes on the answers. Most visitors are not logged in: they land on one question page from a search engine, read it and leave. Logged-in users post, answer and vote, and anyone can search old questions by keyword.',
    functional: [
      req('Ask questions and post answers', ['ask', 'question', 'answer', 'post']),
      req('Vote answers up or down', ['vote', 'upvote', 'downvote', 'score']),
      req('Read a question page with all its answers', ['read', 'view', 'page', 'question page']),
      req('Search questions by keyword', ['search', 'keyword', 'find', 'full-text']),
      req('Tag questions by topic', ['tag', 'topic', 'label']),
    ],
    non_functional: [
      req('Pages load in under 250 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('6,000 page views per second at peak', ['6,000', '6k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Pages stay readable even if posting is down', ['availability', 'available', 'uptime', 'read-only', '99.9']),
      req('No question, answer or vote is ever lost', ['durable', 'durability', 'never lost', 'persist', 'replicat']),
    ],
    constraints: ['About 95% of requests are page views, 1% are posts and 4% are votes', 'Most page views come from visitors who are not logged in', 'A few thousand questions get most of the views each day'],
    scale: { users: 30000, peak_rps: 6000, latency_target_ms: 250, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 30000, requests_per_user: 0.2 },
    required: [
      { type: 'CDN', why: 'Visitors who are not logged in all see the same page, so the edge can answer them.' },
      { type: 'Cache', why: 'A popular question is read thousands of times between edits.' },
      { type: 'SearchIndex', why: 'Keyword search over millions of posts needs an inverted index.' },
      { type: 'Database', why: 'Questions, answers and votes must be stored durably.' },
    ],
    concepts: ['cdn', 'caching', 'search', 'queues'],
    hints: [
      'Most visitors are not logged in. Does their page have to be built fresh every time?',
      'A popular question is read thousands of times between edits. Where should those reads land?',
      'Searching with LIKE over millions of rows is slow. Which part is built for keyword search?',
      'Does a vote have to reach the database before the page answers?',
    ],
    stages: [
      {
        title: 'One server renders every page',
        goal: 'Find the first thing that breaks.',
        insights: ['One app server builds about 1,000 pages per second, a sixth of the peak.', 'Every page view runs several database queries, and so does every search.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Visitors', { users: 30000, requests_per_user: 0.2 });
          node('Server', 'app', 'Web App');
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95 });
          chain('users', 'app', 'db');
        }),
      },
      {
        title: 'A server pool and a cache',
        goal: 'Spread page building over several servers and keep hot questions in memory.',
        insights: ['Eight servers carry the normal peak, and the cache answers most question lookups.', 'A spike still overwhelms a fixed pool, and search still runs as slow database queries.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Visitors', { users: 30000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Web App', { replicas: 8 });
          node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.85 });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
      {
        title: 'Pages at the edge, real search',
        goal: 'Serve anonymous pages from the CDN and move search to an index.',
        insights: ['The CDN answers 60% of page views, so only logged-in users and misses reach your servers.', 'A search index answers keyword queries without touching the database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Visitors', { users: 30000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.6 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Web App', { autoscaling: true, autoscale_min: 3, autoscale_max: 20 });
          node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('SearchIndex', 'search', 'Question Search', { shards: 2, replicas: 1 });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, ...HA_DB });
          chain('users', 'cdn', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'search', { ratio: 0.1 });
        }),
      },
      {
        title: 'Votes in the background, hardened',
        goal: 'Take vote writes off the request path and protect every layer.',
        insights: ['Votes go on a queue. Counters add them up and write totals in batches, so the database sees a quarter of the vote writes.', 'A rate limiter stops vote bots; logins, HTTPS, encryption and backups protect accounts and posts.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Visitors', { users: 30000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.6 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Web App', { autoscaling: true, autoscale_min: 3, autoscale_max: 30, auth: 'jwt' });
          node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.85, ...HA_CACHE, encryption_at_rest: true });
          node('SearchIndex', 'search', 'Question Search', { shards: 2, replicas: 1, encryption_at_rest: true });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.99, ...HA_DB, ...SECURE_DB });
          node('Queue', 'votes', 'Vote Queue', { encryption_at_rest: true });
          node('Worker', 'counter', 'Vote Counters', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'search', { ratio: 0.1 });
          link('app', 'votes', { ratio: 0.04 });
          chain('votes', 'counter');
          link('counter', 'db', { ratio: 0.25, readShare: 0 });
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 6 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'nearby_places',
    title: 'Nearby Places',
    difficulty: 'Beginner',
    category: 'Location',
    summary: 'Show restaurants and shops near the user, with ratings and opening hours.',
    statement: 'A mobile app lists places near the user: restaurants, cafés and shops with ratings, photos and opening hours. Users search around a point on the map, filter by category, then open a place to see its details. Owners update their details now and then; places almost never move.',
    functional: [
      req('Find places within a distance of a point', ['nearby', 'near', 'radius', 'distance', 'location', 'geo']),
      req('Filter by category, rating and open now', ['filter', 'category', 'rating', 'open now']),
      req('Show the details of a place', ['detail', 'place page', 'hours', 'photos']),
      req('Owners update their place', ['owner', 'update', 'edit', 'business']),
    ],
    non_functional: [
      req('Searches answer in under 200 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('5,000 requests per second at peak', ['5,000', '5k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Search keeps working when a server or node fails', ['availability', 'available', 'redundan', 'failover', 'replica']),
      req('Owner updates show up within a few minutes', ['minute', 'fresh', 'stale', 'eventual', 'consisten']),
    ],
    constraints: ['70% of requests are searches and 30% are detail views', 'About 1% of requests are owner updates', 'Searches cluster around city centres, so many of them repeat'],
    scale: { users: 50000, peak_rps: 5000, latency_target_ms: 200, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 50000, requests_per_user: 0.1 },
    required: [
      { type: 'SearchIndex', why: 'Searching by distance and filters needs a geo index, not table scans.' },
      { type: 'Cache', why: 'Place details and popular searches are read far more often than they change.' },
      { type: 'LoadBalancer', why: 'One server cannot carry the peak.' },
      { type: 'Database', why: 'Place data needs a durable source of truth.' },
    ],
    concepts: ['search', 'caching', 'load-balancing', 'replication'],
    hints: [
      'A query like "latitude between X and Y" scans most of the table. What kind of index answers "near this point" quickly?',
      'People in the same city centre run nearly the same search. Could you reuse the answer for a minute?',
      'Places almost never move. How long could their details stay in a cache?',
      'If one search node dies, does search stop? Replicas keep a second copy of every shard.',
    ],
    stages: [
      {
        title: 'Scan the table for every search',
        goal: 'See why "near me" queries need a special index.',
        insights: ['Two servers handle about 2,000 requests per second, well short of the peak.', 'Without an index, every search scans the places table: about 4× less capacity and 5× slower queries.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'App Users', { users: 50000, requests_per_user: 0.1 });
          node('Server', 'app', 'Places API', { replicas: 2 });
          node('Database', 'db', 'Places DB', { read_ratio: 0.99, indexed: false });
          chain('users', 'app', 'db');
        }),
      },
      {
        title: 'A geo index behind a load balancer',
        goal: 'Answer distance searches from an index and spread the servers.',
        insights: ['A geo index answers "places near this point" by checking a few map cells instead of every row.', 'Eight servers cover the peak, but a spike still swamps them, and the index has no spare copy.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'App Users', { users: 50000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Places API', { replicas: 8 });
          node('SearchIndex', 'search', 'Geo Index', { shards: 2 });
          node('Database', 'db', 'Places DB', { read_ratio: 0.97, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app');
          link('app', 'search', { ratio: 0.7 });
          link('app', 'db', { ratio: 0.3 });
        }),
      },
      {
        title: 'Cache details and popular searches',
        goal: 'Stop repeating the same work for the same city centre.',
        insights: ['Searches around busy centres repeat, so a search cache with a one-minute TTL answers half of them.', 'Place details stay cached for an hour; the database only sees misses and owner updates.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'App Users', { users: 50000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Places API', { autoscaling: true, autoscale_min: 3, autoscale_max: 40 });
          node('Cache', 'results', 'Search Cache', { hit_ratio: 0.5, ttl_seconds: 60, ...HA_CACHE });
          node('SearchIndex', 'search', 'Geo Index', { shards: 2, replicas: 1 });
          node('Cache', 'details', 'Place Cache', { hit_ratio: 0.9, ttl_seconds: 3600, ...HA_CACHE });
          node('Database', 'db', 'Places DB', { read_ratio: 0.97, ...HA_DB });
          chain('users', 'lb', 'app');
          link('app', 'results', { ratio: 0.7 });
          link('results', 'search');
          link('app', 'details', { ratio: 0.3 });
          link('app', 'db', { ratio: 0.3 });
        }),
      },
      {
        title: 'Ready for spikes, hardened',
        goal: 'Leave room for six times the peak and lock the API down.',
        insights: ['Three shards with a replica each carry the biggest spike, and losing a node only removes one copy.', 'A rate limiter, HTTPS and API keys stop scrapers from copying the whole places database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'App Users', { users: 50000, requests_per_user: 0.1 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Places API', { autoscaling: true, autoscale_min: 4, autoscale_max: 50, auth: 'api_key' });
          node('Cache', 'results', 'Search Cache', { hit_ratio: 0.6, ttl_seconds: 60, ...HA_CACHE, encryption_at_rest: true });
          node('SearchIndex', 'search', 'Geo Index', { shards: 3, replicas: 1, encryption_at_rest: true });
          node('Cache', 'details', 'Place Cache', { hit_ratio: 0.9, ttl_seconds: 3600, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Places DB', { read_ratio: 0.97, ...HA_DB, ...SECURE_DB });
          chain('users', 'rl', 'lb', 'app');
          link('app', 'results', { ratio: 0.7 });
          link('results', 'search');
          link('app', 'details', { ratio: 0.3 });
          link('app', 'db', { ratio: 0.3 });
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 6 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'photo_sharing',
    title: 'Photo Sharing',
    difficulty: 'Intermediate',
    category: 'Social',
    summary: 'Upload photos, make thumbnails and serve a fast photo feed.',
    statement: 'People post photos and scroll a feed of photos from accounts they follow. Images make up most of the bytes and most of the requests; the feed API is called constantly; every upload needs thumbnails in several sizes before it shows up in feeds.',
    functional: [
      req('Upload a photo with a caption', ['upload', 'photo', 'caption', 'post']),
      req('Make thumbnails in several sizes', ['thumbnail', 'resize', 'size', 'resolution']),
      req('Show a feed of photos from followed accounts', ['feed', 'timeline', 'follow', 'home']),
      req('Like and comment on photos', ['like', 'comment', 'reaction']),
    ],
    non_functional: [
      req('Feed requests answer in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('15,000 requests per second at peak', ['15,000', '15k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Uploaded photos are never lost', ['durable', 'durability', 'never lost', 'replicat', 'persist']),
      req('Images load quickly all over the world', ['cdn', 'edge', 'global', 'worldwide', 'region']),
    ],
    constraints: ['About 70% of requests are image downloads', 'About 2% of API calls are uploads', 'Making the thumbnails for one photo takes about a second'],
    scale: { users: 100000, peak_rps: 15000, latency_target_ms: 300, regular_spike: 3, mega_spike: 5 },
    traffic: { users: 100000, requests_per_user: 0.15 },
    required: [
      { type: 'BlobStorage', why: 'Photos and thumbnails are large files.' },
      { type: 'CDN', why: 'Images never change once uploaded, so edges around the world can serve them.' },
      { type: 'Queue', why: 'Thumbnails should be made after the upload request has answered.' },
      { type: 'Cache', why: 'Feeds are read far more often than they change.' },
    ],
    concepts: ['cdn', 'queues', 'caching', 'sharding'],
    hints: [
      'Images are most of the traffic. Should they pass through your app servers at all?',
      'A photo never changes after upload. How long could a CDN keep it?',
      'Making thumbnails takes a second. Does the uploader need to wait for it?',
      'When the API grows five times, which runs out first: servers, cache or database writes?',
    ],
    stages: [
      {
        title: 'Images through the app servers',
        goal: 'See what happens when every image passes through your code.',
        insights: ['Every image download goes through an app server, so ten servers face 15,000 requests per second.', 'One database answers every feed request and holds every like and comment.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Users', { users: 100000, requests_per_user: 0.15 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'App Servers', { replicas: 10 });
          node('BlobStorage', 'blob', 'Photo Storage');
          node('Database', 'db', 'Photos DB', { read_ratio: 0.95 });
          chain('users', 'lb', 'app');
          link('app', 'blob', { ratio: 0.7 });
          link('app', 'db', { ratio: 0.3 });
        }),
      },
      {
        title: 'Images from a CDN',
        goal: 'Send image downloads to the edge so the servers only handle the API.',
        insights: ['Images never change after upload, so the CDN answers 90% of them and storage only sees misses.', 'The servers now handle only feed traffic, but every feed read still goes to the database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Users', { users: 100000, requests_per_user: 0.15 });
          node('CDN', 'cdn', 'Image CDN', { cache_hit_ratio: 0.9, ttl_seconds: 86400 });
          node('BlobStorage', 'blob', 'Photo Storage', { capacity: 30000 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Feed API', { replicas: 8 });
          node('Database', 'db', 'Photos DB', { read_ratio: 0.95, ...HA_DB });
          link('users', 'cdn', { ratio: 0.7 });
          link('cdn', 'blob');
          link('users', 'lb', { ratio: 0.3 });
          chain('lb', 'app', 'db');
        }),
      },
      {
        title: 'Feed cache and background thumbnails',
        goal: 'Keep feed reads in memory and make thumbnails after the upload answers.',
        insights: ['Feeds are cached for a few minutes, so most feed reads never reach the database.', 'An upload answers as soon as the original is stored; workers make the thumbnails a moment later.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Users', { users: 100000, requests_per_user: 0.15 });
          node('CDN', 'cdn', 'Image CDN', { cache_hit_ratio: 0.9, ttl_seconds: 86400 });
          node('BlobStorage', 'blob', 'Photo Storage', { capacity: 30000 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Feed API', { autoscaling: true, autoscale_min: 4, autoscale_max: 30 });
          node('Cache', 'cache', 'Feed Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Photos DB', { read_ratio: 0.95, ...HA_DB });
          node('Queue', 'uploads', 'Upload Events');
          node('Worker', 'thumbs', 'Thumbnail Makers', { capacity: 50, base_latency: 1000, concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 12, idempotent: true });
          link('users', 'cdn', { ratio: 0.7 });
          link('cdn', 'blob');
          link('users', 'lb', { ratio: 0.3 });
          chain('lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'blob', { ratio: 0.02 });
          link('app', 'uploads', { ratio: 0.02 });
          chain('uploads', 'thumbs', 'blob');
        }),
      },
      {
        title: 'Shard the writes, harden',
        goal: 'Let likes, comments and uploads grow five times and protect stored photos.',
        insights: ['Two shards split likes, comments and uploads, so writes can grow five times.', 'A rate limiter, HTTPS, logins and encryption protect accounts and stored photos.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Users', { users: 100000, requests_per_user: 0.15 });
          node('CDN', 'cdn', 'Image CDN', { cache_hit_ratio: 0.9, ttl_seconds: 86400 });
          node('BlobStorage', 'blob', 'Photo Storage', { capacity: 30000, versioning: true, encryption_at_rest: true });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Feed API', { autoscaling: true, autoscale_min: 4, autoscale_max: 40, auth: 'jwt' });
          node('Cache', 'cache', 'Feed Cache', { hit_ratio: 0.85, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Photos DB', { read_ratio: 0.95, shards: 2, ...HA_DB, ...SECURE_DB });
          node('Queue', 'uploads', 'Upload Events', { encryption_at_rest: true });
          node('Worker', 'thumbs', 'Thumbnail Makers', { capacity: 50, base_latency: 1000, concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 12, idempotent: true });
          link('users', 'cdn', { ratio: 0.7 });
          link('cdn', 'blob');
          link('users', 'rl', { ratio: 0.3 });
          chain('rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'blob', { ratio: 0.02 });
          link('app', 'uploads', { ratio: 0.02 });
          chain('uploads', 'thumbs', 'blob');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 5, extra: [
      { id: 'cdn-purge', name: 'CDN edges emptied', tier: 'system', scenario: 'failure', typeOverrides: { CDN: { hitMult: 0 } }, weight: 1,
        description: 'A bad deploy empties every edge cache, so all image requests go to storage until the edges refill.' },
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'game_leaderboard',
    title: 'Game Leaderboard',
    difficulty: 'Intermediate',
    category: 'Gaming',
    summary: 'Rank hundreds of thousands of players live and survive the end-of-tournament rush.',
    statement: 'A mobile game posts each player\'s score after every match and shows the global top 100, the player\'s own rank and a friends board. When a tournament ends, almost everyone sends a final score and checks their rank within the same minute.',
    functional: [
      req('Submit a score after a match', ['submit', 'score', 'record', 'match']),
      req('Show the global top 100', ['top', 'global', 'leaderboard', 'top 100']),
      req('Show a player\'s own rank', ['own rank', 'my rank', 'position', 'rank']),
      req('Show a friends leaderboard', ['friend', 'social']),
    ],
    non_functional: [
      req('Rank requests answer in under 200 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('10,000 requests per second, and 6× when a tournament ends', ['10,000', '10k', 'rps', 'spike', '6x', '6×', 'burst']),
      req('No submitted score is ever lost', ['durable', 'never lost', 'persist', 'at least once', 'queue']),
      req('Ranks may lag a few seconds behind', ['eventual', 'second', 'lag', 'delay', 'consisten']),
    ],
    constraints: ['200,000 players online at peak', '40% of requests submit a score and 60% read ranks', 'Sorting millions of rows on every request is far too slow'],
    scale: { users: 200000, peak_rps: 10000, latency_target_ms: 200, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 200000, requests_per_user: 0.05 },
    required: [
      { type: 'Cache', why: 'An in-memory sorted set returns ranks without sorting millions of rows.' },
      { type: 'Queue', why: 'Scores arrive in bursts; a queue lets storage catch up.' },
      { type: 'Worker', why: 'Workers save scores in batches in the background.' },
      { type: 'Database', why: 'Scores must survive a cache restart.' },
    ],
    concepts: ['caching', 'queues', 'sharding', 'capacity-planning'],
    hints: [
      'Four in ten requests are writes. Will read replicas help with those?',
      'Sorting on every request is expensive. Which data structure keeps scores sorted in memory?',
      'Does the player have to wait for the score to reach the database?',
      'When a tournament ends, traffic jumps six times within a minute. What has to grow on its own?',
    ],
    stages: [
      {
        title: 'Rank with SQL',
        goal: 'See why sorting a table on every request does not scale.',
        insights: ['Four servers handle 4,000 requests per second, under half the peak.', 'Every rank request sorts the scores table, and 4,000 score writes per second land on one database.'],
        graph: design(({ node, chain }) => {
          node('User', 'players', 'Players', { users: 200000, requests_per_user: 0.05 });
          node('Server', 'app', 'Game API', { replicas: 4 });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.6 });
          chain('players', 'app', 'db');
        }),
      },
      {
        title: 'More servers, more replicas',
        goal: 'Try the usual fixes and see which part is still stuck.',
        insights: ['Replicas share the reads, but all 4,000 writes per second still land on one primary.', 'This is the classic write bottleneck: read replicas never help with writes.'],
        graph: design(({ node, chain }) => {
          node('User', 'players', 'Players', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Game API', { replicas: 12 });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.6, ...HA_DB });
          chain('players', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Ranks in memory, writes in the background',
        goal: 'Answer ranks from a sorted set and buffer score writes.',
        insights: ['A sorted set in memory returns any player\'s rank in about a millisecond.', 'Scores go on a queue. Writers update the sorted set and save scores in batches of ten, so the database sees a tenth of the writes.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'players', 'Players', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Game API', { autoscaling: true, autoscale_min: 6, autoscale_max: 40 });
          node('Cache', 'ranks', 'Rank Store', { hit_ratio: 0.97, ...HA_CACHE });
          node('Queue', 'scores', 'Score Queue');
          node('Worker', 'writer', 'Score Writers', { concurrency: 32, autoscaling: true, autoscale_min: 4, autoscale_max: 20, idempotent: true });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.6, shards: 2, read_replicas: 1, failover: true });
          chain('players', 'lb', 'app');
          link('app', 'ranks', { ratio: 0.6 });
          link('ranks', 'db', { readShare: 1 });
          link('app', 'scores', { ratio: 0.4 });
          chain('scores', 'writer', 'ranks');
          link('writer', 'db', { ratio: 0.1, readShare: 0 });
        }),
      },
      {
        title: 'Tournament-ready',
        goal: 'Survive six times the peak and stop fake scores.',
        insights: ['When a tournament ends, servers and writers scale out while the queue soaks up the burst.', 'The rate limiter stops scripts from flooding the board with fake scores, and logins tie every score to a player.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'players', 'Players', { users: 200000, requests_per_user: 0.05 });
          node('RateLimiter', 'rl', 'Rate Limiter', { replicas: 2, limit_per_client_rps: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Game API', { autoscaling: true, autoscale_min: 10, autoscale_max: 100, auth: 'jwt' });
          node('Cache', 'ranks', 'Rank Store', { hit_ratio: 0.97, replicas: 3, request_coalescing: true, encryption_at_rest: true });
          node('Queue', 'scores', 'Score Queue', { capacity: 100000, max_depth: 2000000, encryption_at_rest: true });
          node('Worker', 'writer', 'Score Writers', { concurrency: 32, autoscaling: true, autoscale_min: 4, autoscale_max: 40, idempotent: true });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.6, shards: 4, ...HA_DB, ...SECURE_DB });
          chain('players', 'rl', 'lb', 'app');
          link('app', 'ranks', { ratio: 0.6 });
          link('ranks', 'db', { readShare: 1 });
          link('app', 'scores', { ratio: 0.4 });
          chain('scores', 'writer', 'ranks');
          link('writer', 'db', { ratio: 0.1, readShare: 0 });
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 6, extra: [
      workerCrash('A score writer crashes', 'A score writer dies during a tournament. Scores should wait safely in the queue.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'hotel_booking',
    title: 'Hotel Booking',
    difficulty: 'Intermediate',
    category: 'Commerce',
    summary: 'Search hotels by city and dates, then book a room without selling it twice.',
    statement: 'Travellers search hotels by city and dates, compare rooms and book one. Searches outnumber bookings fifty to one. A room must never be sold twice, and the payment provider is slow and sometimes fails.',
    functional: [
      req('Search hotels by city, dates and guests', ['search', 'city', 'dates', 'availability']),
      req('Show hotel and room details', ['detail', 'room', 'hotel page', 'photos']),
      req('Book a room', ['book', 'reserve', 'reservation', 'hold']),
      req('Pay for a booking', ['pay', 'payment', 'charge', 'card']),
      req('Cancel a booking', ['cancel', 'refund']),
    ],
    non_functional: [
      req('Searches answer in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('8,000 requests per second at peak', ['8,000', '8k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('A room is never sold twice', ['double', 'twice', 'consisten', 'transaction', 'lock', 'oversell']),
      req('A slow payment provider never slows down search', ['isolat', 'async', 'timeout', 'circuit', 'decoupl', 'queue']),
    ],
    constraints: ['70% of requests are searches, 28% are hotel pages and 2% are bookings', 'The payment provider takes about a second and allows 1,000 calls per second', 'A booking holds the room for 10 minutes while the payment completes'],
    scale: { users: 40000, peak_rps: 8000, latency_target_ms: 300, regular_spike: 3, mega_spike: 5 },
    traffic: { users: 40000, requests_per_user: 0.2 },
    required: [
      { type: 'SearchIndex', why: 'Searching by city, dates and filters needs an index.' },
      { type: 'Cache', why: 'Hotel pages are read far more often than they change.' },
      { type: 'Database', why: 'Bookings need transactions so a room is never sold twice.' },
      { type: 'Queue', why: 'Payments should run after the room is held, outside the request.' },
    ],
    concepts: ['search', 'consistency', 'resilience-patterns', 'queues'],
    hints: [
      'Search is 70% of traffic. Should it run as SQL queries on the bookings database?',
      'Hotel pages hardly change. What could answer most of those reads?',
      'How do you stop two people from booking the last room at the same moment?',
      'The payment provider is slow and sometimes fails. Must the traveller wait for it?',
    ],
    stages: [
      {
        title: 'Search with SQL, pay inline',
        goal: 'See what searching a bookings database does to it.',
        insights: ['Searching by city and dates with SQL scans whole tables, so the database collapses long before the peak.', 'Bookings wait on the payment provider with no time limit.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Travellers', { users: 40000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Booking App', { replicas: 8 });
          node('Database', 'db', 'Hotels DB', { read_ratio: 0.98, indexed: false });
          node('ThirdPartyAPI', 'psp', 'Payment Provider', { provider: 'payment', capacity: 1000, base_latency: 1000, timeout_ms: 0 });
          chain('users', 'lb', 'app', 'db');
          link('app', 'psp', { ratio: 0.02 });
        }),
      },
      {
        title: 'A search index',
        goal: 'Move city-and-date search off the database.',
        insights: ['The search index answers city-and-date queries, so the database only serves hotel pages and bookings.', 'Payments still run inside the booking request, and the index has no spare copy.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Travellers', { users: 40000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Booking App', { replicas: 12 });
          node('SearchIndex', 'search', 'Hotel Search', { shards: 2 });
          node('Database', 'db', 'Hotels DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'psp', 'Payment Provider', { provider: 'payment', capacity: 1000, base_latency: 1000 });
          chain('users', 'lb', 'app');
          link('app', 'search', { ratio: 0.7 });
          link('app', 'db', { ratio: 0.3 });
          link('app', 'psp', { ratio: 0.02 });
        }),
      },
      {
        title: 'Cache hotel pages, pay in the background',
        goal: 'Serve hotel pages from memory and stop waiting on the payment provider.',
        insights: ['Hotel pages come from the cache; the database serves misses and bookings.', 'The booking service holds the room in a transaction, answers at once and leaves the payment to a worker.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Travellers', { users: 40000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Booking App', { autoscaling: true, autoscale_min: 4, autoscale_max: 30 });
          node('SearchIndex', 'search', 'Hotel Search', { shards: 3, replicas: 1 });
          node('Cache', 'cache', 'Hotel Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Service', 'booking', 'Booking Service', { replicas: 2 });
          node('Database', 'db', 'Hotels DB', { read_ratio: 0.99, ...HA_DB });
          node('Queue', 'payq', 'Payment Queue');
          node('Worker', 'payer', 'Payment Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'psp', 'Payment Provider', { provider: 'payment', capacity: 1000, base_latency: 1000, circuit_breaker: true });
          chain('users', 'lb', 'app');
          link('app', 'search', { ratio: 0.7 });
          link('app', 'cache', { ratio: 0.28 });
          link('app', 'db', { ratio: 0.28 });
          link('app', 'booking', { ratio: 0.02 });
          link('booking', 'db', { readShare: 0.5 });
          link('booking', 'payq');
          chain('payq', 'payer', 'psp');
        }),
      },
      {
        title: 'Ready for the holiday rush, hardened',
        goal: 'Carry five times the peak and never lose a confirmed booking.',
        insights: ['Four shards with two replicas each keep search fast at five times the peak.', 'Synchronous replication means a confirmed booking survives losing the primary; backups and encryption protect guest data.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Travellers', { users: 40000, requests_per_user: 0.2 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Booking App', { autoscaling: true, autoscale_min: 6, autoscale_max: 70, auth: 'jwt' });
          node('SearchIndex', 'search', 'Hotel Search', { shards: 4, replicas: 2, encryption_at_rest: true });
          node('Cache', 'cache', 'Hotel Cache', { hit_ratio: 0.85, ...HA_CACHE, encryption_at_rest: true });
          node('Service', 'booking', 'Booking Service', { autoscaling: true, autoscale_min: 2, autoscale_max: 10 });
          node('Database', 'db', 'Hotels DB', { read_ratio: 0.99, replication: 'sync', ...HA_DB, ...SECURE_DB });
          node('Queue', 'payq', 'Payment Queue', { max_depth: 1000000, encryption_at_rest: true });
          node('Worker', 'payer', 'Payment Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'psp', 'Payment Provider', { provider: 'payment', capacity: 1000, base_latency: 1000, circuit_breaker: true });
          chain('users', 'rl', 'lb', 'app');
          link('app', 'search', { ratio: 0.7 });
          link('app', 'cache', { ratio: 0.28 });
          link('app', 'db', { ratio: 0.28 });
          link('app', 'booking', { ratio: 0.02 });
          link('booking', 'db', { readShare: 0.5 });
          link('booking', 'payq');
          chain('payq', 'payer', 'psp');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 5, extra: [
      providerSlow('psp-slow', 'Payment provider slows down', 10, 'The payment provider takes ten times longer than usual. Search and bookings should keep working.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'code_judge',
    title: 'Online Code Judge',
    difficulty: 'Intermediate',
    category: 'Developer tools',
    summary: 'Run untrusted code against hidden tests and survive the last minutes of a contest.',
    statement: 'Programmers read a problem, write code in the browser and submit it. Each submission is compiled and run against hidden tests in a sandbox, which takes a couple of seconds. During weekly contests, submissions jump eightfold in the final minutes, and everyone keeps refreshing the standings.',
    functional: [
      req('Read problem statements', ['problem', 'statement', 'read', 'browse']),
      req('Submit code in several languages', ['submit', 'code', 'language', 'compile']),
      req('Run code in a sandbox against hidden tests', ['sandbox', 'run', 'execute', 'test case', 'isolat']),
      req('Show the verdict and the contest standings', ['verdict', 'result', 'standing', 'ranking', 'leaderboard']),
    ],
    non_functional: [
      req('Pages answer in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('3,000 requests per second, and 8× at the end of a contest', ['3,000', '3k', 'rps', 'spike', '8x', '8×', 'burst']),
      req('User code can never harm the platform', ['sandbox', 'isolat', 'security', 'container', 'untrusted']),
      req('No submission is lost or judged twice', ['durable', 'never lost', 'idempot', 'exactly once', 'duplicate']),
    ],
    constraints: ['90% of requests are page views, standings and verdict checks', 'About 300 submissions per second on a normal day', 'Judging one submission takes about 2 seconds of sandbox time'],
    scale: { users: 30000, peak_rps: 3000, latency_target_ms: 300, regular_spike: 3, mega_spike: 8 },
    traffic: { users: 30000, requests_per_user: 0.1 },
    required: [
      { type: 'Queue', why: 'Submissions wait for a free sandbox instead of holding a web request open.' },
      { type: 'Worker', why: 'Sandboxed runners take submissions off the queue and judge them.' },
      { type: 'Cache', why: 'Problem pages and standings are read constantly.' },
      { type: 'BlobStorage', why: 'Hidden test files are large and every runner reads them.' },
    ],
    concepts: ['queues', 'capacity-planning', 'caching', 'idempotency'],
    hints: [
      'Judging takes two seconds. What happens to a web server that waits for it?',
      'Contest endings bring eight times the submissions. What lets runners fall behind without losing work?',
      'Problem statements are the same for everyone. Where could they be served from?',
      'If a runner crashes halfway, the submission is delivered again. How do you avoid recording two verdicts?',
    ],
    stages: [
      {
        title: 'Judge inside the web request',
        goal: 'See what happens when a web request waits two seconds for a sandbox.',
        insights: ['Four sandboxes judge about 160 submissions per second, but 300 arrive.', 'Each submission holds a web request open for two seconds while it is judged.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Programmers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Judge App', { replicas: 4 });
          node('Service', 'sandbox', 'Sandbox', { capacity: 40, base_latency: 2000, replicas: 4 });
          node('Database', 'db', 'Judge DB', { read_ratio: 0.95 });
          chain('users', 'lb', 'app', 'db');
          link('app', 'sandbox', { ratio: 0.1 });
        }),
      },
      {
        title: 'Judge in the background',
        goal: 'Queue submissions and let runners work through them.',
        insights: ['A submission now answers as soon as it is queued, and the page checks back for the verdict.', 'Twenty runners judge 400 submissions per second. The end of a contest brings far more, and the small queue overflows.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Programmers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Judge App', { replicas: 6 });
          node('Queue', 'subs', 'Submission Queue');
          node('Worker', 'runner', 'Sandbox Runners', { capacity: 20, base_latency: 2000, replicas: 20 });
          node('Database', 'db', 'Judge DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app', 'db');
          link('app', 'subs', { ratio: 0.1 });
          chain('subs', 'runner');
          link('runner', 'db', { readShare: 0 });
        }),
      },
      {
        title: 'Cache pages, scale the runners',
        goal: 'Serve pages from memory and let runners grow with the queue.',
        insights: ['Problem pages and standings come from the cache.', 'Runners scale out as the queue grows, and a deep queue holds a burst for minutes instead of rejecting it.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Programmers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Judge App', { autoscaling: true, autoscale_min: 4, autoscale_max: 20 });
          node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Queue', 'subs', 'Submission Queue', { max_depth: 1000000 });
          node('Worker', 'runner', 'Sandbox Runners', { capacity: 20, base_latency: 2000, concurrency: 16, autoscaling: true, autoscale_min: 10, autoscale_max: 150 });
          node('BlobStorage', 'tests', 'Test Files');
          node('Database', 'db', 'Judge DB', { read_ratio: 0.98, ...HA_DB });
          chain('users', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'subs', { ratio: 0.1 });
          chain('subs', 'runner', 'tests');
          link('runner', 'db', { readShare: 0 });
        }),
      },
      {
        title: 'Contest-ready, locked down',
        goal: 'Survive eight times the peak and record exactly one verdict per submission.',
        insights: ['The CDN serves statements and static files, so half of all requests never reach your servers, and submissions are a fifth of what does.', 'Idempotent runners record one verdict per submission even when a crash makes the queue deliver it twice.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Programmers', { users: 30000, requests_per_user: 0.1 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.5 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Judge App', { autoscaling: true, autoscale_min: 4, autoscale_max: 30, auth: 'jwt' });
          node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.9, ...HA_CACHE, encryption_at_rest: true });
          node('Queue', 'subs', 'Submission Queue', { max_depth: 2000000, encryption_at_rest: true });
          node('Worker', 'runner', 'Sandbox Runners', { capacity: 20, base_latency: 2000, concurrency: 16, autoscaling: true, autoscale_min: 10, autoscale_max: 150, idempotent: true });
          node('BlobStorage', 'tests', 'Test Files', { encryption_at_rest: true });
          node('Database', 'db', 'Judge DB', { read_ratio: 0.98, shards: 4, ...HA_DB, ...SECURE_DB });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'subs', { ratio: 0.2 });
          chain('subs', 'runner', 'tests');
          link('runner', 'db', { readShare: 0 });
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 8, extra: [
      workerCrash('A sandbox runner crashes', 'A runner dies in the last minutes of a contest. Submissions should wait in the queue until another runner picks them up.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'web_crawler',
    title: 'Web Crawler',
    difficulty: 'Intermediate',
    category: 'Data',
    summary: 'Download billions of web pages politely, even when websites are slow.',
    statement: 'A search engine needs fresh copies of billions of web pages. A scheduler hands out URLs to crawl. Fetchers download the pages, parsers pull out text and new links, and URLs already crawled are skipped. Websites are slow and flaky, and must not be hammered.',
    functional: [
      req('Hand out URLs to crawl', ['frontier', 'schedule', 'url', 'seed']),
      req('Download pages', ['fetch', 'download', 'crawl', 'http']),
      req('Skip URLs already crawled', ['duplicate', 'dedup', 'seen', 'bloom']),
      req('Extract text and new links', ['parse', 'extract', 'link', 'text']),
      req('Respect robots.txt and crawl politely', ['robots', 'polite', 'per host', 'throttle', 'rate']),
    ],
    non_functional: [
      req('2,000 URLs per second, and 4× during a full recrawl', ['2,000', '2k', 'per second', 'throughput', 'scale']),
      req('Slow websites never stall the crawl', ['timeout', 'slow', 'circuit', 'isolat']),
      req('No downloaded page is lost', ['durable', 'never lost', 'persist', 'replicat']),
      req('A crash never restarts the crawl from scratch', ['resume', 'checkpoint', 'queue', 'idempot', 'restart']),
    ],
    constraints: ['The scheduler sends about 2,000 URLs per second', 'A page download takes about 800 ms, and about 1% of sites fail', 'About a third of discovered URLs were already crawled'],
    scale: { users: 100, peak_rps: 2000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 100, requests_per_user: 20 },
    required: [
      { type: 'Queue', why: 'The URL frontier holds work until a fetcher is free.' },
      { type: 'Worker', why: 'Fetchers and parsers do the slow work in the background.' },
      { type: 'BlobStorage', why: 'Raw pages are large and kept for the indexer.' },
      { type: 'Cache', why: 'A fast "crawled already?" check avoids downloading the same page twice.' },
    ],
    concepts: ['queues', 'resilience-patterns', 'idempotency', 'capacity-planning'],
    hints: [
      'A download takes almost a second. Should the scheduler wait for it?',
      'A third of URLs were already crawled. Where can you check that in a millisecond?',
      'Some websites take 20 seconds to answer. How long should a fetcher wait?',
      'If a fetcher crashes, how is its URL picked up again without saving the page twice?',
    ],
    stages: [
      {
        title: 'Fetch while the scheduler waits',
        goal: 'See what happens when the caller waits for every download.',
        insights: ['The scheduler waits almost a second for every page, and slow sites make it wait longer.', 'Two servers and one database both run at full capacity at the normal rate.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'scheduler', 'Crawl Scheduler', { users: 100, requests_per_user: 20 });
          node('Server', 'api', 'Fetch API', { replicas: 2 });
          node('ThirdPartyAPI', 'web', 'Websites', { provider: 'other', capacity: 50000, base_latency: 800, error_rate: 0.01, timeout_ms: 0 });
          node('Database', 'db', 'URL DB', { read_ratio: 0.8 });
          chain('scheduler', 'api', 'web');
          link('api', 'db');
        }),
      },
      {
        title: 'A URL frontier',
        goal: 'Queue URLs and let fetchers download at their own pace.',
        insights: ['URLs go on a queue and the scheduler moves on; fetchers download pages in the background.', 'Every URL is still checked against the database, which runs hot, and slow websites can hold a fetcher forever.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'scheduler', 'Crawl Scheduler', { users: 100, requests_per_user: 20 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Frontier API', { replicas: 4 });
          node('Database', 'db', 'URL DB', { read_ratio: 0.8, read_replicas: 1, failover: true });
          node('Queue', 'frontier', 'URL Frontier');
          node('Worker', 'fetcher', 'Fetchers', { concurrency: 64, replicas: 4, idempotent: true });
          node('ThirdPartyAPI', 'web', 'Websites', { provider: 'other', capacity: 50000, base_latency: 800, error_rate: 0.01, timeout_ms: 0 });
          node('BlobStorage', 'pages', 'Page Store');
          chain('scheduler', 'lb', 'api', 'db');
          link('api', 'frontier');
          chain('frontier', 'fetcher', 'web');
          link('fetcher', 'pages');
        }),
      },
      {
        title: 'Skip duplicates, parse in the background',
        goal: 'Check "crawled already?" in memory and split parsing from fetching.',
        insights: ['A cache of crawled URLs answers "seen it?" in a millisecond, so a third of URLs never reach the frontier.', 'Parsers pull out links in the background and save them in batches.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'scheduler', 'Crawl Scheduler', { users: 100, requests_per_user: 20 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Frontier API', { autoscaling: true, autoscale_min: 3, autoscale_max: 20 });
          node('Cache', 'seen', 'Seen URLs', { hit_ratio: 0.9, ttl_seconds: 86400, ...HA_CACHE });
          node('Database', 'db', 'URL DB', { read_ratio: 0.8, shards: 2, ...HA_DB });
          node('Queue', 'frontier', 'URL Frontier', { max_depth: 1000000 });
          node('Worker', 'fetcher', 'Fetchers', { concurrency: 64, autoscaling: true, autoscale_min: 4, autoscale_max: 20, idempotent: true });
          node('ThirdPartyAPI', 'web', 'Websites', { provider: 'other', capacity: 50000, base_latency: 800, error_rate: 0.01, timeout_ms: 0 });
          node('BlobStorage', 'pages', 'Page Store');
          node('Queue', 'parseq', 'Parse Queue');
          node('Worker', 'parser', 'Parsers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 20, idempotent: true });
          chain('scheduler', 'lb', 'api', 'seen');
          link('api', 'db');
          link('api', 'frontier', { ratio: 0.67 });
          chain('frontier', 'fetcher', 'web');
          link('fetcher', 'pages');
          link('fetcher', 'parseq');
          chain('parseq', 'parser');
          link('parser', 'db', { ratio: 0.1, readShare: 0 });
        }),
      },
      {
        title: 'Polite, resilient and secured',
        goal: 'Give up on slow sites quickly and protect the frontier.',
        insights: ['Fetchers give up on a site after 2 seconds, and a circuit breaker stops calling sites that keep failing.', 'The gateway checks the scheduler\'s key and caps its rate, so a runaway job cannot flood the frontier.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'scheduler', 'Crawl Scheduler', { users: 100, requests_per_user: 20 });
          node('APIGateway', 'gw', 'Frontier Gateway', { replicas: 2, rate_limiting: true, auth: 'api_key', https: true });
          node('Server', 'api', 'Frontier API', { autoscaling: true, autoscale_min: 3, autoscale_max: 20 });
          node('Cache', 'seen', 'Seen URLs', { hit_ratio: 0.9, ttl_seconds: 86400, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'URL DB', { read_ratio: 0.8, shards: 2, ...HA_DB, ...SECURE_DB });
          node('Queue', 'frontier', 'URL Frontier', { max_depth: 1000000, encryption_at_rest: true });
          node('Worker', 'fetcher', 'Fetchers', { concurrency: 64, autoscaling: true, autoscale_min: 4, autoscale_max: 30, idempotent: true, timeout_ms: 2000, retry_limit: 1, circuit_breaker: true });
          node('ThirdPartyAPI', 'web', 'Websites', { provider: 'other', capacity: 50000, base_latency: 800, error_rate: 0.01, timeout_ms: 2000, retry_limit: 0, circuit_breaker: true });
          node('BlobStorage', 'pages', 'Page Store', { encryption_at_rest: true });
          node('Queue', 'parseq', 'Parse Queue', { encryption_at_rest: true });
          node('Worker', 'parser', 'Parsers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 20, idempotent: true });
          chain('scheduler', 'gw', 'api', 'seen');
          link('api', 'db');
          link('api', 'frontier', { ratio: 0.67 });
          chain('frontier', 'fetcher', 'web');
          link('fetcher', 'pages');
          link('fetcher', 'parseq');
          chain('parseq', 'parser');
          link('parser', 'db', { ratio: 0.1, readShare: 0 });
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4, extra: [
      workerCrash('A fetcher crashes', 'A fetcher dies mid-crawl. Its URLs should go back on the queue for another fetcher.'),
      providerSlow('sites-slow', 'Websites slow down', 5, 'Many websites answer five times slower than usual. Time limits decide whether fetchers keep moving.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'ai_assistant',
    title: 'AI Chat Assistant',
    difficulty: 'Intermediate',
    category: 'AI',
    summary: 'Answer chat messages with a language model that is slow, rate limited and costly.',
    statement: 'People chat with an AI assistant. Each new message is answered by a large language model from an outside provider: answers take a second or two, the provider caps how many requests you may send, and every call costs money. Many questions are asked again and again, and some clients are scripts trying to use the model for free.',
    functional: [
      req('Send a message and get an AI answer', ['message', 'answer', 'prompt', 'chat', 'reply']),
      req('Keep the conversation history', ['history', 'conversation', 'context', 'store']),
      req('Reuse answers to repeated questions', ['cache', 'repeat', 'reuse', 'semantic']),
      req('Limit how much each user can send', ['limit', 'quota', 'rate', 'per user']),
    ],
    non_functional: [
      req('95% of answers arrive within 4 seconds', ['latency', 'second', 'p95', 'stream', '4 s']),
      req('2,000 requests per second at peak', ['2,000', '2k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Keep answering if one model provider fails', ['failover', 'fallback', 'second provider', 'redundan', 'availability']),
      req('Keep the cost per answer under control', ['cost', 'budget', 'cache', 'cheap', 'token']),
    ],
    constraints: ['Half of requests send a new message; the rest load chat history', 'The model takes about 1.5 seconds per answer', 'Each provider allows 1,500 requests per second', 'About 10% of traffic comes from scripts hammering the API'],
    scale: { users: 50000, peak_rps: 2000, latency_target_ms: 4000, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 50000, requests_per_user: 0.04, abusive_share: 0.1 },
    required: [
      { type: 'ThirdPartyAPI', why: 'Answers come from an outside language model.' },
      { type: 'Cache', why: 'Repeated questions can be answered without paying for another model call.' },
      { type: 'RateLimiter', why: 'Scripts must not burn through the model quota.' },
      { type: 'Database', why: 'Conversations have to be stored.' },
    ],
    concepts: ['caching', 'rate-limiting', 'resilience-patterns', 'redundancy'],
    hints: [
      'The provider allows 1,500 calls per second. How many does your biggest spike need?',
      'Many people ask the same thing. What if the second person got the first answer?',
      'One provider will have a bad day. What would let you switch to another one?',
      'A tenth of traffic is scripts. Where is the cheapest place to stop them?',
    ],
    stages: [
      {
        title: 'Call the model for every message',
        goal: 'See how quickly one provider limit runs out.',
        insights: ['Every new message calls the model, so a spike passes the provider\'s limit of 1,500 calls per second.', 'Scripts get answers too, burning paid model calls, and one database holds every conversation.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 50000, requests_per_user: 0.04 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Chat API', { replicas: 6 });
          node('ThirdPartyAPI', 'llm', 'Model Provider', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000 });
          node('Database', 'db', 'Chat History', { read_ratio: 0.5 });
          chain('users', 'lb', 'app');
          link('app', 'llm', { ratio: 0.5 });
          link('app', 'db');
        }),
      },
      {
        title: 'Cache repeated answers',
        goal: 'Answer repeated questions without paying for another model call.',
        insights: ['Four in ten new questions were asked before, so the cache answers them and only three in ten requests reach the model.', 'There is still one provider: if it goes down, every new message fails.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 50000, requests_per_user: 0.04 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Chat API', { autoscaling: true, autoscale_min: 3, autoscale_max: 10 });
          node('Cache', 'answers', 'Answer Cache', { hit_ratio: 0.4, ttl_seconds: 3600, ...HA_CACHE });
          node('ThirdPartyAPI', 'llm', 'Model Provider', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000 });
          node('Database', 'db', 'Chat History', { read_ratio: 0.5, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app');
          link('app', 'answers', { ratio: 0.5 });
          link('app', 'llm', { ratio: 0.3 });
          link('app', 'db');
        }),
      },
      {
        title: 'Two providers behind a model gateway',
        goal: 'Survive one provider going down.',
        insights: ['The model gateway spreads calls over two providers and sends everything to one if the other fails.', 'Chat history now limits growth: every message is a database write.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 50000, requests_per_user: 0.04 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Chat API', { autoscaling: true, autoscale_min: 3, autoscale_max: 20 });
          node('Cache', 'answers', 'Answer Cache', { hit_ratio: 0.4, ttl_seconds: 3600, ...HA_CACHE });
          node('APIGateway', 'gw', 'Model Gateway', { replicas: 2 });
          node('ThirdPartyAPI', 'llmA', 'Provider A', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000, circuit_breaker: true });
          node('ThirdPartyAPI', 'llmB', 'Provider B', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000, circuit_breaker: true });
          node('Database', 'db', 'Chat History', { read_ratio: 0.5, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app');
          link('app', 'answers', { ratio: 0.5 });
          link('app', 'gw', { ratio: 0.3 });
          link('gw', 'llmA');
          link('gw', 'llmB');
          link('app', 'db');
        }),
      },
      {
        title: 'Limits, scale and hardening',
        goal: 'Stop scripts at the edge and let history grow four times.',
        insights: ['The rate limiter turns away scripts before they cost a model call.', 'A sharded store keeps up with history writes; logins, HTTPS and encryption protect private conversations.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 50000, requests_per_user: 0.04 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 0.5 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Chat API', { autoscaling: true, autoscale_min: 3, autoscale_max: 20, auth: 'jwt' });
          node('Cache', 'answers', 'Answer Cache', { hit_ratio: 0.4, ttl_seconds: 3600, ...HA_CACHE, encryption_at_rest: true });
          node('APIGateway', 'gw', 'Model Gateway', { replicas: 2 });
          node('ThirdPartyAPI', 'llmA', 'Provider A', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000, circuit_breaker: true });
          node('ThirdPartyAPI', 'llmB', 'Provider B', { provider: 'llm', capacity: 1500, base_latency: 1500, error_rate: 0.01, timeout_ms: 10000, circuit_breaker: true });
          node('Database', 'db', 'Chat History', { engine: 'nosql', capacity: 5000, read_ratio: 0.5, shards: 2, ...HA_DB, ...SECURE_DB });
          chain('users', 'rl', 'lb', 'app');
          link('app', 'answers', { ratio: 0.5 });
          link('app', 'gw', { ratio: 0.3 });
          link('gw', 'llmA');
          link('gw', 'llmB');
          link('app', 'db');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4, extra: [
      providerDown('provider-outage', 'A model provider goes down', 'The busiest model provider stops answering. New messages should still get answers.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'payment_system',
    title: 'Payment System',
    difficulty: 'Advanced',
    category: 'Finance',
    summary: 'Charge cards exactly once, keep a perfect ledger and tell shops what happened.',
    statement: 'Online shops call your API to charge their customers\' cards. Shops retry when a request times out, so the same charge can arrive twice and must still happen only once. Card networks are slow and sometimes go down, the ledger must never lose an entry, and every shop expects a webhook with the result.',
    functional: [
      req('Charge a card through an API', ['charge', 'card', 'payment', 'api']),
      req('Refund a payment', ['refund', 'reverse', 'chargeback']),
      req('Record every money movement in a ledger', ['ledger', 'record', 'double-entry', 'journal', 'balance']),
      req('Tell the shop the result with a webhook', ['webhook', 'notify', 'callback', 'event']),
    ],
    non_functional: [
      req('A retried request never charges twice', ['idempot', 'exactly once', 'duplicate', 'twice', 'dedup']),
      req('The ledger never loses an entry', ['durable', 'never lose', 'consisten', 'sync', 'acid', 'replicat']),
      req('95% of charges answer in under 1.5 seconds', ['latency', 'second', 'p95', 'ms']),
      req('1,000 requests per second, and 5× on sale days', ['1,000', '1k', 'rps', 'throughput', 'spike', '5x', '5×']),
      req('Card data is protected', ['encrypt', 'pci', 'tokeni', 'security', 'tls']),
    ],
    constraints: ['60% of requests are charges and 40% are status checks', 'Every charge and a quarter of status checks call a card network, which takes about 600 ms', 'Each card network allows 3,000 calls per second', 'Shops retry about 2% of requests'],
    scale: { users: 2000, peak_rps: 1000, latency_target_ms: 1500, regular_spike: 3, mega_spike: 5 },
    traffic: { users: 2000, requests_per_user: 0.5 },
    required: [
      { type: 'APIGateway', why: 'Shops must be authenticated and rate limited at one front door.' },
      { type: 'Database', why: 'The ledger is the source of truth for money.' },
      { type: 'Cache', why: 'Idempotency keys are checked on every request and must be fast.' },
      { type: 'ThirdPartyAPI', why: 'Cards are charged through outside card networks.' },
      { type: 'Queue', why: 'Webhooks are sent after the charge, with retries.' },
    ],
    concepts: ['idempotency', 'consistency', 'resilience-patterns', 'security'],
    hints: [
      'A shop retries a charge after a timeout. How does your API know it has seen this request before?',
      'Which is worse for a ledger: a slower write, or a lost one? Which replication setting does that point to?',
      'One card network goes down. Could a router send charges to another one?',
      'A shop\'s webhook endpoint is down. Should that slow down charges?',
    ],
    stages: [
      {
        title: 'One server, one card network',
        goal: 'See which single points of failure a payment path has.',
        insights: ['Two servers and one database carry the normal load, but nothing is redundant.', 'A retried request is charged again, because nothing remembers requests already handled.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shops', 'Shops', { users: 2000, requests_per_user: 0.5 });
          node('Server', 'app', 'Payment App', { replicas: 2 });
          node('Database', 'db', 'Ledger DB', { read_ratio: 0.4 });
          node('ThirdPartyAPI', 'net', 'Card Network', { provider: 'payment', capacity: 3000, base_latency: 600 });
          chain('shops', 'app', 'db');
          link('app', 'net', { ratio: 0.7 });
        }),
      },
      {
        title: 'Idempotency keys and a safe ledger',
        goal: 'Make retries harmless and stop losing ledger entries.',
        insights: ['Each request carries an idempotency key; a repeat returns the first result instead of charging again.', 'Synchronous replication confirms a ledger write only once a replica has it, so a crash loses nothing.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shops', 'Shops', { users: 2000, requests_per_user: 0.5 });
          node('APIGateway', 'gw', 'API Gateway', { auth: 'api_key', https: true });
          node('Service', 'pay', 'Payment Service', { replicas: 4, retry_limit: 0, timeout_ms: 3000 });
          node('Cache', 'idem', 'Idempotency Keys', { hit_ratio: 0.9 });
          node('Database', 'db', 'Ledger DB', { read_ratio: 0.4, replication: 'sync', read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'net', 'Card Network', { provider: 'payment', capacity: 3000, base_latency: 600 });
          chain('shops', 'gw', 'pay', 'idem');
          link('pay', 'db');
          link('pay', 'net', { ratio: 0.7 });
        }),
      },
      {
        title: 'Webhooks with retries',
        goal: 'Tell shops about every result without slowing down charges.',
        insights: ['Results go on a queue; senders deliver webhooks and retry when a shop\'s endpoint is down.', 'All charges still depend on one card network.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shops', 'Shops', { users: 2000, requests_per_user: 0.5 });
          node('APIGateway', 'gw', 'API Gateway', { auth: 'api_key', https: true });
          node('Service', 'pay', 'Payment Service', { autoscaling: true, autoscale_min: 2, autoscale_max: 10, retry_limit: 0, timeout_ms: 3000 });
          node('Cache', 'idem', 'Idempotency Keys', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Ledger DB', { read_ratio: 0.4, replication: 'sync', shards: 2, read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'net', 'Card Network', { provider: 'payment', capacity: 3000, base_latency: 600, circuit_breaker: true });
          node('Queue', 'events', 'Payment Events');
          node('Worker', 'sender', 'Webhook Senders', { concurrency: 32, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'hooks', 'Shop Endpoints', { provider: 'other', capacity: 5000, base_latency: 300, error_rate: 0.05, circuit_breaker: true });
          chain('shops', 'gw', 'pay', 'idem');
          link('pay', 'db');
          link('pay', 'net', { ratio: 0.7 });
          link('pay', 'events', { ratio: 0.6 });
          chain('events', 'sender', 'hooks');
        }),
      },
      {
        title: 'Two card networks, hardened',
        goal: 'Survive a card network outage and five times the peak.',
        insights: ['A card router sends most charges to the main network and fails over to the backup when it goes down.', 'Four ledger shards carry sale-day writes; rate limits, encryption and backups protect money and card data.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shops', 'Shops', { users: 2000, requests_per_user: 0.5 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, rate_limiting: true, auth: 'api_key', https: true });
          node('Service', 'pay', 'Payment Service', { autoscaling: true, autoscale_min: 2, autoscale_max: 20, retry_limit: 0, timeout_ms: 3000 });
          node('Cache', 'idem', 'Idempotency Keys', { hit_ratio: 0.9, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Ledger DB', { read_ratio: 0.4, replication: 'sync', shards: 4, ...HA_DB, ...SECURE_DB });
          node('APIGateway', 'router', 'Card Router', { replicas: 2 });
          node('ThirdPartyAPI', 'netA', 'Main Card Network', { provider: 'payment', capacity: 3000, base_latency: 600, circuit_breaker: true });
          node('ThirdPartyAPI', 'netB', 'Backup Card Network', { provider: 'payment', capacity: 3000, base_latency: 600, circuit_breaker: true });
          node('Queue', 'events', 'Payment Events', { encryption_at_rest: true });
          node('Worker', 'sender', 'Webhook Senders', { concurrency: 32, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'hooks', 'Shop Endpoints', { provider: 'other', capacity: 5000, base_latency: 300, error_rate: 0.05, circuit_breaker: true });
          chain('shops', 'gw', 'pay', 'idem');
          link('pay', 'db');
          link('pay', 'router', { ratio: 0.7 });
          link('router', 'netA', { ratio: 0.6 });
          link('router', 'netB', { ratio: 0.4 });
          link('pay', 'events', { ratio: 0.6 });
          chain('events', 'sender', 'hooks');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 5, extra: [
      providerDown('network-outage', 'A card network goes down', 'The busiest card network stops answering. Charges should move to another network.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'metrics_monitoring',
    title: 'Metrics and Alerting',
    difficulty: 'Advanced',
    category: 'Infrastructure',
    summary: 'Collect metrics from every server, draw dashboards and page someone when things break.',
    statement: 'Agents on thousands of servers send batches of metrics every few seconds. Engineers watch dashboards, and alert rules page the on-call engineer when something looks wrong. Ingestion never stops and is almost all writes. When an incident starts, servers send more and engineers pile onto dashboards at the same time.',
    functional: [
      req('Accept metrics from agents', ['ingest', 'agent', 'collect', 'push', 'metric']),
      req('Store time series and roll up old data', ['time series', 'store', 'downsample', 'rollup', 'retention']),
      req('Show dashboards and graphs', ['dashboard', 'graph', 'query', 'chart']),
      req('Check alert rules and page the on-call engineer', ['alert', 'rule', 'page', 'notify', 'on-call']),
    ],
    non_functional: [
      req('10,000 batches per second, and 4× during incidents', ['10,000', '10k', 'writes', 'throughput', 'spike', '4x', '4×']),
      req('Dashboards answer in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('Alerts fire within a minute', ['minute', 'alert', 'delay', 'fast']),
      req('Ingestion never stops, even while storage is slow', ['availability', 'available', 'buffer', 'queue', 'durab']),
    ],
    constraints: ['Agents send about 10,000 batches per second', 'Engineers send about 500 dashboard queries per second', 'Recent data is queried far more than old data'],
    scale: { users: 11000, peak_rps: 10500, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 11000, requests_per_user: 0.9545 },
    required: [
      { type: 'Queue', why: 'A stream buffers ingestion so storage can fall behind for a moment without losing data.' },
      { type: 'Worker', why: 'Workers write points in batches and check alert rules.' },
      { type: 'Database', why: 'A sharded time-series store holds the points.' },
      { type: 'Cache', why: 'Dashboards ask for the same recent data again and again.' },
    ],
    concepts: ['queues', 'sharding', 'caching', 'observability'],
    hints: [
      'Almost every request is a write. What happens when they all go straight to one database?',
      'Could a buffer absorb bursts so storage can catch up?',
      'Writing points in batches of ten cuts database writes by ten. Who should do the batching?',
      'Dashboards and ingestion have very different traffic. Should they share servers?',
    ],
    stages: [
      {
        title: 'Every point straight to one database',
        goal: 'See what 10,000 writes per second do to a single database.',
        insights: ['Twelve servers barely keep up, and each batch becomes a database write.', 'One database takes about 2,000 writes per second, a fifth of what arrives.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'agents', 'Server Agents', { users: 10000, requests_per_user: 1 });
          node('User', 'engineers', 'Engineers', { users: 1000, requests_per_user: 0.5 });
          node('Server', 'api', 'Metrics API', { replicas: 12 });
          node('Database', 'db', 'Metrics DB', { read_ratio: 0.05 });
          chain('agents', 'api', 'db');
          link('engineers', 'api');
        }),
      },
      {
        title: 'Buffer writes in a stream',
        goal: 'Split ingestion from dashboards and let a queue absorb bursts.',
        insights: ['Ingestion only puts batches on a stream; writers save them to a time-series store with four shards.', 'Sixteen writers keep up at normal load, but an incident doubles ingestion and the stream fills up.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'agents', 'Server Agents', { users: 10000, requests_per_user: 1 });
          node('User', 'engineers', 'Engineers', { users: 1000, requests_per_user: 0.5 });
          node('LoadBalancer', 'ingest_lb', 'Ingest LB');
          node('Server', 'ingest', 'Ingest API', { autoscaling: true, autoscale_min: 6, autoscale_max: 20 });
          node('Queue', 'stream', 'Metrics Stream', { capacity: 100000, partitions: 32, max_depth: 1000000 });
          node('Worker', 'writer', 'Writers', { concurrency: 32, replicas: 16, idempotent: true });
          node('Database', 'tsdb', 'Time-Series DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.05, shards: 4 });
          node('Server', 'query', 'Query API', { replicas: 2 });
          chain('agents', 'ingest_lb', 'ingest', 'stream', 'writer');
          link('writer', 'tsdb', { readShare: 0 });
          chain('engineers', 'query', 'tsdb');
        }),
      },
      {
        title: 'Batch writes, cache dashboards',
        goal: 'Cut database writes by ten and keep dashboards fast.',
        insights: ['Writers save points in batches of ten, so the store sees a tenth of the writes.', 'Dashboards ask for the same recent windows, so a cache answers most of their queries.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'agents', 'Server Agents', { users: 10000, requests_per_user: 1 });
          node('User', 'engineers', 'Engineers', { users: 1000, requests_per_user: 0.5 });
          node('LoadBalancer', 'ingest_lb', 'Ingest LB');
          node('Server', 'ingest', 'Ingest API', { autoscaling: true, autoscale_min: 6, autoscale_max: 20 });
          node('Queue', 'stream', 'Metrics Stream', { capacity: 100000, partitions: 32, max_depth: 1000000 });
          node('Worker', 'writer', 'Writers', { concurrency: 32, autoscaling: true, autoscale_min: 8, autoscale_max: 40, idempotent: true });
          node('Database', 'tsdb', 'Time-Series DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.05, shards: 4, read_replicas: 1, failover: true });
          node('LoadBalancer', 'query_lb', 'Query LB');
          node('Server', 'query', 'Query API', { autoscaling: true, autoscale_min: 2, autoscale_max: 10 });
          node('Cache', 'cache', 'Dashboard Cache', { hit_ratio: 0.8, ...HA_CACHE });
          chain('agents', 'ingest_lb', 'ingest', 'stream', 'writer');
          link('writer', 'tsdb', { ratio: 0.1, readShare: 0 });
          chain('engineers', 'query_lb', 'query', 'cache');
          link('query', 'tsdb');
        }),
      },
      {
        title: 'Alerts that fire during incidents',
        goal: 'Page someone within a minute, even at four times the load.',
        insights: ['Points that match alert rules also go on an alert queue; evaluators page the on-call engineer through an outside provider.', 'Ingestion scales to four times the load, and the stream holds minutes of data if the writers fall behind.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'agents', 'Server Agents', { users: 10000, requests_per_user: 1 });
          node('User', 'engineers', 'Engineers', { users: 1000, requests_per_user: 0.5 });
          node('RateLimiter', 'rl', 'Rate Limiter', { replicas: 2 });
          node('LoadBalancer', 'ingest_lb', 'Ingest LB', { https: true });
          node('Server', 'ingest', 'Ingest API', { autoscaling: true, autoscale_min: 10, autoscale_max: 80, auth: 'api_key' });
          node('Queue', 'stream', 'Metrics Stream', { capacity: 200000, partitions: 64, max_depth: 5000000, encryption_at_rest: true });
          node('Worker', 'writer', 'Writers', { concurrency: 64, autoscaling: true, autoscale_min: 6, autoscale_max: 40, idempotent: true });
          node('Database', 'tsdb', 'Time-Series DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.05, shards: 4, ...HA_DB, ...SECURE_DB });
          node('Queue', 'alerts', 'Alert Queue', { encryption_at_rest: true });
          node('Worker', 'evaluator', 'Alert Evaluators', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'pager', 'Paging Provider', { provider: 'sms', capacity: 200, base_latency: 300, circuit_breaker: true });
          node('LoadBalancer', 'query_lb', 'Query LB', { https: true });
          node('Server', 'query', 'Query API', { autoscaling: true, autoscale_min: 2, autoscale_max: 10, auth: 'jwt' });
          node('Cache', 'cache', 'Dashboard Cache', { hit_ratio: 0.8, ...HA_CACHE, encryption_at_rest: true });
          chain('agents', 'rl', 'ingest_lb', 'ingest', 'stream', 'writer');
          link('writer', 'tsdb', { ratio: 0.1, readShare: 0 });
          link('ingest', 'alerts', { ratio: 0.05 });
          chain('alerts', 'evaluator');
          link('evaluator', 'pager', { ratio: 0.01 });
          chain('engineers', 'query_lb', 'query', 'cache');
          link('query', 'tsdb');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4, extra: [
      workerCrash('A writer crashes', 'A writer dies during an incident. Points should wait in the stream, not be lost.'),
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'file_sync',
    title: 'File Sync',
    difficulty: 'Advanced',
    category: 'Storage',
    summary: 'Keep files in sync across devices, moving only the chunks that changed.',
    statement: 'People keep folders in sync across laptops and phones. Files are split into chunks so only changed chunks travel. Devices often ask whether anything changed, upload and download chunks, and expect their other devices to hear about a change within seconds. A file must never be lost or corrupted.',
    functional: [
      req('Upload and download files in chunks', ['chunk', 'block', 'upload', 'download']),
      req('Find what changed since the last sync', ['change', 'delta', 'diff', 'sync', 'cursor']),
      req('Tell other devices about changes', ['notify', 'push', 'long poll', 'websocket', 'event']),
      req('Keep old versions and restore deleted files', ['version', 'history', 'restore', 'undelete']),
      req('Share folders with other people', ['share', 'permission', 'collaborat']),
    ],
    non_functional: [
      req('Change checks answer in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('10,000 requests per second at peak', ['10,000', '10k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Files are never lost or corrupted', ['durable', 'never lost', 'corrupt', 'checksum', 'replicat']),
      req('A chunk that already exists is never uploaded again', ['dedup', 'hash', 'content address', 'bandwidth']),
    ],
    constraints: ['200,000 devices online at peak', '60% of requests are change checks, 30% move chunks and 10% save file changes', 'Chunks are 4 MB and stored once, by their content hash'],
    scale: { users: 200000, peak_rps: 10000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 200000, requests_per_user: 0.05 },
    required: [
      { type: 'BlobStorage', why: 'Chunks are large binary objects.' },
      { type: 'Database', why: 'File metadata and versions need a consistent store.' },
      { type: 'Cache', why: 'Change checks repeat constantly and usually find nothing new.' },
      { type: 'Queue', why: 'Change notifications go out to other devices in the background.' },
    ],
    concepts: ['sharding', 'caching', 'queues', 'consistency'],
    hints: [
      'Chunks are big and metadata is small. Should one server handle both?',
      'Most change checks find nothing new. What could answer them without the database?',
      'Saving a file change is a write. When writes grow four times, what spreads them?',
      'How does the phone hear about a change made on the laptop without asking every second?',
    ],
    stages: [
      {
        title: 'Files through one small pool',
        goal: 'See where a simple file server runs out.',
        insights: ['Four servers handle 4,000 requests per second, under half the peak.', 'Every change check is a database read, and every chunk passes through the same servers.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'devices', 'Devices', { users: 200000, requests_per_user: 0.05 });
          node('Server', 'app', 'Sync Server', { replicas: 4 });
          node('BlobStorage', 'blob', 'Chunk Storage');
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.86 });
          chain('devices', 'app');
          link('app', 'db', { ratio: 0.7 });
          link('app', 'blob', { ratio: 0.3 });
        }),
      },
      {
        title: 'Metadata and chunks apart',
        goal: 'Give chunks their own service and cache the change checks.',
        insights: ['A block service moves chunks, so the sync API only handles small metadata requests.', 'The metadata cache answers most change checks, but a fixed pool of twelve servers cannot take a spike.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'devices', 'Devices', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Sync API', { replicas: 12 });
          node('Cache', 'cache', 'Metadata Cache', { hit_ratio: 0.85 });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.86, read_replicas: 1, failover: true });
          node('Service', 'blocks', 'Block Service', { replicas: 4 });
          node('BlobStorage', 'blob', 'Chunk Storage', { capacity: 30000 });
          chain('devices', 'lb', 'api');
          link('api', 'cache', { ratio: 0.7 });
          link('api', 'db', { ratio: 0.7 });
          link('api', 'blocks', { ratio: 0.3 });
          chain('blocks', 'blob');
        }),
      },
      {
        title: 'Notify devices, scale out',
        goal: 'Push changes to other devices and let every tier grow.',
        insights: ['Each saved change goes on a queue; notifiers push it to the user\'s other devices.', 'At four times the load, two metadata shards cannot keep up with the writes, and the autoscaling limits are too low.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'devices', 'Devices', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Sync API', { autoscaling: true, autoscale_min: 6, autoscale_max: 40 });
          node('Cache', 'cache', 'Metadata Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.86, shards: 2, ...HA_DB });
          node('Service', 'blocks', 'Block Service', { autoscaling: true, autoscale_min: 2, autoscale_max: 10 });
          node('BlobStorage', 'blob', 'Chunk Storage', { capacity: 30000 });
          node('Queue', 'changes', 'Change Events');
          node('Worker', 'notifier', 'Notifiers', { concurrency: 32, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000, base_latency: 200, circuit_breaker: true });
          chain('devices', 'lb', 'api');
          link('api', 'cache', { ratio: 0.7 });
          link('api', 'db', { ratio: 0.7 });
          link('api', 'blocks', { ratio: 0.3 });
          chain('blocks', 'blob');
          link('api', 'changes', { ratio: 0.1 });
          chain('changes', 'notifier', 'push');
        }),
      },
      {
        title: 'Never lose a byte',
        goal: 'Carry four times the peak and protect every file.',
        insights: ['Four metadata shards spread the writes; versioning keeps old copies of every chunk.', 'Rate limits, HTTPS, logins and encryption protect private files.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'devices', 'Devices', { users: 200000, requests_per_user: 0.05 });
          node('RateLimiter', 'rl', 'Rate Limiter', { replicas: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'api', 'Sync API', { autoscaling: true, autoscale_min: 10, autoscale_max: 80, auth: 'oauth' });
          node('Cache', 'cache', 'Metadata Cache', { hit_ratio: 0.85, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.86, shards: 4, ...HA_DB, ...SECURE_DB });
          node('Service', 'blocks', 'Block Service', { autoscaling: true, autoscale_min: 4, autoscale_max: 20 });
          node('BlobStorage', 'blob', 'Chunk Storage', { capacity: 30000, versioning: true, encryption_at_rest: true });
          node('Queue', 'changes', 'Change Events', { encryption_at_rest: true });
          node('Worker', 'notifier', 'Notifiers', { concurrency: 32, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000, base_latency: 200, circuit_breaker: true });
          chain('devices', 'rl', 'lb', 'api');
          link('api', 'cache', { ratio: 0.7 });
          link('api', 'db', { ratio: 0.7 });
          link('api', 'blocks', { ratio: 0.3 });
          chain('blocks', 'blob');
          link('api', 'changes', { ratio: 0.1 });
          chain('changes', 'notifier', 'push');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'email_service',
    title: 'Email Service',
    difficulty: 'Advanced',
    category: 'Messaging',
    summary: 'Receive, filter, store, search and send email for hundreds of thousands of people.',
    statement: 'A webmail service. Mail from other servers arrives all day and must be checked for spam before it reaches an inbox. People read their inbox, open messages with attachments, search years of old mail and send new mail through an outbound relay that has its own limits.',
    functional: [
      req('Receive mail from other servers', ['receive', 'inbound', 'smtp', 'incoming']),
      req('Filter spam before delivery', ['spam', 'filter', 'phishing', 'virus']),
      req('Show the inbox and open messages', ['inbox', 'open', 'read', 'list']),
      req('Search old mail', ['search', 'find', 'full-text', 'query']),
      req('Send mail', ['send', 'outbound', 'compose', 'relay']),
    ],
    non_functional: [
      req('The inbox loads in under 300 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('9,000 requests per second at peak', ['9,000', '9k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('Accepted mail is never lost', ['durable', 'never lost', 'persist', 'replicat', 'queue']),
      req('Mail keeps arriving when spam checks are slow', ['buffer', 'queue', 'async', 'decoupl', 'isolat']),
    ],
    constraints: ['300,000 people use webmail at peak; other mail servers deliver about 1,500 messages per second', 'Bodies and attachments average 75 KB', 'A spam check takes about 200 ms per message'],
    scale: { users: 301500, peak_rps: 9000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 301500, requests_per_user: 0.02985 },
    required: [
      { type: 'Queue', why: 'Incoming mail is accepted and buffered before the slow spam check.' },
      { type: 'BlobStorage', why: 'Bodies and attachments are large.' },
      { type: 'SearchIndex', why: 'Searching years of mail needs a full-text index.' },
      { type: 'Database', why: 'Folders, flags and threads need a durable store.' },
      { type: 'Cache', why: 'The first page of the inbox is read constantly.' },
    ],
    concepts: ['queues', 'search', 'sharding', 'idempotency'],
    hints: [
      'Reading mail and receiving mail have very different traffic. Should they share servers?',
      'A spam check takes 200 ms. Should the sending server wait for it?',
      'Where should 75 KB bodies and attachments live?',
      'If a spam worker crashes, the message is delivered again. How do you avoid two copies in the inbox?',
    ],
    stages: [
      {
        title: 'One pool, one database',
        goal: 'See what happens when reading and receiving share everything.',
        insights: ['Eight servers handle 8,000 requests per second, a little under the peak.', 'Every inbox view, message and spam check hits one database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'readers', 'Mail Users', { users: 300000, requests_per_user: 0.025 });
          node('User', 'mailservers', 'Other Mail Servers', { users: 1500, requests_per_user: 1 });
          node('Server', 'app', 'Mail Server', { replicas: 8 });
          node('Database', 'db', 'Mail DB', { read_ratio: 0.85 });
          chain('readers', 'app', 'db');
          link('mailservers', 'app');
        }),
      },
      {
        title: 'Split reading from receiving',
        goal: 'Give webmail and inbound mail their own paths, and move bodies to blob storage.',
        insights: ['Bodies and attachments live in blob storage; the inbox cache answers most inbox views.', 'Inbound mail waits for a spam check that handles only 1,200 messages per second, so the SMTP servers stall.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'readers', 'Mail Users', { users: 300000, requests_per_user: 0.025 });
          node('User', 'mailservers', 'Other Mail Servers', { users: 1500, requests_per_user: 1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Mail API', { replicas: 10 });
          node('Cache', 'cache', 'Inbox Cache', { hit_ratio: 0.85 });
          node('Database', 'db', 'Mail DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
          node('BlobStorage', 'blob', 'Message Storage', { capacity: 30000 });
          node('Server', 'smtp', 'Inbound SMTP', { replicas: 2 });
          node('Service', 'spam', 'Spam Filter', { capacity: 300, base_latency: 200, replicas: 4 });
          chain('readers', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'blob', { ratio: 0.3 });
          chain('mailservers', 'smtp', 'spam', 'db');
          link('spam', 'blob');
        }),
      },
      {
        title: 'Accept mail first, filter later',
        goal: 'Queue inbound mail so spam checks can fall behind safely, and add search.',
        insights: ['Inbound mail is accepted straight onto a queue; spam workers check, store and index it.', 'A search index answers keyword search over years of mail, but two fixed SMTP servers cannot take a spike in incoming mail.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'readers', 'Mail Users', { users: 300000, requests_per_user: 0.025 });
          node('User', 'mailservers', 'Other Mail Servers', { users: 1500, requests_per_user: 1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Mail API', { autoscaling: true, autoscale_min: 4, autoscale_max: 30 });
          node('Cache', 'cache', 'Inbox Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Mail DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.95, shards: 2, ...HA_DB });
          node('BlobStorage', 'blob', 'Message Storage', { capacity: 30000 });
          node('SearchIndex', 'search', 'Mail Search', { shards: 2, replicas: 1 });
          node('Server', 'smtp', 'Inbound SMTP', { replicas: 2 });
          node('Queue', 'inbound', 'Inbound Queue', { max_depth: 1000000 });
          node('Worker', 'spam', 'Spam Workers', { capacity: 100, base_latency: 200, concurrency: 32, autoscaling: true, autoscale_min: 4, autoscale_max: 20, idempotent: true });
          chain('readers', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'blob', { ratio: 0.3 });
          link('api', 'search', { ratio: 0.1 });
          chain('mailservers', 'smtp', 'inbound', 'spam');
          link('spam', 'db', { readShare: 0 });
          link('spam', 'blob');
          link('spam', 'search');
        }),
      },
      {
        title: 'Outbound relay and hardening',
        goal: 'Send mail through a relay, carry four times the peak and protect every mailbox.',
        insights: ['Outgoing mail goes on its own queue; relay workers hand it to the outbound provider within its limits.', 'Both entry points use TLS and authentication, and everything stored is encrypted and backed up.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'readers', 'Mail Users', { users: 300000, requests_per_user: 0.025 });
          node('User', 'mailservers', 'Other Mail Servers', { users: 1500, requests_per_user: 1 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'api', 'Mail API', { autoscaling: true, autoscale_min: 6, autoscale_max: 60, auth: 'oauth' });
          node('Cache', 'cache', 'Inbox Cache', { hit_ratio: 0.85, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Mail DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.95, shards: 4, ...HA_DB, ...SECURE_DB });
          node('BlobStorage', 'blob', 'Message Storage', { capacity: 30000, encryption_at_rest: true });
          node('SearchIndex', 'search', 'Mail Search', { shards: 3, replicas: 1, encryption_at_rest: true });
          node('LoadBalancer', 'inbound_lb', 'Inbound LB', { https: true });
          node('Server', 'smtp', 'Inbound SMTP', { autoscaling: true, autoscale_min: 2, autoscale_max: 12, auth: 'api_key' });
          node('Queue', 'inbound', 'Inbound Queue', { max_depth: 2000000, encryption_at_rest: true });
          node('Worker', 'spam', 'Spam Workers', { capacity: 100, base_latency: 200, concurrency: 32, autoscaling: true, autoscale_min: 4, autoscale_max: 30, idempotent: true });
          node('Queue', 'outbound', 'Outbound Queue', { encryption_at_rest: true });
          node('Worker', 'relay', 'Relay Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          node('ThirdPartyAPI', 'relaysvc', 'Outbound Relay', { provider: 'email', capacity: 3000, base_latency: 300, circuit_breaker: true });
          chain('readers', 'rl', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'blob', { ratio: 0.3 });
          link('api', 'search', { ratio: 0.1 });
          link('api', 'outbound', { ratio: 0.05 });
          chain('outbound', 'relay', 'relaysvc');
          chain('mailservers', 'inbound_lb', 'smtp', 'inbound', 'spam');
          link('spam', 'db', { readShare: 0 });
          link('spam', 'blob');
          link('spam', 'search');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4, extra: [
      workerCrash('A spam worker crashes', 'A spam worker dies while mail is pouring in. Messages should wait in the queue and arrive once.'),
    ] }),
  },
];

// ================================================================ content: kafka problems
// Systems built around Kafka. Each one starts without Kafka (or with a naive setup) and
// grows until it survives broker loss, consumer crashes, outages, bad messages and schema
// changes while keeping its delivery promises.

const KAFKA_SECURE = { tls: true, client_auth: 'sasl_scram', acls: true };
const kafkaProviderDown = (id, name, description) => ({ id, name, tier: 'system', scenario: 'failure', prefer: ['ThirdPartyAPI'], ignore_freshness: true, weight: 1, description });
const kafkaProviderSlow = (id, name, provider, times, description) => ({ id, name, tier: 'system', scenario: 'failure', typeOverrides: { ThirdPartyAPI: { latencyMult: times, where: { provider } } }, weight: 1, description });

export const KAFKA_PROBLEMS = [
  // ---------------------------------------------------------------------------
  {
    id: 'clickstream_analytics',
    track: 'kafka',
    title: 'Live Clickstream Analytics',
    difficulty: 'Beginner',
    category: 'Kafka · Analytics',
    summary: 'Collect every page view and click, show what is trending within seconds, and keep the raw events for reports.',
    statement: 'A news site wants to know what readers do right now. Every page view and click is sent to a collection API. Editors watch a live dashboard of the most-read stories, updated within a few seconds, and analysts run daily reports over the raw events. The collection API must answer instantly, even when a story goes viral.',
    functional: [
      req('Collect page views and clicks from browsers', ['collect', 'event', 'click', 'page view', 'track', 'ingest']),
      req('Show the most-read stories, updated within seconds', ['dashboard', 'trending', 'most read', 'top', 'live']),
      req('Keep raw events for daily reports', ['archive', 'raw', 'report', 'batch', 'warehouse', 's3']),
    ],
    non_functional: [
      req('4,000 events per second at peak', ['4,000', '4k', 'events per second', 'throughput', 'scale']),
      req('The collection API answers in under 150 ms at p95', ['latency', 'ms', 'p95', 'fast']),
      req('Dashboard data is at most 5 seconds old', ['fresh', 'seconds', 'real-time', 'real time', 'lag']),
      req('No event is lost when a server or broker fails', ['durable', 'lost', 'replicat', 'acks', 'no data loss']),
    ],
    constraints: ['Each event is about 1 KB', 'A viral story triples the traffic within a minute', 'Reports may run hours later, so raw events must be kept'],
    scale: { users: 20000, peak_rps: 4000, latency_target_ms: 150, freshness_target_ms: 5000, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 20000, requests_per_user: 0.2 },
    required: [
      { type: 'KafkaTopic', why: 'A durable log absorbs bursts of events and lets several readers use them independently.' },
      { type: 'StreamProcessor', why: 'Counting views per story over short windows is a streaming job, not a database write per event.' },
      { type: 'KafkaConnect', why: 'A sink connector archives raw events without custom code.' },
    ],
    concepts: ['kafka-basics', 'kafka-partitions', 'kafka-consumers', 'kafka-streams'],
    hints: [
      'Does the browser need to wait until an event is stored in the database?',
      'Kafka keeps events for days. Who else could read the same events besides the dashboard?',
      'The dashboard needs counts per story, not every single click. Where should the counting happen?',
      'If one broker holds the only copy of a partition, what happens when it dies?',
    ],
    stages: [
      {
        title: 'Every click is a database write',
        goal: 'Find the first thing that breaks.',
        insights: ['Each event is an insert, so the database must take 4,000 writes a second; it manages about 2,500.', 'The API waits for every insert, so a slow database means a slow API.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Browsers', { users: 20000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Collect API', { replicas: 6 });
          node('Database', 'db', 'Events DB', { read_ratio: 0, capacity: 2500 });
          chain('users', 'lb', 'api', 'db');
        }),
      },
      {
        title: 'Kafka takes the clicks',
        goal: 'Let the API hand events to Kafka and answer straight away.',
        insights: ['The API answers as soon as Kafka has the event: a write to a topic takes a few milliseconds.', 'But a single broker holding the only copy is a single point of failure, and the writer still inserts every event into the database, which cannot keep up.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Browsers', { users: 20000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Collect API', { replicas: 6 });
          node('KafkaTopic', 'clicks', 'clicks', { partitions: 4, brokers: 1, replication_factor: 1, min_insync_replicas: 1, acks: '1', retention_hours: 12 });
          node('ConsumerGroup', 'writer', 'DB Writer', { replicas: 4, capacity: 1500 });
          node('Database', 'db', 'Events DB', { read_ratio: 0, capacity: 2500 });
          chain('users', 'lb', 'api', 'clicks', 'writer', 'db');
        }),
      },
      {
        title: 'Count in a stream, archive with a connector',
        goal: 'Replicate the topic, count views in a stream processor and archive raw events.',
        insights: ['Three brokers keep three copies; acks=all with min.insync.replicas 2 means one broker can die without losing a confirmed event.', 'The stream processor turns 4,000 clicks a second into about 200 story counts, and a sink connector copies the raw events to the archive.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Browsers', { users: 20000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Collect API', { autoscaling: true, autoscale_min: 6, autoscale_max: 20 });
          node('KafkaTopic', 'clicks', 'clicks', { partitions: 12, retention_hours: 12 });
          node('StreamProcessor', 'counter', 'Trend Counter', { replicas: 3, state: 'stateful', window: 'tumbling' });
          node('KafkaTopic', 'counts', 'story-counts', { partitions: 6, cleanup_policy: 'compact' });
          node('KafkaConnect', 'dbsink', 'Dashboard Sink', { mode: 'sink', connector: 'jdbc' });
          node('Database', 'db', 'Dashboard DB', { read_ratio: 0, capacity: 2500, read_replicas: 1, failover: true });
          node('KafkaConnect', 'archive', 'Archive Sink', { mode: 'sink', connector: 's3' });
          node('BlobStorage', 'lake', 'Event Archive');
          chain('users', 'lb', 'api', 'clicks', 'counter');
          link('counter', 'counts', { ratio: 0.05 });
          chain('counts', 'dbsink', 'db');
          link('clicks', 'archive');
          link('archive', 'lake', { ratio: 0.01 });
        }),
      },
      {
        title: 'Hardened for bad days',
        goal: 'Survive a day-long consumer outage, bad messages and schema changes.',
        insights: ['Three days of retention lets consumers come back from a long outage and carry on where they stopped.', 'Dead letter topics park bad messages, a Schema Registry rejects breaking format changes, and TLS, authentication and ACLs lock the cluster down.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Browsers', { users: 20000, requests_per_user: 0.2 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, auth: 'api_key', https: true, rate_limiting: true });
          node('Server', 'api', 'Collect API', { autoscaling: true, autoscale_min: 6, autoscale_max: 40 });
          node('KafkaTopic', 'clicks', 'clicks', { partitions: 12, retention_hours: 72, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry');
          node('StreamProcessor', 'counter', 'Trend Counter', { autoscaling: true, autoscale_min: 3, autoscale_max: 12, state: 'stateful', window: 'tumbling', standby_replicas: 1, dead_letter_topic: true });
          node('KafkaTopic', 'counts', 'story-counts', { partitions: 6, cleanup_policy: 'compact', ...KAFKA_SECURE });
          node('KafkaConnect', 'dbsink', 'Dashboard Sink', { mode: 'sink', connector: 'jdbc', dead_letter_topic: true });
          node('Database', 'db', 'Dashboard DB', { read_ratio: 0, capacity: 2500, ...HA_DB, ...SECURE_DB });
          node('KafkaConnect', 'archive', 'Archive Sink', { mode: 'sink', connector: 's3', replicas: 3, dead_letter_topic: true });
          node('BlobStorage', 'lake', 'Event Archive', { encryption_at_rest: true });
          chain('users', 'gw', 'api', 'clicks', 'counter');
          link('api', 'schemas');
          link('counter', 'schemas');
          link('counter', 'counts', { ratio: 0.05 });
          chain('counts', 'dbsink', 'db');
          link('clicks', 'archive');
          link('archive', 'lake', { ratio: 0.01 });
        }),
      },
    ],
    tests: kafkaTests({ spike: 3, mega: 6 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'order_events',
    track: 'kafka',
    title: 'Order Events with an Outbox',
    difficulty: 'Intermediate',
    category: 'Kafka · E-commerce',
    summary: 'Announce every order to payments, inventory and email: never lost, never charged twice, always in order.',
    statement: 'When a customer places an order, the Order Service saves it and tells the rest of the company. Payments charges the card, Inventory reserves the items and Email sends a confirmation. No order may be lost, nobody may be charged twice, the events of one order (placed, paid, shipped) must be handled in order, and the orders database and the events must never disagree, even if a service crashes halfway.',
    functional: [
      req('Place an order and save it', ['place', 'order', 'save', 'checkout']),
      req('Charge the payment once per order', ['payment', 'charge', 'card', 'once']),
      req('Reserve stock and send a confirmation email', ['inventory', 'stock', 'reserve', 'email', 'confirmation']),
      req('Let new services react to orders without changing the Order Service', ['decouple', 'subscribe', 'new service', 'fan out', 'event']),
    ],
    non_functional: [
      req('2,000 requests per second at peak', ['2,000', '2k', 'rps', 'throughput', 'scale']),
      req('Orders answer in under 300 ms at p95', ['latency', 'ms', 'p95']),
      req('No order event is ever lost or applied twice', ['exactly once', 'exactly-once', 'idempot', 'duplicate', 'lost']),
      req('Events of one order are processed in order', ['order', 'ordering', 'sequence', 'partition key', 'key']),
      req('The database and the events never disagree', ['outbox', 'cdc', 'dual write', 'consisten', 'transaction']),
    ],
    constraints: ['30% of requests change an order; the rest read order status', 'The payment provider allows 1,500 calls a second', 'Payments may take up to 10 seconds to start after an order'],
    scale: { users: 10000, peak_rps: 2000, latency_target_ms: 300, freshness_target_ms: 10000, regular_spike: 3, mega_spike: 8 },
    traffic: { users: 10000, requests_per_user: 0.2 },
    kafka: { ordered: true, schemas: true },
    required: [
      { type: 'KafkaTopic', why: 'One stream of order events that every interested service reads on its own.' },
      { type: 'KafkaConnect', why: 'Change data capture publishes exactly what the database committed (the outbox pattern).' },
      { type: 'ConsumerGroup', why: 'Each downstream service reads the events at its own pace.' },
      { type: 'SchemaRegistry', why: 'Many teams read order events; a format change must not break them.' },
    ],
    concepts: ['kafka-delivery', 'kafka-connect-cdc', 'kafka-partitions', 'kafka-schemas'],
    hints: [
      'If the Order Service calls Payments, Inventory and Email itself, what happens when one of them is down?',
      'The service saves the order and then publishes an event. What if it crashes in between?',
      'Kafka keeps order within a partition only. Which key keeps one order\'s events together?',
      'After a consumer crash, some events come back. How do you make charging the same event twice harmless?',
    ],
    stages: [
      {
        title: 'The order service calls everyone',
        goal: 'See why calling every service directly is fragile.',
        insights: ['Every order waits for the payment provider, the inventory service and the email provider.', 'If any of them is down, placing an order fails, and every new consumer means changing the Order Service.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers', { users: 10000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'orders', 'Order Service', { replicas: 4 });
          node('Database', 'db', 'Orders DB', { read_ratio: 0.7, ...HA_DB });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          node('Service', 'stock', 'Inventory Service', { replicas: 2 });
          node('Database', 'stockdb', 'Inventory DB', { read_ratio: 0.2, ...HA_DB });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 1000 });
          chain('users', 'lb', 'orders', 'db');
          link('orders', 'pay', { ratio: 0.3 });
          link('orders', 'stock', { ratio: 0.3 });
          link('orders', 'mail', { ratio: 0.3 });
          link('stock', 'stockdb');
        }),
      },
      {
        title: 'Publish an event, everyone listens',
        goal: 'Decouple the services with a topic.',
        insights: ['Orders are accepted even when the payment provider is down; payments catch up later.', 'But the service writes the database and Kafka separately, messages have no key (no ordering), acks=1 can lose events, and a consumer crash charges some cards twice.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers', { users: 10000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'orders', 'Order Service', { replicas: 4 });
          node('Database', 'db', 'Orders DB', { read_ratio: 0.7, ...HA_DB });
          node('KafkaTopic', 'events', 'order-events', { partition_key: 'none', acks: '1', idempotent_producer: false });
          node('ConsumerGroup', 'payments', 'Payments', { replicas: 3, capacity: 400 });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          node('ConsumerGroup', 'inventory', 'Inventory', { replicas: 3 });
          node('Database', 'stockdb', 'Inventory DB', { read_ratio: 0.2, ...HA_DB });
          node('ConsumerGroup', 'emails', 'Emails', { replicas: 3 });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 1000 });
          chain('users', 'lb', 'orders', 'db');
          link('orders', 'events', { ratio: 0.3 });
          chain('events', 'payments', 'pay');
          chain('events', 'inventory', 'stockdb');
          chain('events', 'emails', 'mail');
        }),
      },
      {
        title: 'Outbox, keys and idempotent consumers',
        goal: 'Publish what the database committed, keep each order in order, and make replays harmless.',
        insights: ['The Order Service only writes the database; CDC streams each committed change, so the events can never disagree with the data.', 'Keyed by order id, all events of an order share a partition. Idempotent consumers ignore events they already handled.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Shoppers', { users: 10000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'orders', 'Order Service', { replicas: 4 });
          node('Database', 'db', 'Orders DB', { read_ratio: 0.7, ...HA_DB });
          node('KafkaConnect', 'cdc', 'Outbox CDC', { mode: 'source', connector: 'cdc', exactly_once: true });
          node('KafkaTopic', 'events', 'order-events', { partitions: 12 });
          node('ConsumerGroup', 'payments', 'Payments', { replicas: 4, capacity: 400, idempotent: true });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          node('ConsumerGroup', 'inventory', 'Inventory', { replicas: 4, idempotent: true });
          node('Database', 'stockdb', 'Inventory DB', { read_ratio: 0.2, ...HA_DB });
          node('ConsumerGroup', 'emails', 'Emails', { replicas: 4, idempotent: true });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 1000 });
          chain('users', 'lb', 'orders', 'db', 'cdc', 'events');
          chain('events', 'payments', 'pay');
          chain('events', 'inventory', 'stockdb');
          chain('events', 'emails', 'mail');
        }),
      },
      {
        title: 'Bad messages and new schemas, handled',
        goal: 'Stop a malformed event or a careless schema change from stopping the company.',
        insights: ['Dead letter topics park events that keep failing, so one bad order never blocks the others; a sharded database and an autoscaled service ride out sales spikes.', 'A Schema Registry with backward compatibility rejects a breaking change before any consumer sees it; TLS, authentication and ACLs restrict who may read orders.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers', { users: 10000, requests_per_user: 0.2 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, auth: 'jwt', https: true, rate_limiting: true });
          node('Service', 'orders', 'Order Service', { autoscaling: true, autoscale_min: 4, autoscale_max: 20 });
          node('Database', 'db', 'Orders DB', { read_ratio: 0.7, capacity: 4000, shards: 3, ...HA_DB, ...SECURE_DB });
          node('KafkaConnect', 'cdc', 'Outbox CDC', { mode: 'source', connector: 'cdc', exactly_once: true });
          node('KafkaTopic', 'events', 'order-events', { partitions: 12, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry');
          node('ConsumerGroup', 'payments', 'Payments', { replicas: 4, capacity: 400, idempotent: true, dead_letter_topic: true });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500, circuit_breaker: true });
          node('ConsumerGroup', 'inventory', 'Inventory', { replicas: 4, idempotent: true, dead_letter_topic: true });
          node('Database', 'stockdb', 'Inventory DB', { read_ratio: 0.2, ...HA_DB, ...SECURE_DB });
          node('ConsumerGroup', 'emails', 'Emails', { replicas: 4, idempotent: true, dead_letter_topic: true });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 1000, circuit_breaker: true });
          chain('users', 'gw', 'orders', 'db', 'cdc', 'events');
          link('cdc', 'schemas');
          chain('events', 'payments', 'pay');
          chain('events', 'inventory', 'stockdb');
          chain('events', 'emails', 'mail');
          link('payments', 'schemas');
        }),
      },
    ],
    tests: kafkaTests({
      spike: 3, mega: 8,
      requires: { normal: ['ordered', 'consistent'], crash: ['no-duplicates'] },
      extra: [kafkaProviderDown('payment-outage', 'Payment provider down', 'The payment provider is down. Orders must still be accepted; payments catch up when it returns.')],
    }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'ride_locations',
    track: 'kafka',
    title: 'Ride-hailing Driver Locations',
    difficulty: 'Intermediate',
    category: 'Kafka · Location',
    summary: 'Stream 50,000 driver positions a second so riders and the matcher always see where cars are now.',
    statement: 'Every active driver\'s phone sends its position every four seconds. Riders watch nearby cars move on the map, and the matching service picks the closest driver for each request. Positions older than a couple of seconds are useless, and one driver\'s positions must never arrive out of order, or cars would jump backwards on the map.',
    functional: [
      req('Receive a position from every active driver every 4 seconds', ['position', 'location', 'gps', 'ping', 'update']),
      req('Keep the latest position of every driver queryable by area', ['geo', 'nearby', 'index', 'latest', 'area']),
      req('Let the matcher find the closest drivers', ['match', 'closest', 'nearest', 'dispatch']),
    ],
    non_functional: [
      req('50,000 position updates per second', ['50,000', '50k', 'throughput', 'per second', 'scale']),
      req('Positions are at most 2 seconds old when used', ['fresh', 'seconds', 'real-time', 'lag', 'latency']),
      req('One driver\'s positions are applied in order', ['order', 'ordering', 'key', 'partition']),
    ],
    constraints: ['200,000 drivers online at peak', 'A position message is about 200 bytes', 'One big city produces a third of all updates'],
    scale: { users: 200000, peak_rps: 50000, latency_target_ms: 100, freshness_target_ms: 2000, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 200000, requests_per_user: 0.25 },
    kafka: { ordered: true },
    required: [
      { type: 'KafkaTopic', why: 'A partitioned log spreads 50,000 updates a second over many machines.' },
      { type: 'StreamProcessor', why: 'Keeps the latest position per driver and updates the geo index.' },
      { type: 'Cache', why: 'An in-memory geo index answers "drivers near this point" in a millisecond.' },
    ],
    concepts: ['kafka-partitions', 'kafka-consumers', 'kafka-rebalancing', 'kafka-streams'],
    hints: [
      'A database cannot take 50,000 writes a second for data that is stale after two seconds. Where should positions live?',
      'If messages are keyed by city, what happens to the partition that holds the biggest city?',
      'Which key keeps one driver\'s positions in order but spreads the load evenly?',
      'With exactly one partition per consumer, what happens to the load when one consumer dies?',
    ],
    stages: [
      {
        title: 'Every ping updates the database',
        goal: 'Find the first thing that breaks.',
        insights: ['50,000 updates a second is more than twice what two database shards can write.', 'And the data is useless after two seconds, so durable rows are the wrong tool anyway.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Driver Phones', { users: 200000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Location API', { replicas: 14, capacity: 5000, base_latency: 5 });
          node('Database', 'db', 'Positions DB', { engine: 'nosql', read_ratio: 0, capacity: 10000, shards: 2 });
          chain('users', 'lb', 'api', 'db');
        }),
      },
      {
        title: 'A topic keyed by city',
        goal: 'Stream positions through Kafka into an in-memory geo index.',
        insights: ['Kafka takes the 50,000 writes easily, and a stream processor keeps the geo index current.', 'But keyed by city, the biggest city\'s third of all updates lands on one partition and one consumer, which falls hopelessly behind.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Driver Phones', { users: 200000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Location API', { replicas: 14, capacity: 5000, base_latency: 5 });
          node('KafkaTopic', 'positions', 'driver-positions', { partitions: 12, message_kb: 0.2, partition_key: 'skewed', hot_key_share: 0.35, retention_hours: 24 });
          node('StreamProcessor', 'indexer', 'Geo Indexer', { replicas: 12 });
          node('Cache', 'geo', 'Driver Geo Index', { replicas: 2, hit_ratio: 1 });
          chain('users', 'lb', 'api', 'positions', 'indexer', 'geo');
        }),
      },
      {
        title: 'Key by driver, scale out',
        goal: 'Spread the load evenly and let consumers scale with it.',
        insights: ['Keyed by driver id, every partition gets an even share, and one driver\'s positions still stay in order.', 'Forty-eight partitions let up to 48 indexers work at once; the autoscaler adds them as traffic grows.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Driver Phones', { users: 200000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Location API', { autoscaling: true, autoscale_min: 14, autoscale_max: 60, capacity: 5000, base_latency: 5 });
          node('KafkaTopic', 'positions', 'driver-positions', { partitions: 48, message_kb: 0.2, retention_hours: 24 });
          node('StreamProcessor', 'indexer', 'Geo Indexer', { autoscaling: true, autoscale_min: 30, autoscale_max: 48 });
          node('Cache', 'geo', 'Driver Geo Index', { replicas: 2, hit_ratio: 1 });
          chain('users', 'lb', 'api', 'positions', 'indexer', 'geo');
        }),
      },
      {
        title: 'Room to lose a consumer',
        goal: 'Survive a crashed indexer, bad messages and schema changes.',
        insights: ['With two partitions per indexer, a crash spreads the lost indexer\'s partitions over the others instead of doubling one indexer\'s load.', 'A dead letter topic, a Schema Registry and a locked-down cluster handle bad messages, format changes and rogue clients.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Driver Phones', { users: 200000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 5, https: true });
          node('Server', 'api', 'Location API', { autoscaling: true, autoscale_min: 14, autoscale_max: 60, capacity: 5000, base_latency: 5, auth: 'jwt', https: true });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 1, replicas: 4 });
          node('KafkaTopic', 'positions', 'driver-positions', { partitions: 96, message_kb: 0.2, retention_hours: 24, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry', { format: 'protobuf' });
          node('StreamProcessor', 'indexer', 'Geo Indexer', { autoscaling: true, autoscale_min: 30, autoscale_max: 96, dead_letter_topic: true });
          node('Cache', 'geo', 'Driver Geo Index', { replicas: 3, hit_ratio: 1, encryption_at_rest: true });
          chain('users', 'rl', 'lb', 'api', 'positions', 'indexer', 'geo');
          link('api', 'schemas');
        }),
      },
    ],
    tests: kafkaTests({ spike: 2, mega: 4, outageHours: 0, db: false, requires: { normal: ['ordered'] } }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'bank_ledger',
    track: 'kafka',
    title: 'Bank Transfer Ledger',
    difficulty: 'Advanced',
    category: 'Kafka · Fintech',
    summary: 'Move money between accounts through Kafka with exactly-once processing: no lost, duplicated or reordered transfers.',
    statement: 'A bank lets customers send money to each other. A transfer debits one account and credits another. Balances are computed by a ledger that reads every transfer, and a fraud checker scores each one. A transfer must never be lost or applied twice, the transfers of one account must be applied in order, and the bank must be able to prove that every recorded transfer was applied.',
    functional: [
      req('Accept a transfer between two accounts', ['transfer', 'send money', 'debit', 'credit']),
      req('Keep every account balance up to date', ['balance', 'ledger', 'account']),
      req('Score every transfer for fraud', ['fraud', 'risk', 'score']),
      req('Show balances and recent transfers', ['statement', 'history', 'balance', 'recent']),
    ],
    non_functional: [
      req('3,000 requests per second at peak', ['3,000', '3k', 'rps', 'throughput']),
      req('Balances reflect a transfer within 3 seconds', ['fresh', 'seconds', 'lag', 'real-time']),
      req('Exactly-once: no transfer lost or applied twice', ['exactly once', 'exactly-once', 'transaction', 'idempot', 'duplicate']),
      req('Transfers of one account are applied in order', ['order', 'ordering', 'key', 'account']),
    ],
    constraints: ['Half of the requests are transfers, half are balance reads', 'A double debit is a regulatory incident', 'The ledger keeps balances as state, rebuilt from its changelog if lost'],
    scale: { users: 30000, peak_rps: 3000, latency_target_ms: 300, freshness_target_ms: 3000, regular_spike: 3, mega_spike: 4 },
    traffic: { users: 30000, requests_per_user: 0.1 },
    kafka: { ordered: true, schemas: true },
    required: [
      { type: 'KafkaTopic', why: 'The durable, ordered log of every transfer is the source the ledger is built from.' },
      { type: 'StreamProcessor', why: 'An exactly-once stream job applies each transfer to balances exactly once.' },
      { type: 'KafkaConnect', why: 'CDC turns committed transfer rows into events (outbox), and a sink writes balances out.' },
      { type: 'SchemaRegistry', why: 'Ledger events are read by several teams and kept for years.' },
    ],
    concepts: ['kafka-delivery', 'kafka-replication', 'kafka-streams', 'kafka-connect-cdc'],
    hints: [
      'If the transfer service writes the database and then publishes, what happens when it crashes in between?',
      'At-least-once processing replays a transfer after a crash. What does that do to a balance?',
      'Which key keeps every transfer of an account in the same partition?',
      'What do acks=all and min.insync.replicas=2 promise when a broker dies?',
    ],
    stages: [
      {
        title: 'One big database transaction',
        goal: 'Find the first thing that breaks.',
        insights: ['Each transfer locks two balance rows and writes a history row, so the database does all the work on every request.', 'At 1,500 transfers a second the primary is overloaded, and fraud checks would make every transfer slower still.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Customers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Transfer Service', { replicas: 4 });
          node('Database', 'db', 'Ledger DB', { read_ratio: 0.5, capacity: 1500, ...HA_DB });
          chain('users', 'lb', 'api', 'db');
        }),
      },
      {
        title: 'Transfers on a topic, at least once',
        goal: 'Apply transfers in a stream processor instead of in the request.',
        insights: ['The service records the transfer and publishes it; a ledger job applies it to balances.', 'But it writes the database and Kafka separately, without a key or acks=all, and at-least-once processing applies a replayed transfer twice: a double debit.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Customers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Transfer Service', { replicas: 4 });
          node('Database', 'db', 'Transfers DB', { read_ratio: 0.5, capacity: 4000, ...HA_DB });
          node('KafkaTopic', 'transfers', 'transfers', { partition_key: 'none', acks: '1' });
          node('StreamProcessor', 'ledger', 'Ledger', { replicas: 3, state: 'stateful' });
          node('KafkaTopic', 'balances', 'balances', { cleanup_policy: 'compact' });
          node('KafkaConnect', 'sink', 'Balance Sink', { mode: 'sink', connector: 'jdbc' });
          node('Database', 'baldb', 'Balances DB', { read_ratio: 0, ...HA_DB });
          chain('users', 'lb', 'api', 'db');
          link('api', 'transfers', { ratio: 0.5 });
          chain('transfers', 'ledger', 'balances', 'sink', 'baldb');
        }),
      },
      {
        title: 'Outbox, keys and exactly-once',
        goal: 'Make every transfer count exactly once, in order.',
        insights: ['CDC publishes only committed transfers (the outbox), keyed by account so each account\'s transfers stay in order.', 'The ledger runs exactly-once: reading a transfer, updating the balance and writing the result commit together, so a crash never applies a transfer twice.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Customers', { users: 30000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Transfer Service', { replicas: 4 });
          node('Database', 'db', 'Transfers DB', { read_ratio: 0.5, capacity: 4000, ...HA_DB });
          node('KafkaConnect', 'cdc', 'Outbox CDC', { mode: 'source', connector: 'cdc', exactly_once: true });
          node('KafkaTopic', 'transfers', 'transfers', { partitions: 12 });
          node('StreamProcessor', 'ledger', 'Ledger', { replicas: 4, state: 'stateful', processing_guarantee: 'exactly_once' });
          node('KafkaTopic', 'balances', 'balances', { cleanup_policy: 'compact' });
          node('KafkaConnect', 'sink', 'Balance Sink', { mode: 'sink', connector: 'jdbc' });
          node('Database', 'baldb', 'Balances DB', { read_ratio: 0, ...HA_DB });
          node('ConsumerGroup', 'fraud', 'Fraud Checker', { replicas: 4, capacity: 800, idempotent: true });
          node('Database', 'riskdb', 'Risk DB', { read_ratio: 0.3, ...HA_DB });
          chain('users', 'lb', 'api', 'db', 'cdc', 'transfers', 'ledger', 'balances', 'sink', 'baldb');
          chain('transfers', 'fraud', 'riskdb');
        }),
      },
      {
        title: 'Audit-ready',
        goal: 'Keep the ledger running through bad events, schema changes and failovers.',
        insights: ['Standby replicas keep a warm copy of the ledger\'s state, so a failover takes seconds instead of minutes of replay.', 'Dead letter topics, a Schema Registry and an encrypted, authenticated cluster complete the picture.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Customers', { users: 30000, requests_per_user: 0.1 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, auth: 'oauth', https: true, rate_limiting: true });
          node('Service', 'api', 'Transfer Service', { autoscaling: true, autoscale_min: 4, autoscale_max: 16 });
          node('Database', 'db', 'Transfers DB', { read_ratio: 0.5, capacity: 4000, shards: 3, ...HA_DB, ...SECURE_DB });
          node('KafkaConnect', 'cdc', 'Outbox CDC', { mode: 'source', connector: 'cdc', exactly_once: true });
          node('KafkaTopic', 'transfers', 'transfers', { partitions: 12, retention_hours: 720, tiered_storage: true, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry', { compatibility: 'full' });
          node('StreamProcessor', 'ledger', 'Ledger', { replicas: 4, state: 'stateful', processing_guarantee: 'exactly_once', standby_replicas: 1, dead_letter_topic: true });
          node('KafkaTopic', 'balances', 'balances', { cleanup_policy: 'compact', ...KAFKA_SECURE });
          node('KafkaConnect', 'sink', 'Balance Sink', { mode: 'sink', connector: 'jdbc', dead_letter_topic: true, exactly_once: true });
          node('Database', 'baldb', 'Balances DB', { read_ratio: 0, ...HA_DB, ...SECURE_DB });
          node('ConsumerGroup', 'fraud', 'Fraud Checker', { replicas: 4, capacity: 800, idempotent: true, dead_letter_topic: true });
          node('Database', 'riskdb', 'Risk DB', { read_ratio: 0.3, ...HA_DB, ...SECURE_DB });
          chain('users', 'gw', 'api', 'db', 'cdc', 'transfers', 'ledger', 'balances', 'sink', 'baldb');
          chain('transfers', 'fraud', 'riskdb');
          link('cdc', 'schemas');
          link('ledger', 'schemas');
        }),
      },
    ],
    tests: kafkaTests({ spike: 3, mega: 4, requires: { normal: ['ordered', 'consistent'], crash: ['no-duplicates'] } }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'log_pipeline',
    track: 'kafka',
    title: 'Log Aggregation Pipeline',
    difficulty: 'Intermediate',
    category: 'Kafka · Observability',
    summary: 'Ship 100,000 log lines a second from every server into search and cheap long-term storage.',
    statement: 'Ten thousand servers produce logs. Engineers search the last few days of logs during incidents, and compliance needs a year of raw logs in cheap storage. Collectors on each server batch log lines and send them on. The pipeline must keep up with incident-time floods of logs, and a log line must be searchable within half a minute.',
    functional: [
      req('Collect log lines from every server', ['collect', 'ship', 'agent', 'log line', 'ingest']),
      req('Search recent logs by text and fields', ['search', 'query', 'elasticsearch', 'index', 'full-text']),
      req('Archive raw logs for a year', ['archive', 'retention', 's3', 'cold storage', 'year']),
    ],
    non_functional: [
      req('100,000 log lines per second at peak', ['100,000', '100k', 'throughput', 'per second']),
      req('Logs are searchable within 30 seconds', ['fresh', 'seconds', 'searchable', 'lag']),
      req('Incident floods of 4× normal do not lose logs', ['burst', 'spike', 'flood', 'buffer', 'durable']),
    ],
    constraints: ['A log line is about 500 bytes', 'Logs compress about 5:1', 'Search keeps 3 days; the archive keeps a year'],
    scale: { users: 10000, peak_rps: 100000, latency_target_ms: 200, freshness_target_ms: 30000, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 10000, requests_per_user: 10 },
    required: [
      { type: 'KafkaTopic', why: 'A buffer that absorbs log floods and feeds several destinations.' },
      { type: 'KafkaConnect', why: 'Ready-made sinks write to the search index and to object storage.' },
      { type: 'SearchIndex', why: 'Full-text search over recent logs.' },
      { type: 'BlobStorage', why: 'Cheap storage for a year of raw logs.' },
    ],
    concepts: ['kafka-producers', 'kafka-partitions', 'kafka-connect-cdc', 'kafka-operations'],
    hints: [
      'What happens to the search index when an incident triples the logs?',
      'Producers that send each line alone waste most of each request. What do linger.ms and compression change?',
      'How much can three brokers write when every message is stored three times?',
      'Each sink task reads its own partitions. What limits how many tasks can work at once?',
    ],
    stages: [
      {
        title: 'Collectors write straight to search',
        goal: 'Find the first thing that breaks.',
        insights: ['Every batch goes straight into the search index, which cannot keep up with indexing at peak.', 'When it slows down, collectors back up and drop logs, exactly during the incidents when logs matter most.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Server Agents', { users: 10000, requests_per_user: 10 });
          node('Service', 'collect', 'Collectors', { replicas: 8, capacity: 20000, base_latency: 5 });
          node('SearchIndex', 'search', 'Log Search', { shards: 2 });
          node('BlobStorage', 'archive', 'Log Archive');
          chain('users', 'collect');
          link('collect', 'search', { ratio: 0.1 });
          link('collect', 'archive', { ratio: 0.01 });
        }),
      },
      {
        title: 'A topic in front, defaults everywhere',
        goal: 'Buffer the logs in Kafka.',
        insights: ['Kafka absorbs the logs and feeds both sinks independently.', 'But producers send tiny uncompressed batches: every line costs a full write on three brokers, so the cluster tops out below the peak.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Server Agents', { users: 10000, requests_per_user: 10 });
          node('Service', 'collect', 'Collectors', { replicas: 8, capacity: 20000, base_latency: 5 });
          node('KafkaTopic', 'logs', 'logs', { partitions: 12, message_kb: 0.5, linger_ms: 0, compression: 'none', retention_hours: 12 });
          node('KafkaConnect', 'searchsink', 'Search Sink', { mode: 'sink', connector: 'search', replicas: 6, capacity: 5000 });
          node('SearchIndex', 'search', 'Log Search', { shards: 4, replicas: 1 });
          node('KafkaConnect', 's3sink', 'Archive Sink', { mode: 'sink', connector: 's3', replicas: 4, capacity: 10000 });
          node('BlobStorage', 'archive', 'Log Archive');
          chain('users', 'collect', 'logs', 'searchsink');
          link('searchsink', 'search', { ratio: 0.1 });
          link('logs', 's3sink');
          link('s3sink', 'archive', { ratio: 0.001 });
        }),
      },
      {
        title: 'Batch, compress and partition',
        goal: 'Make each broker carry far more lines.',
        insights: ['Waiting 20 ms per batch and compressing with zstd shrink the data about 2× per write; the same brokers carry three times the lines.', 'Forty-eight partitions let 24 search-sink tasks and 12 archive tasks work in parallel.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Server Agents', { users: 10000, requests_per_user: 10 });
          node('Service', 'collect', 'Collectors', { autoscaling: true, autoscale_min: 8, autoscale_max: 30, capacity: 20000, base_latency: 5 });
          node('KafkaTopic', 'logs', 'logs', { partitions: 48, message_kb: 0.5, linger_ms: 20, compression: 'zstd', retention_hours: 12 });
          node('KafkaConnect', 'searchsink', 'Search Sink', { mode: 'sink', connector: 'search', replicas: 24, capacity: 5000 });
          node('SearchIndex', 'search', 'Log Search', { shards: 4, replicas: 1 });
          node('KafkaConnect', 's3sink', 'Archive Sink', { mode: 'sink', connector: 's3', replicas: 12, capacity: 10000 });
          node('BlobStorage', 'archive', 'Log Archive', { storage_class: 'infrequent' });
          chain('users', 'collect', 'logs', 'searchsink');
          link('searchsink', 'search', { ratio: 0.1 });
          link('logs', 's3sink');
          link('s3sink', 'archive', { ratio: 0.001 });
        }),
      },
      {
        title: 'Room for floods and failures',
        goal: 'Survive a 4× flood, a lost sink task, a long outage and bad lines.',
        insights: ['Nine brokers carry a 4× incident flood, and 36 search-sink tasks still keep up after one of them dies.', 'Three days of retention covers a long sink outage; dead letter topics catch malformed lines, and agents reach the collectors over TLS through a rate limiter.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Server Agents', { users: 10000, requests_per_user: 10 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 40, replicas: 8 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 10, https: true });
          node('Service', 'collect', 'Collectors', { autoscaling: true, autoscale_min: 8, autoscale_max: 30, capacity: 20000, base_latency: 5, auth: 'api_key' });
          node('KafkaTopic', 'logs', 'logs', { partitions: 48, brokers: 9, message_kb: 0.5, linger_ms: 20, compression: 'zstd', retention_hours: 72, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry', { format: 'json_schema' });
          node('KafkaConnect', 'searchsink', 'Search Sink', { mode: 'sink', connector: 'search', replicas: 36, capacity: 5000, dead_letter_topic: true });
          node('SearchIndex', 'search', 'Log Search', { shards: 4, replicas: 1, encryption_at_rest: true });
          node('KafkaConnect', 's3sink', 'Archive Sink', { mode: 'sink', connector: 's3', replicas: 16, capacity: 10000, dead_letter_topic: true });
          node('BlobStorage', 'archive', 'Log Archive', { storage_class: 'infrequent', encryption_at_rest: true });
          chain('users', 'rl', 'lb', 'collect', 'logs', 'searchsink');
          link('collect', 'schemas');
          link('searchsink', 'search', { ratio: 0.1 });
          link('logs', 's3sink');
          link('s3sink', 'archive', { ratio: 0.001 });
        }),
      },
    ],
    tests: kafkaTests({ spike: 2, mega: 4, db: false }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'notification_fanout',
    track: 'kafka',
    title: 'Notification Fan-out',
    difficulty: 'Intermediate',
    category: 'Kafka · Messaging',
    summary: 'Turn events from every product team into push, email and SMS messages, without one slow channel holding up the rest.',
    statement: 'Product teams publish events such as "order shipped" or "new follower". A notification platform turns them into push notifications, emails and text messages through outside providers. Every user gets a push; some also get an email or an SMS. A slow or failing provider must not delay the other channels, and nobody should get the same SMS twice.',
    functional: [
      req('Accept notification events from any team', ['event', 'publish', 'team', 'producer']),
      req('Send push, email and SMS through providers', ['push', 'email', 'sms', 'provider', 'channel']),
      req('Respect each user\'s channel preferences', ['preference', 'opt', 'settings', 'channel']),
    ],
    non_functional: [
      req('5,000 events per second at peak', ['5,000', '5k', 'throughput', 'per second']),
      req('Notifications go out within 30 seconds', ['30 seconds', 'fresh', 'delay', 'lag']),
      req('A slow provider never delays other channels', ['isolat', 'separate', 'independent', 'bulkhead', 'consumer group']),
      req('Nobody gets the same SMS twice', ['duplicate', 'idempot', 'once', 'dedup']),
    ],
    constraints: ['Every event becomes a push; 40% also become an email and 10% an SMS', 'The SMS provider sometimes slows down to over a second per message', 'Providers allow 10,000 pushes, 3,000 emails and 1,000 SMS per second'],
    scale: { users: 20000, peak_rps: 5000, latency_target_ms: 200, freshness_target_ms: 30000, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 20000, requests_per_user: 0.25 },
    required: [
      { type: 'KafkaTopic', why: 'Teams publish once; every channel reads the same stream.' },
      { type: 'ConsumerGroup', why: 'A separate group per channel keeps a slow provider from holding up the others.' },
      { type: 'ThirdPartyAPI', why: 'Push, email and SMS go out through providers.' },
    ],
    concepts: ['kafka-consumers', 'kafka-rebalancing', 'kafka-delivery', 'resilience-patterns'],
    hints: [
      'If one consumer calls all three providers in turn, what does a slow SMS provider do to push notifications?',
      'Every consumer group gets every message. How could each channel get its own group?',
      'A poll fetches 500 messages. If each takes over a second, what does max.poll.interval.ms do?',
      'After a crash, the last few messages are processed again. How do you avoid a second SMS?',
    ],
    stages: [
      {
        title: 'The API calls every provider',
        goal: 'See why calling providers in the request is fragile.',
        insights: ['Each event waits for three providers in a row, so the slowest provider sets everyone\'s latency.', 'If the SMS provider stops, events that need an SMS fail outright.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Product Teams', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Notify API', { replicas: 6 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000 });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 3000 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 1000 });
          chain('users', 'lb', 'api', 'push');
          link('api', 'mail', { ratio: 0.4 });
          link('api', 'sms', { ratio: 0.1 });
        }),
      },
      {
        title: 'One consumer does everything',
        goal: 'Queue the events in Kafka and send them in the background.',
        insights: ['Teams get an instant answer: their event is safely in Kafka.', 'But one consumer group calls all three providers, so it is slow, and any SMS slowdown holds up every push and email behind it.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Product Teams', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Notify API', { replicas: 6 });
          node('KafkaTopic', 'events', 'notifications', { partitions: 6 });
          node('ConsumerGroup', 'notifier', 'Notifier', { replicas: 6 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000 });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 3000 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 1000 });
          chain('users', 'lb', 'api', 'events', 'notifier', 'push');
          link('notifier', 'mail', { ratio: 0.4 });
          link('notifier', 'sms', { ratio: 0.1 });
        }),
      },
      {
        title: 'A consumer group per channel',
        goal: 'Isolate the channels from each other.',
        insights: ['Each channel reads the whole stream in its own group, keeps what it needs and moves at its own pace.', 'But when the SMS provider slows to over a second per message, a 500-message poll takes longer than max.poll.interval.ms and the SMS group rebalances endlessly.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Product Teams', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Service', 'api', 'Notify API', { replicas: 6 });
          node('KafkaTopic', 'events', 'notifications', { partitions: 12 });
          node('ConsumerGroup', 'pushers', 'Push Sender', { replicas: 12, capacity: 800 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000 });
          node('ConsumerGroup', 'mailers', 'Email Sender', { replicas: 6 });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 3000 });
          node('ConsumerGroup', 'texters', 'SMS Sender', { replicas: 6 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 1000 });
          chain('users', 'lb', 'api', 'events', 'pushers', 'push');
          link('events', 'mailers', { ratio: 0.4 });
          link('events', 'texters', { ratio: 0.1 });
          link('mailers', 'mail');
          link('texters', 'sms');
        }),
      },
      {
        title: 'Small polls, no duplicates, no blockers',
        goal: 'Tame slow providers, bad events and replays.',
        insights: ['Fetching 50 messages per poll keeps each batch well inside max.poll.interval.ms even when SMS is slow, and 24 partitions spread a crashed sender\'s work over the rest.', 'Idempotent senders skip messages they already sent, and dead letter topics park events that keep failing.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Product Teams', { users: 20000, requests_per_user: 0.25 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, auth: 'api_key', https: true, rate_limiting: true });
          node('Service', 'api', 'Notify API', { autoscaling: true, autoscale_min: 6, autoscale_max: 30 });
          node('KafkaTopic', 'events', 'notifications', { partitions: 24, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry');
          node('ConsumerGroup', 'pushers', 'Push Sender', { replicas: 12, capacity: 800, idempotent: true, dead_letter_topic: true });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 10000, circuit_breaker: true });
          node('ConsumerGroup', 'mailers', 'Email Sender', { replicas: 6, idempotent: true, dead_letter_topic: true, max_poll_records: 100 });
          node('ThirdPartyAPI', 'mail', 'Email Provider', { provider: 'email', capacity: 3000, circuit_breaker: true });
          node('ConsumerGroup', 'texters', 'SMS Sender', { replicas: 6, idempotent: true, dead_letter_topic: true, max_poll_records: 50 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 1000, circuit_breaker: true });
          chain('users', 'gw', 'api', 'events', 'pushers', 'push');
          link('api', 'schemas');
          link('events', 'mailers', { ratio: 0.4 });
          link('events', 'texters', { ratio: 0.1 });
          link('mailers', 'mail');
          link('texters', 'sms');
        }),
      },
    ],
    tests: kafkaTests({
      spike: 3, mega: 6, db: false, requires: { crash: ['no-duplicates'] },
      extra: [kafkaProviderSlow('sms-slow', 'SMS provider 10× slower', 'sms', 10, 'The SMS provider takes over a second per message. Push and email must not notice.')],
    }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'catalog_cdc',
    track: 'kafka',
    title: 'Product Catalog: CDC to Search and Cache',
    difficulty: 'Intermediate',
    category: 'Kafka · E-commerce',
    summary: 'Stream every catalog change from the database into search and cache, so prices are right everywhere within seconds.',
    statement: 'Shoppers browse and search a catalog of ten million products. Merchants change prices and stock all day. The database is the source of truth, but search results and cached product pages must show the new price within a few seconds, and a crash must never leave search showing a price the database does not have.',
    functional: [
      req('Browse and read product pages', ['product page', 'browse', 'read', 'detail']),
      req('Search products by text and filters', ['search', 'filter', 'query', 'facet']),
      req('Merchants update price and stock', ['price', 'stock', 'update', 'merchant']),
    ],
    non_functional: [
      req('5,000 requests per second at peak', ['5,000', '5k', 'rps', 'throughput']),
      req('Changes appear in search and cache within 5 seconds', ['fresh', 'seconds', 'stale', 'lag', 'propagat']),
      req('Search and cache never disagree with the database for long', ['consisten', 'cdc', 'outbox', 'dual write', 'source of truth']),
    ],
    constraints: ['95% of requests are reads; 5% are merchant updates', '30% of reads are searches', 'Only the latest version of each product matters downstream'],
    scale: { users: 20000, peak_rps: 5000, latency_target_ms: 200, freshness_target_ms: 5000, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 20000, requests_per_user: 0.25 },
    kafka: { schemas: true },
    required: [
      { type: 'KafkaConnect', why: 'A CDC source connector streams committed row changes; a sink connector updates search.' },
      { type: 'KafkaTopic', why: 'A compacted topic holds the latest version of every product.' },
      { type: 'SearchIndex', why: 'Text search and filters over the catalog.' },
      { type: 'Cache', why: 'Product pages are read far more often than they change.' },
    ],
    concepts: ['kafka-connect-cdc', 'kafka-retention', 'kafka-schemas', 'cache-invalidation'],
    hints: [
      'If the API updates the database and then the search index, what happens when the second step fails?',
      'Change data capture reads the database log. Why does that guarantee search sees exactly what was committed?',
      'Only the latest version of a product matters. Which cleanup policy keeps exactly that, forever?',
      'Cached pages must change when the price does. Who should tell the cache?',
    ],
    stages: [
      {
        title: 'Update the database, then search',
        goal: 'See what goes wrong when the API keeps two stores in step itself.',
        insights: ['Every merchant update waits for both the database and the search index, and the database has no failover.', 'If the index update fails after the database commit, search shows the old price until someone notices.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers & Merchants', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Catalog API', { replicas: 8 });
          node('Cache', 'cache', 'Product Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95 });
          node('SearchIndex', 'search', 'Product Search', { shards: 2, replicas: 1 });
          chain('users', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'search', { ratio: 0.35 });
        }),
      },
      {
        title: 'The API publishes changes itself',
        goal: 'Move index updates off the request path.',
        insights: ['A sink connector keeps search updated in the background.', 'But the API writes the database and publishes to Kafka as two steps: a crash between them leaves search out of step for good.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers & Merchants', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Catalog API', { replicas: 8 });
          node('Cache', 'cache', 'Product Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, ...HA_DB });
          node('KafkaTopic', 'changes', 'product-changes', { retention_hours: 12 });
          node('KafkaConnect', 'sink', 'Search Sink', { mode: 'sink', connector: 'search' });
          node('SearchIndex', 'search', 'Product Search', { shards: 2, replicas: 1 });
          chain('users', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'search', { ratio: 0.3 });
          link('api', 'changes', { ratio: 0.05 });
          chain('changes', 'sink', 'search');
        }),
      },
      {
        title: 'Change data capture and a compacted topic',
        goal: 'Publish exactly what the database committed.',
        insights: ['A CDC connector reads the database log, so every committed change, and only those, reaches Kafka.', 'The topic is compacted: it keeps the latest version of every product forever, so a new consumer can rebuild search from scratch. A cache invalidator evicts changed products.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers & Merchants', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Catalog API', { replicas: 8 });
          node('Cache', 'cache', 'Product Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, ...HA_DB });
          node('KafkaConnect', 'cdc', 'Catalog CDC', { mode: 'source', connector: 'cdc' });
          node('KafkaTopic', 'changes', 'product-changes', { cleanup_policy: 'compact' });
          node('KafkaConnect', 'sink', 'Search Sink', { mode: 'sink', connector: 'search' });
          node('SearchIndex', 'search', 'Product Search', { shards: 2, replicas: 1 });
          node('ConsumerGroup', 'evict', 'Cache Invalidator', { replicas: 3, idempotent: true });
          chain('users', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'search', { ratio: 0.3 });
          chain('db', 'cdc', 'changes', 'sink', 'search');
          link('changes', 'evict');
          link('evict', 'cache');
        }),
      },
      {
        title: 'Safe schema changes',
        goal: 'Let teams change the product schema without breaking the pipeline.',
        insights: ['A Schema Registry checks every new version of the product schema before CDC may use it.', 'Dead letter topics park changes the sink cannot index, and the cluster is encrypted and access-controlled.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Shoppers & Merchants', { users: 20000, requests_per_user: 0.25 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 2 });
          node('Server', 'api', 'Catalog API', { autoscaling: true, autoscale_min: 8, autoscale_max: 40, auth: 'jwt', https: true });
          node('Cache', 'cache', 'Product Cache', { hit_ratio: 0.9, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, ...HA_DB, ...SECURE_DB });
          node('KafkaConnect', 'cdc', 'Catalog CDC', { mode: 'source', connector: 'cdc', exactly_once: true });
          node('KafkaTopic', 'changes', 'product-changes', { cleanup_policy: 'compact', ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry');
          node('KafkaConnect', 'sink', 'Search Sink', { mode: 'sink', connector: 'search', dead_letter_topic: true });
          node('SearchIndex', 'search', 'Product Search', { shards: 2, replicas: 1, encryption_at_rest: true });
          node('ConsumerGroup', 'evict', 'Cache Invalidator', { replicas: 3, idempotent: true, dead_letter_topic: true });
          chain('users', 'rl', 'lb', 'api', 'cache');
          link('api', 'db');
          link('api', 'search', { ratio: 0.3 });
          chain('db', 'cdc', 'changes', 'sink', 'search');
          link('cdc', 'schemas');
          link('changes', 'evict');
          link('evict', 'cache');
        }),
      },
    ],
    tests: kafkaTests({ spike: 3, mega: 6, requires: { normal: ['consistent'] } }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'iot_telemetry',
    track: 'kafka',
    title: 'IoT Telemetry Across Regions',
    difficulty: 'Advanced',
    category: 'Kafka · IoT',
    summary: 'Collect 100,000 sensor readings a second in a regional cluster, alert locally, and mirror everything to a central analytics cluster.',
    statement: 'A million industrial sensors report a reading every ten seconds to a regional gateway. Alerts for readings out of range must fire within a minute in the region. All readings are also copied to a central cluster, where a stream job aggregates them per device and minute for dashboards. One huge customer owns 40% of all devices.',
    functional: [
      req('Receive readings from a million devices', ['device', 'sensor', 'reading', 'telemetry', 'ingest']),
      req('Alert on out-of-range readings in the region', ['alert', 'threshold', 'alarm', 'out of range']),
      req('Copy all readings to a central cluster', ['mirror', 'replicat', 'central', 'cross-region', 'mirrormaker']),
      req('Aggregate readings per device and minute', ['aggregate', 'window', 'per minute', 'rollup', 'downsample']),
    ],
    non_functional: [
      req('100,000 readings per second', ['100,000', '100k', 'throughput', 'per second']),
      req('Alerts and dashboards are at most a minute behind', ['minute', 'fresh', 'lag', 'real-time']),
      req('A crashed worker or broker loses nothing', ['durable', 'replicat', 'failover', 'no data loss']),
    ],
    constraints: ['A reading is about 300 bytes', 'One customer owns 40% of devices', 'The central cluster is in another region, 60 ms away'],
    scale: { users: 1000000, peak_rps: 100000, latency_target_ms: 100, freshness_target_ms: 60000, regular_spike: 2, mega_spike: 3 },
    traffic: { users: 1000000, requests_per_user: 0.1 },
    required: [
      { type: 'KafkaTopic', why: 'Regional and central topics buffer and partition the readings.' },
      { type: 'MirrorMaker', why: 'Copies topics from the regional cluster to the central one.' },
      { type: 'StreamProcessor', why: 'Windowed aggregation per device and minute.' },
      { type: 'ConsumerGroup', why: 'Regional alerting reads readings close to the devices.' },
    ],
    concepts: ['kafka-partitions', 'kafka-streams', 'kafka-operations', 'kafka-replication'],
    hints: [
      'Keyed by customer, where do 40% of all readings go?',
      'Which key spreads load evenly but still keeps one device\'s readings in order?',
      'MirrorMaker is a consumer too. What limits how many of its tasks can work at once?',
      'If every partition has exactly one reader, what happens when a reader dies?',
    ],
    stages: [
      {
        title: 'Gateways write to a database',
        goal: 'Find the first thing that breaks.',
        insights: ['A hundred thousand inserts a second swamps even a sharded database.', 'And every analytics query would compete with the inserts.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Devices', { users: 1000000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 4 });
          node('Service', 'gw', 'Device Gateway', { replicas: 12, capacity: 10000, base_latency: 5 });
          node('Database', 'db', 'Readings DB', { engine: 'nosql', read_ratio: 0, capacity: 10000, shards: 4 });
          chain('users', 'lb', 'gw', 'db');
        }),
      },
      {
        title: 'A regional topic keyed by customer',
        goal: 'Buffer readings in Kafka and aggregate them in a stream job.',
        insights: ['Kafka takes the readings, and the aggregator writes one row per device and minute.', 'But keyed by customer, the big customer\'s 40% lands on one partition, and the one aggregator reading it falls far behind.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Devices', { users: 1000000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 4 });
          node('Service', 'gw', 'Device Gateway', { replicas: 12, capacity: 10000, base_latency: 5 });
          node('KafkaTopic', 'readings', 'readings', { partitions: 24, message_kb: 0.3, partition_key: 'skewed', hot_key_share: 0.4, retention_hours: 12 });
          node('StreamProcessor', 'agg', 'Minute Aggregator', { replicas: 24, capacity: 5000, state: 'stateful', window: 'tumbling' });
          node('Database', 'tsdb', 'Metrics DB', { engine: 'nosql', read_ratio: 0.2, capacity: 10000, shards: 2, ...HA_DB });
          chain('users', 'lb', 'gw', 'readings', 'agg');
          link('agg', 'tsdb', { ratio: 0.02 });
        }),
      },
      {
        title: 'Key by device, alert in the region, mirror to the centre',
        goal: 'Spread the load and split regional from central work.',
        insights: ['Keyed by device id, 96 partitions share the load evenly. Regional alerting reads them close to the devices.', 'MirrorMaker copies every reading to the central cluster, where the aggregator runs.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Devices', { users: 1000000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 4 });
          node('Service', 'gw', 'Device Gateway', { autoscaling: true, autoscale_min: 12, autoscale_max: 40, capacity: 10000, base_latency: 5 });
          node('KafkaTopic', 'readings', 'readings (region)', { partitions: 96, message_kb: 0.3, retention_hours: 12 });
          node('ConsumerGroup', 'alerts', 'Regional Alerting', { replicas: 32, capacity: 5000, idempotent: true });
          node('ThirdPartyAPI', 'pager', 'Pager Service', { provider: 'push', capacity: 1000 });
          node('MirrorMaker', 'mirror', 'Mirror to Central', { replicas: 24 });
          node('KafkaTopic', 'central', 'readings (central)', { partitions: 96, message_kb: 0.3, retention_hours: 12 });
          node('StreamProcessor', 'agg', 'Minute Aggregator', { autoscaling: true, autoscale_min: 24, autoscale_max: 96, capacity: 5000, state: 'stateful', window: 'tumbling' });
          node('Database', 'tsdb', 'Metrics DB', { engine: 'nosql', read_ratio: 0.2, capacity: 10000, shards: 2, ...HA_DB });
          chain('users', 'lb', 'gw', 'readings', 'alerts');
          link('alerts', 'pager', { ratio: 0.001 });
          chain('readings', 'mirror', 'central', 'agg');
          link('agg', 'tsdb', { ratio: 0.02 });
        }),
      },
      {
        title: 'Spare capacity for every reader',
        goal: 'Survive a crashed mirror task, long outages and bad readings.',
        insights: ['Thirty-two mirror tasks share 96 partitions three each, so losing one moves just three partitions; six brokers per cluster carry a 3× surge, and three days of retention cover a long outage.', 'Dead letter topics park malformed readings, standby replicas keep aggregator state warm, and both clusters are locked down.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Devices', { users: 1000000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer', { replicas: 8, https: true });
          node('Service', 'gw', 'Device Gateway', { autoscaling: true, autoscale_min: 12, autoscale_max: 40, capacity: 10000, base_latency: 5, auth: 'api_key' });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 0.5, replicas: 8 });
          node('KafkaTopic', 'readings', 'readings (region)', { partitions: 96, brokers: 6, message_kb: 0.3, retention_hours: 72, ...KAFKA_SECURE });
          node('SchemaRegistry', 'schemas', 'Schema Registry', { format: 'protobuf' });
          node('ConsumerGroup', 'alerts', 'Regional Alerting', { replicas: 32, capacity: 5000, idempotent: true, dead_letter_topic: true });
          node('ThirdPartyAPI', 'pager', 'Pager Service', { provider: 'push', capacity: 1000, circuit_breaker: true });
          node('MirrorMaker', 'mirror', 'Mirror to Central', { replicas: 32 });
          node('KafkaTopic', 'central', 'readings (central)', { partitions: 96, brokers: 6, message_kb: 0.3, retention_hours: 72, ...KAFKA_SECURE });
          node('StreamProcessor', 'agg', 'Minute Aggregator', { autoscaling: true, autoscale_min: 24, autoscale_max: 96, capacity: 5000, state: 'stateful', window: 'tumbling', standby_replicas: 1, dead_letter_topic: true });
          node('Database', 'tsdb', 'Metrics DB', { engine: 'nosql', read_ratio: 0.2, capacity: 10000, shards: 2, ...HA_DB, ...SECURE_DB });
          chain('users', 'rl', 'lb', 'gw', 'readings', 'alerts');
          link('gw', 'schemas');
          link('alerts', 'pager', { ratio: 0.001 });
          chain('readings', 'mirror', 'central', 'agg');
          link('agg', 'tsdb', { ratio: 0.02 });
        }),
      },
    ],
    tests: kafkaTests({ spike: 2, mega: 3 }),
  },
];

// ================================================================ content: problems
const CORE_PROBLEMS = [
  // ---------------------------------------------------------------------------
  {
    id: 'url_shortener',
    title: 'URL Shortener',
    difficulty: 'Beginner',
    category: 'Web',
    summary: 'Turn long links into short codes and redirect millions of clicks a day.',
    statement: 'Build a service that turns a long URL into a short code and redirects anyone who opens the short link. Redirects outnumber new links roughly 100 to 1, and a single viral link can pull a large share of all traffic.',
    functional: [
      req('Create a short code for a long URL', ['shorten', 'short code', 'create', 'generate', 'encode']),
      req('Redirect a short code to its original URL', ['redirect', 'resolve', 'lookup', '301', '302']),
      req('Optional custom aliases', ['custom', 'alias', 'vanity']),
      req('Links can expire', ['expire', 'expiry', 'expiration', 'ttl']),
      req('Count clicks per link', ['click', 'analytics', 'count', 'stats', 'metrics']),
    ],
    non_functional: [
      req('Redirects answer in under 150 ms at p95', ['latency', 'ms', 'p95', 'p99', 'fast']),
      req('99.9% availability for redirects', ['99.9', 'availability', 'available', 'uptime']),
      req('10,000 redirects per second at peak', ['10,000', '10k', 'rps', 'throughput', 'requests per second', 'scale']),
      req('A created link is never lost', ['durable', 'durability', 'never lost', 'persist', 'replicat']),
      req('Codes are unique and hard to guess', ['unique', 'collision', 'random', 'guess']),
    ],
    constraints: ['10,000 redirects/s at peak, about 100 new links/s', 'Codes are 7 characters of base62', 'Links are kept for 5 years unless they expire earlier'],
    scale: { users: 20000, peak_rps: 10000, latency_target_ms: 150, regular_spike: 3, mega_spike: 8 },
    traffic: { users: 20000, requests_per_user: 0.5 },
    required: [
      { type: 'Cache', why: 'Redirect lookups repeat constantly; serving them from memory keeps the database out of the hot path.' },
      { type: 'LoadBalancer', why: 'One server cannot carry 10,000 redirects per second.' },
      { type: 'Database', why: 'The code-to-URL mapping must be durable.' },
    ],
    concepts: ['caching', 'load-balancing', 'replication', 'cdn'],
    hints: [
      'Start by asking what a single server can handle. How many do you need for 10,000 rps?',
      'Almost every request is a lookup of the same few thousand codes. Where could those answers live so the database never sees them?',
      'Permanent redirects can be cached by browsers and CDNs too. How much traffic could you answer before it reaches your servers?',
      'What happens to redirects if the database primary dies? Replicas with failover keep reads and writes going.',
    ],
    stages: [
      {
        title: 'One server, one database',
        goal: 'See where a single box runs out of road.',
        insights: ['One app server handles about 1,000 rps, a tenth of the peak.', 'Every redirect is a database read, so the database is next in line to fall over.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Visitors', { users: 20000, requests_per_user: 0.5 });
          node('Server', 'app', 'App Server');
          node('Database', 'db', 'Links DB', { read_ratio: 0.99 });
          chain('users', 'app', 'db');
        }),
      },
      {
        title: 'Scale out the app tier',
        goal: 'Spread traffic across a pool of identical, stateless servers.',
        insights: ['A load balancer lets you add servers without clients noticing.', 'The servers are fine now, but all 10,000 lookups per second still hit one database.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Visitors', { users: 20000, requests_per_user: 0.5 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'App Servers', { replicas: 12 });
          node('Database', 'db', 'Links DB', { read_ratio: 0.99 });
          chain('users', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Cache the hot links',
        goal: 'Keep repeated lookups out of the database.',
        insights: ['With a 90% hit ratio only one lookup in ten reaches the database.', 'Writes still go to the database, which is why the cache does not help with new links.', 'Two cache nodes with request coalescing keep a cache restart from stampeding the database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Visitors', { users: 20000, requests_per_user: 0.5 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'App Servers', { autoscaling: true, autoscale_min: 6, autoscale_max: 24 });
          node('Cache', 'cache', 'Link Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Links DB', { read_ratio: 0.99, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
      {
        title: 'Redirects at the edge, hardened',
        goal: 'Answer most redirects at the CDN, protect the API, and survive failures.',
        insights: ['Permanent redirects are cacheable, so the CDN answers most of them without touching your servers.', 'Click events go through a queue so analytics never slows a redirect down.', 'Replicas with failover, backups and encryption cover the durability requirement.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Visitors', { users: 20000, requests_per_user: 0.5 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.7 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'App Servers', { autoscaling: true, autoscale_min: 4, autoscale_max: 30, auth: 'api_key' });
          node('Cache', 'cache', 'Link Cache', { hit_ratio: 0.92, ...HA_CACHE, encryption_at_rest: true });
          node('Database', 'db', 'Links DB', { read_ratio: 0.99, ...HA_DB, ...SECURE_DB });
          node('Queue', 'clicks', 'Click Events', { encryption_at_rest: true });
          node('Worker', 'agg', 'Click Aggregator', { concurrency: 32, autoscaling: true, autoscale_min: 2, autoscale_max: 12, idempotent: true });
          node('Database', 'stats', 'Click Stats DB', { engine: 'nosql', read_ratio: 0.1, shards: 2, ...HA_DB, ...SECURE_DB });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'clicks', { ratio: 0.1 });
          chain('clicks', 'agg', 'stats');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 8 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'pastebin',
    title: 'Pastebin',
    difficulty: 'Beginner',
    category: 'Web',
    summary: 'Store text snippets and serve them back fast, with expiry.',
    statement: 'Users paste text (up to 10 MB), get a link, and share it. Most pastes are read a handful of times, but some get linked from popular forums and are read thousands of times in an hour.',
    functional: [
      req('Create a paste and return a link', ['create', 'paste', 'upload', 'link', 'url']),
      req('Read a paste by its link', ['read', 'view', 'fetch', 'get']),
      req('Pastes expire after a chosen time', ['expire', 'expiry', 'ttl', 'delete']),
      req('Optional private pastes', ['private', 'access', 'permission', 'secret']),
    ],
    non_functional: [
      req('Reads under 250 ms at p95', ['latency', 'ms', 'p95', 'fast']),
      req('5,000 reads per second at peak', ['5,000', '5k', 'rps', 'throughput', 'scale']),
      req('Pastes are durable until they expire', ['durable', 'durability', 'replicat', 'persist']),
      req('Highly available reads', ['availability', 'available', '99.9', 'uptime']),
    ],
    constraints: ['Paste bodies average 20 KB and can reach 10 MB', 'Reads outnumber writes 10 to 1', 'Default expiry is 30 days'],
    scale: { users: 5000, peak_rps: 5000, latency_target_ms: 250, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 5000, requests_per_user: 1 },
    required: [
      { type: 'BlobStorage', why: 'Paste bodies are large blobs that do not belong in database rows.' },
      { type: 'Database', why: 'Metadata such as owner, expiry and visibility needs queries.' },
      { type: 'CDN', why: 'Popular pastes are public, immutable content that edges can serve.' },
    ],
    concepts: ['databases', 'cdn', 'caching', 'redundancy'],
    hints: [
      'Large bodies make database rows heavy. Where else could the text live?',
      'A paste never changes after it is created. What does that mean for caching it?',
      'Metadata lookups happen on every read. Can they be cached as well?',
    ],
    stages: [
      {
        title: 'Everything in one database',
        goal: 'Feel the limits of storing big blobs in rows.',
        insights: ['One server and one database cannot carry 5,000 reads per second.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Readers', { users: 5000, requests_per_user: 1 });
          node('Server', 'app', 'App Server');
          node('Database', 'db', 'Pastes DB', { read_ratio: 0.9, base_latency: 25 });
          chain('users', 'app', 'db');
        }),
      },
      {
        title: 'Bodies in object storage',
        goal: 'Keep the database small and let storage hold the text.',
        insights: ['Object storage is cheap and durable but slow per request.', 'Metadata still hits the database on every read.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 5000, requests_per_user: 1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'App Servers', { replicas: 8 });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.9 });
          node('BlobStorage', 'blob', 'Paste Bodies');
          chain('users', 'lb', 'app', 'db');
          link('app', 'blob');
        }),
      },
      {
        title: 'Serve popular pastes from the edge',
        goal: 'Immutable content belongs in a CDN; hot metadata belongs in a cache.',
        insights: ['Because pastes never change, the CDN can cache them until they expire.', 'The cache shields the metadata database from repeat lookups.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 5000, requests_per_user: 1 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.8 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'App Servers', { autoscaling: true, autoscale_min: 3, autoscale_max: 12 });
          node('Cache', 'cache', 'Metadata Cache', { hit_ratio: 0.85 });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.9, read_replicas: 1, failover: true });
          node('BlobStorage', 'blob', 'Paste Bodies');
          chain('users', 'cdn', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'blob');
        }),
      },
      {
        title: 'Expire, protect and recover',
        goal: 'Clean up expired pastes in the background and close the security gaps.',
        insights: ['A cleanup worker deletes expired pastes without slowing reads.', 'Rate limiting stops one client from flooding the create endpoint.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 5000, requests_per_user: 1 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.8 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'App Servers', { autoscaling: true, autoscale_min: 3, autoscale_max: 16, auth: 'jwt' });
          node('Cache', 'cache', 'Metadata Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Metadata DB', { read_ratio: 0.9, ...HA_DB, ...SECURE_DB });
          node('BlobStorage', 'blob', 'Paste Bodies', { encryption_at_rest: true });
          node('Queue', 'expiry', 'Expiry Jobs');
          node('Worker', 'cleanup', 'Cleanup Worker', { idempotent: true, replicas: 2 });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'blob');
          link('app', 'expiry', { ratio: 0.05 });
          chain('expiry', 'cleanup');
          link('cleanup', 'blob');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 6 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'notification_service',
    title: 'Notification Service',
    difficulty: 'Beginner',
    category: 'Messaging',
    summary: 'Fan out email, SMS and push notifications reliably without blocking callers.',
    statement: 'Other teams call your API to notify users by email, SMS or push. The API must answer quickly even when a provider is slow, never send the same message twice, and respect each user\'s channel preferences.',
    functional: [
      req('Accept a notification request through an API', ['api', 'accept', 'request', 'endpoint', 'send']),
      req('Deliver by email, SMS or push', ['email', 'sms', 'push', 'channel', 'provider']),
      req('Respect user preferences and opt-outs', ['preference', 'opt-out', 'opt out', 'unsubscribe', 'settings']),
      req('Retry failed deliveries', ['retry', 'retries', 'backoff', 'dead letter', 'dlq']),
      req('Keep a delivery log', ['log', 'history', 'status', 'audit']),
    ],
    non_functional: [
      req('The API answers within 200 ms', ['latency', 'ms', 'p95', 'fast', '200']),
      req('No duplicate notifications', ['duplicate', 'idempot', 'exactly once', 'dedup']),
      req('2,000 requests per second at peak', ['2,000', '2k', 'rps', 'throughput', 'scale']),
      req('A provider outage does not take the API down', ['isolat', 'circuit', 'fallback', 'decoupl', 'async']),
    ],
    constraints: ['Peak of 2,000 notification requests per second', 'About 50% email, 40% push, 10% SMS', 'Providers enforce their own rate limits'],
    scale: { users: 2000, peak_rps: 2000, latency_target_ms: 200, regular_spike: 3, mega_spike: 5 },
    traffic: { users: 2000, requests_per_user: 1 },
    required: [
      { type: 'Queue', why: 'Sending should happen after the API has answered, not while the caller waits.' },
      { type: 'Worker', why: 'Something has to drain the queue and talk to providers.' },
      { type: 'ThirdPartyAPI', why: 'Email, SMS and push are delivered by external providers.' },
    ],
    concepts: ['queues', 'idempotency', 'resilience-patterns', 'rate-limiting'],
    hints: [
      'Does the caller need to wait for the email to be sent, or just for the request to be accepted?',
      'What happens if a worker crashes after sending but before acknowledging the message?',
      'If the email provider goes down for ten minutes, where do the messages wait?',
    ],
    stages: [
      {
        title: 'Send while the caller waits',
        goal: 'See how a slow provider becomes your latency and your outage.',
        insights: ['Callers wait for the provider on every request.', 'The email provider\'s rate limit caps your whole API.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'Client Services', { users: 2000, requests_per_user: 1 });
          node('Server', 'api', 'Notify API', { replicas: 3 });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1200 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 2000, base_latency: 60 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 300, base_latency: 250 });
          chain('clients', 'api');
          link('api', 'email', { ratio: 0.5 });
          link('api', 'push', { ratio: 0.4 });
          link('api', 'sms', { ratio: 0.1 });
        }),
      },
      {
        title: 'Accept now, send later',
        goal: 'Put a queue between accepting a request and delivering it.',
        insights: ['The API only enqueues, so it answers in milliseconds.', 'Workers absorb provider slowness, but they still run jobs twice after a crash.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'Client Services', { users: 2000, requests_per_user: 1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Notify API', { replicas: 3 });
          node('Database', 'log', 'Delivery Log', { read_ratio: 0.2 });
          node('Queue', 'q', 'Send Queue');
          node('Worker', 'senders', 'Sender Workers', { replicas: 4, concurrency: 16 });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1200 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 2000, base_latency: 60 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 300, base_latency: 250 });
          chain('clients', 'lb', 'api', 'log');
          link('api', 'q');
          chain('q', 'senders');
          link('senders', 'email', { ratio: 0.5 });
          link('senders', 'push', { ratio: 0.4 });
          link('senders', 'sms', { ratio: 0.1 });
        }),
      },
      {
        title: 'Retries without duplicates',
        goal: 'Make every step safe to repeat and contain provider failures.',
        insights: ['Idempotent workers make at-least-once delivery safe.', 'Circuit breakers stop workers from waiting on a dead provider.', 'Preferences are cached so every request does not query the database.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'Client Services', { users: 2000, requests_per_user: 1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'Notify API', { replicas: 3 });
          node('Cache', 'prefs', 'Preferences Cache', { hit_ratio: 0.95, request_coalescing: true });
          node('Database', 'log', 'Delivery Log', { read_ratio: 0.2, read_replicas: 1, failover: true });
          node('Queue', 'q', 'Send Queue');
          node('Worker', 'senders', 'Sender Workers', { replicas: 4, concurrency: 16, idempotent: true });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1200, circuit_breaker: true, timeout_ms: 1000 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 2000, base_latency: 60, circuit_breaker: true, timeout_ms: 1000 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 300, base_latency: 250, circuit_breaker: true, timeout_ms: 1500 });
          chain('clients', 'lb', 'api', 'prefs');
          link('api', 'log');
          link('api', 'q');
          chain('q', 'senders');
          link('senders', 'email', { ratio: 0.5 });
          link('senders', 'push', { ratio: 0.4 });
          link('senders', 'sms', { ratio: 0.1 });
        }),
      },
      {
        title: 'Scale and protect',
        goal: 'Autoscale workers, protect the API and harden the data layer.',
        insights: ['Workers autoscale with the backlog instead of being sized for the worst day.', 'Per-client rate limits stop one noisy team from starving the others.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'Client Services', { users: 2000, requests_per_user: 1 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 5 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'api', 'Notify API', { autoscaling: true, autoscale_min: 3, autoscale_max: 15, auth: 'api_key' });
          node('Cache', 'prefs', 'Preferences Cache', { hit_ratio: 0.95, ...HA_CACHE });
          node('Database', 'log', 'Delivery Log', { engine: 'nosql', read_ratio: 0.2, shards: 6, ...HA_DB, ...SECURE_DB });
          node('Queue', 'q', 'Send Queue', { max_depth: 2000000 });
          node('Worker', 'senders', 'Sender Workers', { concurrency: 16, idempotent: true, autoscaling: true, autoscale_min: 4, autoscale_max: 30 });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1200, circuit_breaker: true, timeout_ms: 1000 });
          node('ThirdPartyAPI', 'push', 'Push Provider', { provider: 'push', capacity: 2000, base_latency: 60, circuit_breaker: true, timeout_ms: 1000 });
          node('ThirdPartyAPI', 'sms', 'SMS Provider', { provider: 'sms', capacity: 300, base_latency: 250, circuit_breaker: true, timeout_ms: 1500 });
          chain('clients', 'rl', 'lb', 'api', 'prefs');
          link('api', 'log');
          link('api', 'q');
          chain('q', 'senders');
          link('senders', 'email', { ratio: 0.5 });
          link('senders', 'push', { ratio: 0.4 });
          link('senders', 'sms', { ratio: 0.1 });
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 5, extra: [
      { id: 'worker-outage', name: 'Workers crash', tier: 'system', scenario: 'failure', prefer: ['Worker'], weight: 1,
        description: 'The sender workers fail. The API should keep accepting requests while the queue holds the backlog.' },
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'ticket_booking',
    title: 'Concert Ticket Sales',
    difficulty: 'Intermediate',
    category: 'Commerce',
    summary: 'Survive the on-sale rush: seat maps, holds, payments and scalper bots.',
    statement: 'When a popular tour goes on sale, tens of thousands of fans (and a lot of scalper bots) hit the site at the same second. Fans browse the seat map, hold seats for a few minutes, pay, and receive a confirmation email. No seat may be sold twice.',
    functional: [
      req('Show a live seat map', ['seat map', 'seats', 'availability', 'browse', 'view']),
      req('Hold seats for a few minutes', ['hold', 'reserve', 'lock', 'reservation']),
      req('Pay and confirm the booking', ['pay', 'payment', 'checkout', 'confirm', 'purchase']),
      req('Email a confirmation', ['email', 'confirmation', 'notify', 'ticket']),
      req('Block bots and scalpers', ['bot', 'scalp', 'captcha', 'rate limit', 'abuse']),
    ],
    non_functional: [
      req('No seat is ever sold twice', ['double', 'oversell', 'consisten', 'transaction', 'lock']),
      req('Pages answer in under 300 ms', ['latency', 'ms', 'p95', 'fast']),
      req('10,000 requests per second at on-sale', ['10,000', '10k', 'rps', 'throughput', 'scale']),
      req('Payments degrade gracefully when the provider struggles', ['circuit', 'timeout', 'retry', 'fallback', 'degrad']),
    ],
    constraints: ['10,000 requests/s at on-sale, about 30% from bots', 'Holds expire after 5 minutes', 'Payment provider allows 1,500 calls/s'],
    scale: { users: 20000, peak_rps: 10000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 20000, requests_per_user: 0.5, abusive_share: 0.3 },
    required: [
      { type: 'RateLimiter', why: 'Bots send far more requests per client than fans do and must be throttled.' },
      { type: 'Cache', why: 'Everyone loads the same seat map at the same time.' },
      { type: 'Queue', why: 'Confirmation emails should not hold up checkout.' },
      { type: 'ThirdPartyAPI', why: 'Payments go through an external provider.' },
    ],
    concepts: ['rate-limiting', 'consistency', 'sharding', 'resilience-patterns'],
    hints: [
      'Bots are a large share of the traffic. Where is the cheapest place to stop them?',
      'Holds are writes that conflict with each other. How can you spread them so one primary is not the bottleneck?',
      'Does the fan need to wait for the confirmation email before seeing "you got the tickets"?',
    ],
    stages: [
      {
        title: 'The naive box office',
        goal: 'Watch a single server and database melt at on-sale.',
        insights: ['Bots and fans compete for the same capacity.'],
        graph: design(({ node, chain }) => {
          node('User', 'fans', 'Fans and Bots', { users: 20000, requests_per_user: 0.5 });
          node('Server', 'app', 'Box Office App');
          node('Database', 'db', 'Inventory DB', { read_ratio: 0.9 });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          chain('fans', 'app', 'db');
          chain('app', 'pay');
        }),
      },
      {
        title: 'Scale out and cache the seat map',
        goal: 'Serve the read-heavy seat map from memory.',
        insights: ['The seat map is identical for everyone, so it caches extremely well.', 'Bot traffic still eats a third of your app capacity.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans and Bots', { users: 20000, requests_per_user: 0.5 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Box Office App', { replicas: 14 });
          node('Cache', 'cache', 'Seat Map Cache', { hit_ratio: 0.85 });
          node('Database', 'db', 'Inventory DB', { read_ratio: 0.9, read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          chain('fans', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'pay', { ratio: 0.02 });
        }),
      },
      {
        title: 'Keep the bots out',
        goal: 'Throttle abusive clients at the edge and take email off the critical path.',
        insights: ['A per-client limit stops bots while fans, who click far less often, sail through.', 'Confirmation emails go through a queue so checkout never waits for the email provider.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans and Bots', { users: 20000, requests_per_user: 0.5 });
          node('RateLimiter', 'rl', 'Bot Shield', { limit_per_client_rps: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Box Office App', { autoscaling: true, autoscale_min: 8, autoscale_max: 30 });
          node('Cache', 'cache', 'Seat Map Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Inventory DB', { read_ratio: 0.9, read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
          node('Queue', 'mailq', 'Confirmation Queue');
          node('Worker', 'mailer', 'Mailer', { idempotent: true, replicas: 2 });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1000, circuit_breaker: true });
          chain('fans', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'pay', { ratio: 0.02 });
          link('app', 'mailq', { ratio: 0.02 });
          chain('mailq', 'mailer', 'email');
        }),
      },
      {
        title: 'Sharded holds and graceful payments',
        goal: 'Spread hold writes across shards and protect checkout from provider trouble.',
        insights: ['Sharding by event and section spreads conflicting writes across primaries.', 'A circuit breaker and short timeout keep a slow payment provider from tying up every server.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans and Bots', { users: 20000, requests_per_user: 0.5 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.3 });
          node('RateLimiter', 'rl', 'Bot Shield', { limit_per_client_rps: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Box Office App', { autoscaling: true, autoscale_min: 8, autoscale_max: 40, auth: 'jwt', timeout_ms: 2000 });
          node('Cache', 'cache', 'Seat Map Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Inventory DB', { read_ratio: 0.9, shards: 4, ...HA_DB, ...SECURE_DB });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500, circuit_breaker: true, timeout_ms: 1500 });
          node('Queue', 'mailq', 'Confirmation Queue');
          node('Worker', 'mailer', 'Mailer', { idempotent: true, autoscaling: true, autoscale_min: 2, autoscale_max: 8 });
          node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1000, circuit_breaker: true });
          chain('fans', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'pay', { ratio: 0.02 });
          link('app', 'mailq', { ratio: 0.02 });
          chain('mailq', 'mailer', 'email');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'news_feed',
    title: 'Social News Feed',
    difficulty: 'Intermediate',
    category: 'Social',
    summary: 'Show each user a fresh, personalised feed in a fraction of a second.',
    statement: 'Users follow other users and open their home feed many times a day. A feed shows the newest posts from everyone they follow. Posting is rare compared with reading, but a post by someone with millions of followers must not take minutes to appear.',
    functional: [
      req('Publish a post with optional media', ['post', 'publish', 'upload', 'media', 'photo']),
      req('Show a home feed of followed accounts', ['feed', 'timeline', 'home', 'follow']),
      req('Follow and unfollow users', ['follow', 'unfollow', 'graph', 'relationship']),
      req('Paginate older posts', ['pagina', 'cursor', 'older', 'scroll']),
    ],
    non_functional: [
      req('Feed loads in under 250 ms at p95', ['latency', 'ms', 'p95', 'fast']),
      req('20,000 feed reads per second', ['20,000', '20k', 'rps', 'throughput', 'scale']),
      req('New posts appear within seconds', ['fresh', 'seconds', 'eventual', 'delay', 'propagat']),
      req('Feeds stay available during failures', ['availability', 'available', '99.9', 'degrad']),
    ],
    constraints: ['100,000 active users, each loading the feed every few seconds', 'Reads outnumber posts 20 to 1', 'Media is served from object storage'],
    scale: { users: 100000, peak_rps: 20000, latency_target_ms: 250, regular_spike: 3, mega_spike: 6 },
    traffic: { users: 100000, requests_per_user: 0.2 },
    required: [
      { type: 'Cache', why: 'Precomputed feeds must be served from memory.' },
      { type: 'Queue', why: 'Fanning a post out to followers is background work.' },
      { type: 'Worker', why: 'Workers write new posts into followers\' feeds.' },
      { type: 'CDN', why: 'Photos and video are static and heavy.' },
    ],
    concepts: ['caching', 'queues', 'cdn', 'consistency'],
    hints: [
      'Building a feed by querying every followed account on each load is expensive. Can you do that work once, when a post is created?',
      'Fan-out on write means one post becomes many cache writes. Who should do that work, and when?',
      'Media is the heaviest part of a feed. Should your servers be serving images at all?',
    ],
    stages: [
      {
        title: 'Build the feed on every request',
        goal: 'See why querying at read time does not scale.',
        insights: ['Each feed load runs an expensive multi-table query.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Readers', { users: 100000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Feed API', { replicas: 24 });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, base_latency: 40 });
          chain('users', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Cache assembled feeds',
        goal: 'Serve feeds from memory instead of rebuilding them.',
        insights: ['Cached feeds make reads cheap.', 'Feeds go stale until the cache entry expires.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 100000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Feed API', { replicas: 24 });
          node('Cache', 'feeds', 'Feed Cache', { hit_ratio: 0.9 });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, base_latency: 40, read_replicas: 1, failover: true });
          chain('users', 'lb', 'app', 'feeds');
          link('app', 'db');
        }),
      },
      {
        title: 'Fan out on write',
        goal: 'Push each new post into followers\' cached feeds in the background.',
        insights: ['Workers update feeds right after a post, so readers see fresh content without rebuilding.', 'The API only records the post and enqueues the fan-out job.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 100000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Feed API', { autoscaling: true, autoscale_min: 10, autoscale_max: 40 });
          node('Cache', 'feeds', 'Feed Cache', { hit_ratio: 0.95, ...HA_CACHE });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, base_latency: 40, ...HA_DB });
          node('Queue', 'fanq', 'Fan-out Queue');
          node('Worker', 'fanout', 'Fan-out Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 10, idempotent: true });
          chain('users', 'lb', 'app', 'feeds');
          link('app', 'db');
          link('app', 'fanq', { ratio: 0.05 });
          chain('fanq', 'fanout', 'feeds');
        }),
      },
      {
        title: 'Media at the edge',
        goal: 'Move images to object storage behind a CDN and harden everything.',
        insights: ['The CDN serves media and static assets, cutting origin traffic in half.', 'Uploads go straight to object storage.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Readers', { users: 100000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.5, capacity: 300000 });
          node('RateLimiter', 'rl', 'Rate Limiter', { replicas: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Feed API', { autoscaling: true, autoscale_min: 6, autoscale_max: 100, auth: 'oauth' });
          node('Cache', 'feeds', 'Feed Cache', { hit_ratio: 0.95, ...HA_CACHE });
          node('Database', 'db', 'Posts DB', { read_ratio: 0.95, base_latency: 40, read_replicas: 4, failover: true, shards: 3, ...SECURE_DB });
          node('BlobStorage', 'media', 'Media Store', { encryption_at_rest: true });
          node('Queue', 'fanq', 'Fan-out Queue');
          node('Worker', 'fanout', 'Fan-out Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 16, idempotent: true });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'feeds');
          link('app', 'db');
          link('app', 'media', { ratio: 0.05 });
          link('app', 'fanq', { ratio: 0.05 });
          chain('fanq', 'fanout', 'feeds');
        }),
      },
    ],
    tests: standardTests({ spike: 3, mega: 6 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'chat_app',
    title: 'Chat Application',
    difficulty: 'Intermediate',
    category: 'Messaging',
    summary: 'Deliver messages in real time to hundreds of thousands of connected users.',
    statement: 'Users keep a chat app open all day. Messages should arrive instantly when the recipient is online, be stored so they can scroll back, and trigger a push notification when the recipient is offline.',
    functional: [
      req('Send and receive messages in real time', ['real time', 'realtime', 'websocket', 'send', 'receive', 'deliver']),
      req('Store message history', ['history', 'store', 'persist', 'scroll']),
      req('Show who is online', ['presence', 'online', 'status', 'last seen']),
      req('Push notifications for offline users', ['push', 'offline', 'notification']),
    ],
    non_functional: [
      req('Messages delivered in under 200 ms', ['latency', 'ms', 'instant', 'p95']),
      req('200,000 concurrent connections', ['200,000', '200k', 'connection', 'concurrent']),
      req('No message is lost', ['durable', 'lost', 'at least once', 'ack', 'persist']),
      req('Messages arrive in order per chat', ['order', 'sequence', 'fifo']),
    ],
    constraints: ['200,000 users connected at peak', 'Each user sends a message every 20 seconds on average', 'One WebSocket server holds about 50,000 connections'],
    scale: { users: 200000, peak_rps: 10000, latency_target_ms: 200, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 200000, requests_per_user: 0.05 },
    required: [
      { type: 'WebSocketServer', why: 'Pushing messages to clients needs long-lived connections.' },
      { type: 'Queue', why: 'Messages must be relayed between connection servers and persisted reliably.' },
      { type: 'Database', why: 'History has to be stored durably.' },
    ],
    concepts: ['queues', 'sharding', 'consistency', 'horizontal-scaling'],
    hints: [
      'How many connections can one server hold? How many servers do you need for 200,000 users?',
      'If the sender and recipient are connected to different servers, how does the message get across?',
      'Writing every message synchronously to the database puts storage on the critical path. What could buffer it?',
    ],
    stages: [
      {
        title: 'One chat server',
        goal: 'Hit the connection ceiling of a single server.',
        insights: ['200,000 connections are four times what one server can hold.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Chat Users', { users: 200000, requests_per_user: 0.05 });
          node('WebSocketServer', 'ws', 'Chat Server');
          node('Database', 'db', 'Messages DB', { engine: 'nosql', read_ratio: 0.5 });
          chain('users', 'ws', 'db');
        }),
      },
      {
        title: 'Spread the connections',
        goal: 'Balance connections across a pool of WebSocket servers.',
        insights: ['Six servers hold the connections, but two users on different servers cannot reach each other yet.', 'Every message is a synchronous database write.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Chat Users', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('WebSocketServer', 'ws', 'Chat Servers', { replicas: 6 });
          node('Database', 'db', 'Messages DB', { engine: 'nosql', read_ratio: 0.5 });
          chain('users', 'lb', 'ws', 'db');
        }),
      },
      {
        title: 'Relay through a message bus',
        goal: 'Let any server deliver to any user and persist messages asynchronously.',
        insights: ['The message bus fans messages out to whichever server holds the recipient.', 'Writers persist messages off the critical path; recent history is served from cache.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('WebSocketServer', 'ws', 'Chat Servers', { replicas: 6 });
          node('Cache', 'recent', 'Recent History', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Messages DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.3, shards: 4, read_replicas: 1, failover: true });
          node('Queue', 'bus', 'Message Bus', { capacity: 100000, partitions: 32 });
          node('Worker', 'writer', 'Message Writers', { concurrency: 32, replicas: 16, idempotent: true });
          chain('users', 'lb', 'ws');
          link('ws', 'recent', { ratio: 0.3 });
          link('recent', 'db', { readShare: 1 });
          link('ws', 'bus');
          chain('bus', 'writer');
          link('writer', 'db', { readShare: 0 });
        }),
      },
      {
        title: 'Offline push and hardening',
        goal: 'Notify offline users and make every layer redundant and secure.',
        insights: ['Offline recipients get a push notification through a separate queue.', 'Autoscaling connection servers keep headroom as the day peaks.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Chat Users', { users: 200000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('WebSocketServer', 'ws', 'Chat Servers', { autoscaling: true, autoscale_min: 6, autoscale_max: 24, auth: 'jwt', https: true });
          node('RateLimiter', 'rl', 'Spam Limiter', { limit_per_client_rps: 2 });
          node('Cache', 'recent', 'Recent History', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'Messages DB', { engine: 'nosql', capacity: 5000, read_ratio: 0.3, shards: 8, ...HA_DB, ...SECURE_DB });
          node('Queue', 'bus', 'Message Bus', { capacity: 100000, partitions: 64 });
          node('Worker', 'writer', 'Message Writers', { concurrency: 32, autoscaling: true, autoscale_min: 8, autoscale_max: 60, idempotent: true });
          node('Queue', 'pushq', 'Push Queue');
          node('Worker', 'pusher', 'Push Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 24, idempotent: true });
          node('ThirdPartyAPI', 'apns', 'Push Provider', { provider: 'push', capacity: 8000, base_latency: 60, circuit_breaker: true });
          chain('users', 'lb', 'rl', 'ws');
          link('ws', 'recent', { ratio: 0.3 });
          link('recent', 'db', { readShare: 1 });
          link('ws', 'bus');
          chain('bus', 'writer');
          link('writer', 'db', { readShare: 0 });
          link('writer', 'pushq', { ratio: 0.2 });
          chain('pushq', 'pusher', 'apns');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 3 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'rate_limited_api',
    title: 'Public API with Rate Limits',
    difficulty: 'Intermediate',
    category: 'Infrastructure',
    summary: 'Keep a public API fast for everyone while a quarter of the traffic is abusive.',
    statement: 'You run a public REST API used by thousands of customers. A few misbehaving clients (scrapers, retry loops, a leaked key) send a quarter of all traffic. Legitimate clients must keep getting fast answers, and limits must hold even with many gateway instances.',
    functional: [
      req('Serve API requests for authenticated clients', ['api', 'endpoint', 'request', 'authenticat', 'key']),
      req('Limit each client to its quota', ['limit', 'quota', 'throttle', 'rate']),
      req('Tell clients when they are limited', ['429', 'retry-after', 'header', 'reject']),
      req('Allow short bursts', ['burst', 'token bucket', 'leaky', 'sliding window']),
    ],
    non_functional: [
      req('p95 under 150 ms for clients within quota', ['latency', 'ms', 'p95', 'fast']),
      req('Limits stay accurate across instances', ['accurate', 'distributed', 'shared', 'redis', 'consisten']),
      req('20,000 requests per second including abuse', ['20,000', '20k', 'rps', 'throughput', 'scale']),
      req('The limiter is never a single point of failure', ['availability', 'fail open', 'redundan', 'single point']),
    ],
    constraints: ['20,000 requests/s, about 25% from abusive clients', 'Normal clients send 1–2 requests per second', 'Gateways run as several instances'],
    scale: { users: 10000, peak_rps: 20000, latency_target_ms: 150, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 10000, requests_per_user: 2, abusive_share: 0.25 },
    required: [
      { type: 'RateLimiter', why: 'Abusive clients must be stopped before they reach the API servers.' },
      { type: 'LoadBalancer', why: 'The API runs on many servers.' },
    ],
    concepts: ['rate-limiting', 'load-balancing', 'consistency', 'security'],
    hints: [
      'Abusive traffic costs the same as real traffic once it reaches your servers. Where should it be rejected?',
      'If each gateway instance counts requests on its own, a client hitting four instances gets four times its quota. How do you share counts?',
      'Which algorithm allows short bursts but caps the sustained rate?',
    ],
    stages: [
      {
        title: 'An unprotected API',
        goal: 'See abusive clients crowd out everyone else.',
        insights: ['A quarter of your capacity goes to abusive traffic.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'API Clients', { users: 10000, requests_per_user: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'API Servers', { replicas: 18 });
          node('Cache', 'cache', 'Response Cache', { hit_ratio: 0.7 });
          node('Database', 'db', 'API DB', { read_ratio: 0.95, read_replicas: 2, failover: true });
          chain('clients', 'lb', 'api', 'cache');
          link('api', 'db');
        }),
      },
      {
        title: 'Limit inside each instance',
        goal: 'Add rate limiting with counters kept in each limiter\'s memory.',
        insights: ['Abuse drops, but with in-memory counters a client spread across instances gets several times its quota.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'API Clients', { users: 10000, requests_per_user: 2 });
          node('RateLimiter', 'rl', 'Rate Limiter', { store: 'in_memory', replicas: 4 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'API Servers', { replicas: 18 });
          node('Cache', 'cache', 'Response Cache', { hit_ratio: 0.7 });
          node('Database', 'db', 'API DB', { read_ratio: 0.95, read_replicas: 2, failover: true });
          chain('clients', 'rl', 'lb', 'api', 'cache');
          link('api', 'db');
        }),
      },
      {
        title: 'Shared counters',
        goal: 'Keep counters in a shared store so limits hold across instances.',
        insights: ['A shared Redis counter makes every instance agree on each client\'s usage.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'API Clients', { users: 10000, requests_per_user: 2 });
          node('RateLimiter', 'rl', 'Rate Limiter', { store: 'redis', replicas: 3 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'API Servers', { autoscaling: true, autoscale_min: 8, autoscale_max: 40 });
          node('Cache', 'cache', 'Response Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'API DB', { read_ratio: 0.95, read_replicas: 2, failover: true });
          chain('clients', 'rl', 'lb', 'api', 'cache');
          link('api', 'db');
        }),
      },
      {
        title: 'A gateway that knows who is calling',
        goal: 'Authenticate at the gateway so limits apply per customer, not per IP.',
        insights: ['Authenticated identity makes per-customer quotas possible and leaked keys revocable.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'clients', 'API Clients', { users: 10000, requests_per_user: 2 });
          node('RateLimiter', 'rl', 'Rate Limiter', { store: 'redis', replicas: 3 });
          node('APIGateway', 'gw', 'API Gateway', { replicas: 2, auth: 'jwt', https: true });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'api', 'API Servers', { autoscaling: true, autoscale_min: 8, autoscale_max: 100, auth: 'jwt' });
          node('Cache', 'cache', 'Response Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('Database', 'db', 'API DB', { read_ratio: 0.95, shards: 4, ...HA_DB, ...SECURE_DB });
          chain('clients', 'rl', 'gw', 'lb', 'api', 'cache');
          link('api', 'db');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 3 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'search_autocomplete',
    title: 'Search Autocomplete',
    difficulty: 'Intermediate',
    category: 'Search',
    summary: 'Suggest completions on every keystroke in well under 100 ms.',
    statement: 'As users type in a search box, show the top suggestions for the prefix typed so far. Every keystroke is a request, suggestions should reflect what people search for today, and the box must feel instant.',
    functional: [
      req('Return top suggestions for a prefix', ['prefix', 'suggest', 'complete', 'top']),
      req('Rank suggestions by popularity', ['rank', 'popular', 'frequency', 'score']),
      req('Update rankings from recent searches', ['update', 'trending', 'recent', 'log']),
      req('Filter offensive suggestions', ['filter', 'block', 'offensive', 'moderat']),
    ],
    non_functional: [
      req('p95 under 100 ms', ['latency', 'ms', 'p95', '100', 'fast']),
      req('20,000 keystrokes per second', ['20,000', '20k', 'rps', 'throughput', 'scale']),
      req('Rankings refresh within an hour', ['fresh', 'hour', 'eventual', 'refresh']),
      req('Highly available', ['availability', 'available', '99.9']),
    ],
    constraints: ['20,000 requests/s at peak', 'A few thousand prefixes cover most traffic', 'Rankings are recomputed from query logs'],
    scale: { users: 50000, peak_rps: 20000, latency_target_ms: 100, regular_spike: 2, mega_spike: 5 },
    traffic: { users: 50000, requests_per_user: 0.4 },
    required: [
      { type: 'SearchIndex', why: 'Prefix lookups with ranking are what search indexes are built for.' },
      { type: 'Cache', why: 'Popular prefixes repeat endlessly.' },
    ],
    concepts: ['search', 'caching', 'cdn', 'queues'],
    hints: [
      'A database LIKE query per keystroke is far too slow. What structure answers prefix queries quickly?',
      'The same short prefixes are typed millions of times. Where could their answers live?',
      'Ranking updates do not need to be instant. How could you compute them away from the request path?',
    ],
    stages: [
      {
        title: 'Query the database on every keystroke',
        goal: 'See a LIKE query collapse under keystroke traffic.',
        insights: ['Without a suitable index every request scans the table.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Searchers', { users: 50000, requests_per_user: 0.4 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Suggest API', { replicas: 20 });
          node('Database', 'db', 'Queries DB', { read_ratio: 0.99, indexed: false });
          chain('users', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'A search index for prefixes',
        goal: 'Serve suggestions from an index built for prefix queries.',
        insights: ['The index answers prefixes quickly, but four copies still cannot take 20,000 requests a second.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Searchers', { users: 50000, requests_per_user: 0.4 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Suggest API', { replicas: 20 });
          node('SearchIndex', 'idx', 'Suggestion Index', { shards: 2, replicas: 1 });
          chain('users', 'lb', 'app', 'idx');
        }),
      },
      {
        title: 'Cache popular prefixes',
        goal: 'Answer hot prefixes from memory in front of the index.',
        insights: ['A read-through cache answers most prefixes; only misses reach the index.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Searchers', { users: 50000, requests_per_user: 0.4 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Suggest API', { autoscaling: true, autoscale_min: 10, autoscale_max: 40 });
          node('Cache', 'cache', 'Prefix Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('SearchIndex', 'idx', 'Suggestion Index', { shards: 2, replicas: 1 });
          chain('users', 'lb', 'app', 'cache', 'idx');
        }),
      },
      {
        title: 'Edge caching and fresh rankings',
        goal: 'Cache top prefixes at the edge and rebuild rankings from logs in the background.',
        insights: ['The shortest prefixes are identical for everyone and cache well at the CDN.', 'Query logs flow through a queue to workers that update the index.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Searchers', { users: 50000, requests_per_user: 0.4 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.5, ttl_seconds: 300, capacity: 400000 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 20, replicas: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true, replicas: 3 });
          node('Server', 'app', 'Suggest API', { autoscaling: true, autoscale_min: 6, autoscale_max: 100, auth: 'api_key' });
          node('Cache', 'cache', 'Prefix Cache', { hit_ratio: 0.85, ...HA_CACHE });
          node('SearchIndex', 'idx', 'Suggestion Index', { shards: 2, replicas: 2 });
          node('Queue', 'logs', 'Query Log');
          node('Worker', 'ranker', 'Ranking Workers', { concurrency: 16, autoscaling: true, autoscale_min: 2, autoscale_max: 12, idempotent: true });
          node('Database', 'db', 'Query Stats DB', { read_ratio: 0.2, ...HA_DB, ...SECURE_DB });
          chain('users', 'cdn', 'rl', 'lb', 'app', 'cache', 'idx');
          link('app', 'logs', { ratio: 0.05 });
          chain('logs', 'ranker', 'db');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 5, extra: [
      { id: 'index-node-loss', name: 'Index node fails', tier: 'system', scenario: 'failure', prefer: ['SearchIndex'], weight: 1,
        description: 'One search index copy dies.' },
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'ride_sharing',
    title: 'Ride Sharing',
    difficulty: 'Advanced',
    category: 'Location',
    summary: 'Track drivers in real time and match riders to the nearest car.',
    statement: 'Drivers send their location every few seconds; riders request a ride and expect to be matched with a nearby driver in seconds. Trips are recorded and paid for at the end. Location traffic dwarfs everything else.',
    functional: [
      req('Ingest driver locations continuously', ['location', 'gps', 'ping', 'update', 'track']),
      req('Find nearby available drivers', ['nearby', 'geo', 'search', 'radius', 'geohash', 'quadtree']),
      req('Match a rider to a driver', ['match', 'dispatch', 'assign']),
      req('Record trips and charge riders', ['trip', 'payment', 'charge', 'fare']),
    ],
    non_functional: [
      req('Matching completes in under 200 ms of server time', ['latency', 'ms', 'p95', 'fast']),
      req('30,000 location updates per second', ['30,000', '30k', 'rps', 'throughput', 'updates']),
      req('Locations are a few seconds fresh', ['fresh', 'seconds', 'real time', 'stale']),
      req('Payments never charge twice', ['idempot', 'duplicate', 'exactly once', 'charge twice']),
    ],
    constraints: ['300,000 drivers and riders online at peak', 'Drivers ping every 4 seconds', 'One WebSocket server holds about 50,000 connections'],
    scale: { users: 300000, peak_rps: 30000, latency_target_ms: 200, regular_spike: 2, mega_spike: 3 },
    traffic: { users: 300000, requests_per_user: 0.1 },
    required: [
      { type: 'WebSocketServer', why: 'Location updates and dispatch notifications ride on persistent connections.' },
      { type: 'SearchIndex', why: 'Nearby-driver queries need a geo index.' },
      { type: 'Queue', why: 'Payments and receipts should not block the trip flow.' },
      { type: 'ThirdPartyAPI', why: 'Payments go through an external provider.' },
    ],
    concepts: ['sharding', 'search', 'horizontal-scaling', 'idempotency'],
    hints: [
      'Writing every GPS ping to a relational database means tens of thousands of writes per second. Do you need durable history for each ping?',
      'Nearby search is a geo query. Which component answers "drivers within 2 km" efficiently?',
      'Charging the rider can happen after the trip ends. What would make that safe to retry?',
    ],
    stages: [
      {
        title: 'Ping a REST server',
        goal: 'See location writes crush a relational database.',
        insights: ['Seventy percent of traffic is writes, so replicas would not help.'],
        graph: design(({ node, chain }) => {
          node('User', 'users', 'Drivers and Riders', { users: 300000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Ride API', { replicas: 32 });
          node('Database', 'db', 'Rides DB', { read_ratio: 0.3 });
          chain('users', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Locations in memory',
        goal: 'Keep the latest location of each driver in an in-memory store fed over WebSockets.',
        insights: ['Only the latest position matters for matching, so it can live in memory.', 'The relational database now only stores trips.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Drivers and Riders', { users: 300000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('WebSocketServer', 'ws', 'Connection Gateway', { replicas: 10 });
          node('Service', 'loc', 'Location Service', { replicas: 30, retry_limit: 1 });
          node('Cache', 'geo', 'Live Locations', { hit_ratio: 0.95, replicas: 3, cluster_mode: true });
          node('Service', 'trips', 'Trip Service', { replicas: 4 });
          node('Database', 'db', 'Trips DB', { read_ratio: 0.6, read_replicas: 1, failover: true });
          chain('users', 'lb', 'ws');
          link('ws', 'loc', { ratio: 0.95 });
          link('ws', 'trips', { ratio: 0.05 });
          chain('loc', 'geo');
          chain('trips', 'db');
        }),
      },
      {
        title: 'Geo index and matching',
        goal: 'Answer nearby-driver queries from a sharded geo index.',
        insights: ['A matching service queries the geo index instead of scanning locations.', 'The index is sharded by region, so each shard only sees local drivers.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Drivers and Riders', { users: 300000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('WebSocketServer', 'ws', 'Connection Gateway', { replicas: 10 });
          node('Service', 'loc', 'Location Service', { autoscaling: true, autoscale_min: 20, autoscale_max: 60, retry_limit: 1 });
          node('Cache', 'geo', 'Live Locations', { hit_ratio: 0.95, replicas: 3, cluster_mode: true, request_coalescing: true });
          node('Service', 'match', 'Matching Service', { replicas: 4 });
          node('SearchIndex', 'geoidx', 'Geo Index', { shards: 4, replicas: 1 });
          node('Database', 'db', 'Trips DB', { read_ratio: 0.6, ...HA_DB });
          chain('users', 'lb', 'ws');
          link('ws', 'loc', { ratio: 0.95 });
          link('ws', 'match', { ratio: 0.05 });
          chain('loc', 'geo');
          chain('match', 'geoidx');
          link('match', 'db');
        }),
      },
      {
        title: 'Payments off the critical path',
        goal: 'Charge riders asynchronously and harden every layer.',
        insights: ['Trip completion enqueues a payment job; idempotency keys make retries safe.', 'A circuit breaker keeps a payment outage from spreading.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'users', 'Drivers and Riders', { users: 300000, requests_per_user: 0.1 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true, replicas: 3 });
          node('RateLimiter', 'rl', 'Rate Limiter', { limit_per_client_rps: 1, replicas: 3 });
          node('WebSocketServer', 'ws', 'Connection Gateway', { autoscaling: true, autoscale_min: 10, autoscale_max: 30, auth: 'jwt', https: true });
          node('Service', 'loc', 'Location Service', { autoscaling: true, autoscale_min: 20, autoscale_max: 80, retry_limit: 1 });
          node('Cache', 'geo', 'Live Locations', { hit_ratio: 0.95, replicas: 3, cluster_mode: true, request_coalescing: true });
          node('Service', 'match', 'Matching Service', { autoscaling: true, autoscale_min: 3, autoscale_max: 12, circuit_breaker: true });
          node('SearchIndex', 'geoidx', 'Geo Index', { shards: 4, replicas: 2 });
          node('Database', 'db', 'Trips DB', { read_ratio: 0.6, shards: 2, ...HA_DB, ...SECURE_DB });
          node('Queue', 'payq', 'Payment Jobs');
          node('Worker', 'payer', 'Payment Workers', { idempotent: true, autoscaling: true, autoscale_min: 2, autoscale_max: 10 });
          node('ThirdPartyAPI', 'psp', 'Payment Provider', { provider: 'payment', capacity: 2000, circuit_breaker: true });
          node('Queue', 'events', 'Dispatch Events');
          node('Worker', 'notifier', 'Dispatch Notifier', { concurrency: 16, idempotent: true, autoscaling: true, autoscale_min: 2, autoscale_max: 12 });
          chain('users', 'lb', 'rl', 'ws');
          link('ws', 'loc', { ratio: 0.95 });
          link('ws', 'match', { ratio: 0.05 });
          link('ws', 'events', { ratio: 0.05 });
          chain('events', 'notifier');
          chain('loc', 'geo');
          chain('match', 'geoidx');
          link('match', 'db');
          link('match', 'payq', { ratio: 0.2 });
          chain('payq', 'payer', 'psp');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 3, cache: true }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'video_streaming',
    title: 'Video Streaming',
    difficulty: 'Advanced',
    category: 'Media',
    summary: 'Upload, transcode and stream video to a large audience.',
    statement: 'Creators upload videos that must be transcoded into several resolutions before anyone can watch. Viewers browse, search and stream; streaming is a constant flow of segment requests and makes up almost all traffic.',
    functional: [
      req('Upload a video', ['upload', 'ingest', 'creator']),
      req('Transcode into several resolutions', ['transcod', 'encode', 'resolution', 'bitrate', 'rendition']),
      req('Stream video segments', ['stream', 'segment', 'hls', 'dash', 'play']),
      req('Search and browse the catalog', ['search', 'browse', 'catalog', 'discover']),
    ],
    non_functional: [
      req('Playback starts in under 300 ms of server time', ['latency', 'ms', 'start', 'buffer', 'p95']),
      req('40,000 segment requests per second', ['40,000', '40k', 'rps', 'throughput', 'scale']),
      req('Uploads are never lost', ['durable', 'lost', 'replicat', 'persist']),
      req('Serve viewers worldwide', ['global', 'worldwide', 'region', 'edge', 'cdn']),
    ],
    constraints: ['200,000 concurrent viewers', '90% of requests are video segments', 'Transcoding one upload takes minutes'],
    scale: { users: 200000, peak_rps: 40000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 200000, requests_per_user: 0.2 },
    required: [
      { type: 'CDN', why: 'Segments are large, static and requested worldwide.' },
      { type: 'BlobStorage', why: 'Videos and renditions live in object storage.' },
      { type: 'Queue', why: 'Transcoding is slow and must run in the background.' },
      { type: 'Worker', why: 'Workers run the transcoding jobs.' },
    ],
    concepts: ['cdn', 'queues', 'search', 'capacity-planning'],
    hints: [
      'Should video bytes ever pass through your application servers?',
      'Transcoding takes minutes. Who does it, and how does the upload request finish quickly?',
      'Browsing and search are a small slice of traffic but need their own read path.',
    ],
    stages: [
      {
        title: 'Stream through the app servers',
        goal: 'Watch storage and servers drown in segment traffic.',
        insights: ['Every segment request goes through your servers and storage.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'viewers', 'Viewers', { users: 200000, requests_per_user: 0.2 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Video API', { replicas: 40 });
          node('BlobStorage', 'blob', 'Video Store');
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95 });
          chain('viewers', 'lb', 'app', 'blob');
          link('app', 'db', { ratio: 0.1 });
        }),
      },
      {
        title: 'Segments from the CDN',
        goal: 'Serve segments from edge caches pulling from object storage.',
        insights: ['The CDN answers almost every segment request; storage only sees misses.', 'The API now handles only browsing traffic.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'viewers', 'Viewers', { users: 200000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'Video CDN', { cache_hit_ratio: 0.95, capacity: 1000000 });
          node('BlobStorage', 'blob', 'Video Store', { capacity: 60000 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Video API', { replicas: 8 });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
          link('viewers', 'cdn', { ratio: 0.9 });
          link('viewers', 'lb', { ratio: 0.1 });
          chain('cdn', 'blob');
          chain('lb', 'app', 'db');
        }),
      },
      {
        title: 'Transcode in the background',
        goal: 'Accept uploads instantly and transcode them with a worker pool.',
        insights: ['Uploads land in object storage and a job is queued; viewers never wait on transcoding.', 'Transcoders scale with the backlog.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'viewers', 'Viewers', { users: 200000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'Video CDN', { cache_hit_ratio: 0.95, capacity: 1000000 });
          node('BlobStorage', 'blob', 'Video Store', { capacity: 60000 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Video API', { autoscaling: true, autoscale_min: 6, autoscale_max: 30 });
          node('Cache', 'meta', 'Metadata Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, ...HA_DB });
          node('Queue', 'tq', 'Transcode Jobs');
          node('Worker', 'tx', 'Transcoders', { capacity: 20, autoscaling: true, autoscale_min: 4, autoscale_max: 40, idempotent: true });
          link('viewers', 'cdn', { ratio: 0.9 });
          link('viewers', 'lb', { ratio: 0.1 });
          chain('cdn', 'blob');
          chain('lb', 'app', 'meta');
          link('app', 'db');
          link('app', 'tq', { ratio: 0.01 });
          chain('tq', 'tx', 'blob');
        }),
      },
      {
        title: 'Search and hardening',
        goal: 'Add catalog search and secure every layer.',
        insights: ['A search index serves catalog queries without loading the database.', 'Encryption, backups and authentication close the security gaps.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'viewers', 'Viewers', { users: 200000, requests_per_user: 0.2 });
          node('CDN', 'cdn', 'Video CDN', { cache_hit_ratio: 0.95, capacity: 1000000 });
          node('BlobStorage', 'blob', 'Video Store', { capacity: 60000, encryption_at_rest: true });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Video API', { autoscaling: true, autoscale_min: 6, autoscale_max: 40, auth: 'oauth' });
          node('Cache', 'meta', 'Metadata Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('SearchIndex', 'search', 'Catalog Search', { shards: 2, replicas: 1 });
          node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, ...HA_DB, ...SECURE_DB });
          node('Queue', 'tq', 'Transcode Jobs');
          node('Worker', 'tx', 'Transcoders', { capacity: 20, autoscaling: true, autoscale_min: 4, autoscale_max: 40, idempotent: true });
          link('viewers', 'cdn', { ratio: 0.9 });
          link('viewers', 'rl', { ratio: 0.1 });
          chain('cdn', 'blob');
          chain('rl', 'lb', 'app', 'meta');
          link('app', 'db');
          link('app', 'search', { ratio: 0.2 });
          link('app', 'tq', { ratio: 0.01 });
          chain('tq', 'tx', 'blob');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4, extra: [
      { id: 'cdn-purge', name: 'CDN edge purge', tier: 'system', scenario: 'failure', typeOverrides: { CDN: { hitMult: 0 } }, weight: 1,
        description: 'A full purge empties every edge cache, so segment requests hit object storage until the edges refill.' },
    ] }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'flash_sale',
    title: 'E-commerce Flash Sale',
    difficulty: 'Advanced',
    category: 'Commerce',
    summary: 'Sell limited stock to a flash crowd without overselling or falling over.',
    statement: 'A retailer runs a one-hour flash sale on a few hundred products. Traffic jumps far beyond normal, bots try to buy everything, orders must never oversell stock, and the payment provider has its own limits.',
    functional: [
      req('Browse the sale catalog', ['browse', 'catalog', 'product', 'listing']),
      req('Add to cart and place an order', ['cart', 'order', 'checkout', 'place']),
      req('Reserve inventory without overselling', ['inventory', 'stock', 'reserve', 'oversell']),
      req('Charge the customer', ['pay', 'payment', 'charge']),
    ],
    non_functional: [
      req('Pages answer in under 300 ms', ['latency', 'ms', 'p95', 'fast']),
      req('30,000 requests per second during the sale', ['30,000', '30k', 'rps', 'throughput', 'scale']),
      req('Never oversell', ['oversell', 'consisten', 'transaction', 'atomic']),
      req('Bots do not crowd out shoppers', ['bot', 'rate limit', 'captcha', 'abuse', 'fair']),
    ],
    constraints: ['30,000 requests/s at the start of the sale, about 15% from bots', 'Orders are about 5% of requests', 'Payment provider allows 2,000 calls/s'],
    scale: { users: 100000, peak_rps: 30000, latency_target_ms: 300, regular_spike: 2, mega_spike: 4 },
    traffic: { users: 100000, requests_per_user: 0.3, abusive_share: 0.15 },
    required: [
      { type: 'CDN', why: 'Product pages and images are the same for everyone.' },
      { type: 'Cache', why: 'Catalog reads must not hit the database.' },
      { type: 'Queue', why: 'Orders should be processed at a pace the database and payment provider can sustain.' },
      { type: 'RateLimiter', why: 'Bots must be throttled.' },
    ],
    concepts: ['cdn', 'queues', 'rate-limiting', 'sharding'],
    hints: [
      'Most requests are people looking at the same products. How much of that can be answered before your servers?',
      'Orders arrive faster than the payment provider allows. What can absorb the difference?',
      'Inventory updates are writes on a few hot rows. How do you avoid one primary becoming the bottleneck?',
    ],
    stages: [
      {
        title: 'Storefront on one database',
        goal: 'See a flash crowd hit the database head-on.',
        insights: ['Every page view becomes database reads.'],
        graph: design(({ node, chain }) => {
          node('User', 'shoppers', 'Shoppers and Bots', { users: 100000, requests_per_user: 0.3 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Store App', { replicas: 30 });
          node('Database', 'db', 'Store DB', { read_ratio: 0.9 });
          chain('shoppers', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Cache the catalog',
        goal: 'Serve pages from the CDN and catalog data from a cache.',
        insights: ['The CDN takes static assets and most product pages off the origin.', 'The cache absorbs catalog reads.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shoppers', 'Shoppers and Bots', { users: 100000, requests_per_user: 0.3 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.6 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Store App', { replicas: 16 });
          node('Cache', 'cache', 'Catalog Cache', { hit_ratio: 0.9 });
          node('Database', 'db', 'Store DB', { read_ratio: 0.9, read_replicas: 1, failover: true });
          chain('shoppers', 'cdn', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
      {
        title: 'Orders through a queue',
        goal: 'Accept orders instantly and process them at a sustainable rate.',
        insights: ['Order workers drain the queue at the pace the database and payment provider allow.', 'Inventory writes are spread across shards.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shoppers', 'Shoppers and Bots', { users: 100000, requests_per_user: 0.3 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.6 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Store App', { autoscaling: true, autoscale_min: 10, autoscale_max: 40 });
          node('Cache', 'cache', 'Catalog Cache', { hit_ratio: 0.9, ...HA_CACHE });
          node('Database', 'db', 'Store DB', { read_ratio: 0.9, ...HA_DB });
          node('Queue', 'orders', 'Order Queue', { max_depth: 1000000 });
          node('Worker', 'ow', 'Order Workers', { concurrency: 16, autoscaling: true, autoscale_min: 3, autoscale_max: 12, idempotent: true });
          node('Database', 'odb', 'Orders DB', { read_ratio: 0.3, shards: 4, read_replicas: 1, failover: true });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 2000, circuit_breaker: true });
          chain('shoppers', 'cdn', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'orders', { ratio: 0.05 });
          chain('orders', 'ow', 'odb');
          link('ow', 'pay');
        }),
      },
      {
        title: 'Bots, failures and flash crowds',
        goal: 'Throttle bots, autoscale aggressively and harden every layer.',
        insights: ['Per-client limits stop bots from reserving all the stock.', 'Everything that holds data is encrypted and backed up.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'shoppers', 'Shoppers and Bots', { users: 100000, requests_per_user: 0.3 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.6, capacity: 500000 });
          node('RateLimiter', 'rl', 'Bot Shield', { limit_per_client_rps: 2, replicas: 2 });
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Store App', { autoscaling: true, autoscale_min: 10, autoscale_max: 90, auth: 'jwt' });
          node('Cache', 'cache', 'Catalog Cache', { hit_ratio: 0.9, ...HA_CACHE, replicas: 3 });
          node('Database', 'db', 'Store DB', { read_ratio: 0.97, shards: 2, read_replicas: 3, failover: true, ...SECURE_DB });
          node('Queue', 'orders', 'Order Queue', { max_depth: 1000000 });
          node('Worker', 'ow', 'Order Workers', { concurrency: 16, autoscaling: true, autoscale_min: 3, autoscale_max: 16, idempotent: true });
          node('Database', 'odb', 'Orders DB', { read_ratio: 0.3, shards: 4, ...HA_DB, ...SECURE_DB });
          node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 3000, circuit_breaker: true });
          chain('shoppers', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
          link('app', 'orders', { ratio: 0.05 });
          chain('orders', 'ow', 'odb');
          link('ow', 'pay');
        }),
      },
    ],
    tests: standardTests({ spike: 2, mega: 4 }),
  },

  // ---------------------------------------------------------------------------
  {
    id: 'live_scores',
    title: 'Live Sports Scores',
    difficulty: 'Advanced',
    category: 'Media',
    summary: 'Serve a scoreboard to half a million fans when the final whistle blows.',
    statement: 'Fans keep a live score page open during big matches and the app refreshes it every few seconds. Traffic is steady during play and explodes in the last minutes of a close game. Scores must be at most a few seconds stale.',
    functional: [
      req('Show live scores for ongoing matches', ['score', 'live', 'scoreboard', 'match']),
      req('Refresh scores every few seconds', ['refresh', 'poll', 'update', 'push']),
      req('Ingest score updates from a data provider', ['ingest', 'provider', 'feed', 'update']),
      req('Show match details and commentary', ['detail', 'commentary', 'event', 'timeline']),
    ],
    non_functional: [
      req('Scores at most 5 seconds stale', ['stale', 'fresh', 'seconds', 'ttl']),
      req('25,000 requests per second, 10× at the finish', ['25,000', '25k', 'rps', 'spike', '10x', '10×']),
      req('Pages answer in under 200 ms', ['latency', 'ms', 'p95', 'fast']),
      req('Stay up during the most important minutes', ['availability', 'available', '99.9', 'spike']),
    ],
    constraints: ['500,000 fans polling every 20 seconds on average', 'Traffic can jump 10× in a minute', 'A score changes a few times per match'],
    scale: { users: 500000, peak_rps: 25000, latency_target_ms: 200, regular_spike: 4, mega_spike: 10 },
    traffic: { users: 500000, requests_per_user: 0.05 },
    required: [
      { type: 'CDN', why: 'Everyone polls the same scoreboard; the edge can answer with a short TTL.' },
      { type: 'Cache', why: 'The origin must not query the database on every poll.' },
    ],
    concepts: ['cdn', 'caching', 'capacity-planning', 'cache-invalidation'],
    hints: [
      'Every fan asks the same question every few seconds. How many of those requests need to reach your servers?',
      'A 5-second TTL at the edge is fresh enough. What does that do to origin traffic?',
      'A 10× spike in a minute is faster than most autoscalers react. What capacity has to already be there?',
    ],
    stages: [
      {
        title: 'Every poll hits the database',
        goal: 'See polling traffic overwhelm the data layer.',
        insights: ['25,000 identical queries per second land on one database.'],
        graph: design(({ node, chain }) => {
          node('User', 'fans', 'Fans', { users: 500000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Scores API', { replicas: 26 });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.99 });
          chain('fans', 'lb', 'app', 'db');
        }),
      },
      {
        title: 'Cache the scoreboard',
        goal: 'Serve the scoreboard from memory.',
        insights: ['A 30-second TTL with invalidation on updates gives a 95% hit ratio.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans', { users: 500000, requests_per_user: 0.05 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Scores API', { replicas: 26 });
          node('Cache', 'cache', 'Scoreboard Cache', { hit_ratio: 0.95, ttl_seconds: 30 });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.99, read_replicas: 1, failover: true });
          chain('fans', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
      {
        title: 'Edge cache with a short TTL',
        goal: 'Let the CDN answer polls for a few seconds at a time.',
        insights: ['With a 5-second edge TTL, the origin sees a small fraction of the polls.', 'The final-minutes spike mostly lands on the CDN.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans', { users: 500000, requests_per_user: 0.05 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.9, ttl_seconds: 5, capacity: 500000 });
          node('LoadBalancer', 'lb', 'Load Balancer');
          node('Server', 'app', 'Scores API', { autoscaling: true, autoscale_min: 4, autoscale_max: 40 });
          node('Cache', 'cache', 'Scoreboard Cache', { hit_ratio: 0.95, ttl_seconds: 30, ...HA_CACHE });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.99, ...HA_DB });
          chain('fans', 'cdn', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
      {
        title: 'Ready for the final whistle',
        goal: 'Keep headroom for a 10× spike and secure the stack.',
        insights: ['A higher minimum instance count means the spike does not wait for the autoscaler.', 'Rate limiting protects the origin from misbehaving clients that bypass the CDN.'],
        graph: design(({ node, chain, link }) => {
          node('User', 'fans', 'Fans', { users: 500000, requests_per_user: 0.05 });
          node('CDN', 'cdn', 'CDN', { cache_hit_ratio: 0.9, ttl_seconds: 5, capacity: 500000 });
          node('RateLimiter', 'rl', 'Rate Limiter');
          node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
          node('Server', 'app', 'Scores API', { autoscaling: true, autoscale_min: 6, autoscale_max: 50, auth: 'api_key' });
          node('Cache', 'cache', 'Scoreboard Cache', { hit_ratio: 0.95, ttl_seconds: 30, ...HA_CACHE });
          node('Database', 'db', 'Scores DB', { read_ratio: 0.99, ...HA_DB, ...SECURE_DB });
          chain('fans', 'cdn', 'rl', 'lb', 'app', 'cache');
          link('app', 'db');
        }),
      },
    ],
    tests: standardTests({ spike: 4, mega: 10 }),
  },
];

const LEVELS = ['Beginner', 'Intermediate', 'Advanced'];
export const PROBLEMS = [...CORE_PROBLEMS, ...MORE_PROBLEMS, ...KAFKA_PROBLEMS]
  .map((p) => ({ track: 'design', ...p }))
  .sort((a, b) => LEVELS.indexOf(a.difficulty) - LEVELS.indexOf(b.difficulty));

export function getProblem(id) {
  return PROBLEMS.find((p) => p.id === id) || null;
}

// ================================================================ content: kafka roadmap
// The Kafka track: three phases after the system design phases, four lessons each.
// Same shape as roadmap.js; authors write the right answer first, the quiz view shuffles.

const q = (question, options, answer, explain) => ({ q: question, options, answer, explain });

export const KAFKA_PHASES = [
  { id: 'phase-6', number: 6, track: 'kafka', title: 'Kafka foundations', summary: 'The log, partitions and keys, producers, and consumer groups.' },
  { id: 'phase-7', number: 7, track: 'kafka', title: 'Kafka reliability', summary: 'Replication, delivery guarantees, retention, rebalances and bad messages.' },
  { id: 'phase-8', number: 8, track: 'kafka', title: 'Kafka ecosystem and operations', summary: 'Schemas, Connect and CDC, stream processing, and running Kafka in production.' },
];

export const KAFKA_TOPICS = [
  // ---- Phase 6 ---------------------------------------------------------------
  {
    id: 'kafka-basics', phase: 'phase-6', title: 'What Kafka is: a durable log', minutes: 8, tags: ['KafkaTopic', 'ConsumerGroup'],
    summary: 'Topics, brokers, producers and consumers, and why a log you can re-read is different from a queue.',
    body: [
      { p: 'Apache Kafka stores streams of events. An event is a small record of something that happened: "order 42 was placed", "user 7 clicked a story". Events are appended to a topic, which is a named log. Nothing is changed or removed when someone reads it; events stay for the retention time (seven days by default) whether or not anyone has read them.' },
      { p: 'Producers write events to topics. Consumers read them at their own pace, and each consumer remembers how far it has read with a number called the offset. Because events stay in the log, many different consumers can read the same topic independently, a new consumer can start from the beginning, and a consumer that was down can catch up where it stopped.' },
      { p: 'A Kafka cluster is a group of servers called brokers. Each topic is split into partitions spread over the brokers, and each partition is copied to several brokers so a broker can fail without losing data. Modern clusters keep their own metadata with KRaft controllers; older ones used ZooKeeper, which Kafka 4.0 removed.' },
      { ul: ['A queue hands each message to one worker and deletes it; Kafka keeps the log and lets every consumer group read all of it.', 'Producers never wait for consumers: Kafka decouples them in time and in speed.', 'Kafka is a good fit for event streams, activity tracking, logs, change data capture and feeding many systems from one source.', 'It is not a database for random lookups; read the log into something built for queries.'] },
    ],
    key_points: ['A topic is an append-only log that keeps events for the retention time.', 'Consumers track their own position (offset), so many can read the same data independently.', 'Brokers store partitions and copy them to each other for durability.'],
    quiz: [
      q('What happens to an event in a Kafka topic after a consumer reads it?', ['It stays in the log until the retention time passes', 'It is deleted immediately', 'It moves to a "read" topic', 'It is locked so no one else can read it'], 0, 'Reading does not remove anything. Events stay for the retention time, which is what lets other consumers read them too, or the same consumer read them again.'),
      q('Three teams need every order event. How do they get them from one topic?', ['Each team reads the topic with its own consumer group', 'The producer writes three copies to three topics', 'They share one consumer group', 'They take turns reading the topic'], 0, 'Every consumer group gets every event. Inside a group the work is shared; across groups each gets the full stream.'),
      q('What is an offset?', ['The position of an event in a partition, used by consumers to remember how far they have read', 'The time zone of the broker', 'The size of a message in bytes', 'The number of copies of a partition'], 0, 'Each event in a partition has an increasing offset. A consumer commits the offset it has processed up to, and restarts from there.'),
    ],
    try: { kind: 'problem', id: 'clickstream_analytics', label: 'Build a live clickstream pipeline' },
  },
  {
    id: 'kafka-partitions', phase: 'phase-6', title: 'Partitions, keys and ordering', minutes: 8, tags: ['KafkaTopic'],
    summary: 'How a topic is split, how a key picks a partition, why order only holds inside one partition, and what a hot key does.',
    body: [
      { p: 'A topic is split into partitions, and each partition is an ordered log of its own living on one leader broker. Partitions are Kafka\'s unit of parallelism: more partitions mean more brokers sharing the writes and more consumers reading at the same time.' },
      { p: 'A producer picks the partition from the message key: the same key always lands on the same partition. That is how Kafka keeps order for one entity: all events with key "order-42" go to one partition and are read in the order they were written. Kafka never promises order across partitions. Messages without a key are spread evenly, with no ordering at all.' },
      { p: 'Choose keys with many, evenly used values, such as user, order or device ids. A key like country or tenant, where one value is huge, sends a big share of all traffic to one partition: a hot partition. That partition, and the single consumer reading it, then limit the whole topic, and adding partitions or consumers does not help.' },
      { ul: ['Plan the partition count for your peak consumer count and throughput; 6–12 is a common start, busy topics use dozens or hundreds.', 'You can add partitions later, but never remove them, and adding them moves keys to new partitions, which breaks per-key ordering for a moment.', 'Order matters per entity, so key by that entity.', 'Split a hot key by adding a suffix (key#1, key#2) and recombine downstream if order across the pieces does not matter.'] },
    ],
    key_points: ['Order holds only within one partition.', 'The key decides the partition; same key, same partition.', 'A skewed key creates a hot partition that caps the topic.'],
    quiz: [
      q('"Order shipped" is sometimes processed before "order paid". What is the most likely cause?', ['The events have no key (or different keys), so they land on different partitions', 'The topic has too much retention', 'acks is set to all', 'The consumer group is too large'], 0, 'Kafka orders events within a partition only. Keying every event by the order id puts all of one order\'s events in one partition.'),
      q('A topic is keyed by tenant, and one tenant sends 40% of all messages. What happens?', ['One partition takes 40% of the load, and its single consumer becomes the bottleneck', 'Kafka spreads that tenant\'s messages over all partitions', 'The tenant\'s messages are rejected', 'Nothing, keys do not affect load'], 0, 'One key always maps to one partition. Re-key by something more even, or split the hot key.'),
      q('Can you reduce a topic from 24 partitions to 12?', ['No: Kafka can add partitions but never remove them', 'Yes, with one configuration change', 'Yes, but only when consumers are stopped', 'Only with ZooKeeper'], 0, 'Partitions can only grow. To have fewer, create a new topic and move the data over.'),
    ],
    try: { kind: 'incident', id: 'kafka-hot-partition', label: 'Cool down a hot partition' },
  },
  {
    id: 'kafka-producers', phase: 'phase-6', title: 'Producers: batching, compression and acks', minutes: 8, tags: ['KafkaTopic'],
    summary: 'What happens between send() and "written": batches, linger.ms, compression, acks, retries and the idempotent producer.',
    body: [
      { p: 'A producer does not send each message on its own. It collects messages for the same partition into batches and sends a batch per request. linger.ms is how long it waits to fill a batch; batch.size caps how big a batch gets. Waiting 5–20 ms costs a little latency and makes brokers far more efficient, because the cost of a request is shared by many messages.' },
      { p: 'Batches can be compressed (lz4, zstd, snappy or gzip). Compressed batches travel and sit on disk in their compressed form, so the same network and disks carry several times more messages. Logs and JSON compress especially well.' },
      { p: 'acks decides when a write counts as done. acks=0 does not wait at all; acks=1 waits for the partition leader; acks=all waits until every in-sync copy has the batch. Only acks=all survives a leader crash without losing confirmed writes. When an acknowledgement is lost, the producer retries; the idempotent producer (on by default since Kafka 3.0) numbers each batch so the broker can drop a duplicate and keep the order.' },
      { ul: ['linger.ms 5–20 and compression are the cheapest throughput wins you will ever get.', 'acks=all plus min.insync.replicas 2 is the standard durable setup.', 'delivery.timeout.ms bounds how long a send may keep retrying before it fails.', 'Keep the idempotent producer on; it removes duplicates caused by retries and keeps retried batches in order.'] },
    ],
    key_points: ['Batching (linger.ms) and compression multiply throughput.', 'acks=all is the only setting that survives a leader crash without loss.', 'The idempotent producer makes retries safe.'],
    quiz: [
      q('Producers send each log line in its own request and brokers are overloaded. What helps most, without new hardware?', ['Set linger.ms to a few milliseconds and enable compression', 'Lower the retention time', 'Add consumer instances', 'Set acks=0'], 0, 'Batching shares the per-request cost over many messages, and compression shrinks every batch.'),
      q('With acks=1, what can happen when a partition leader crashes?', ['Writes it confirmed but had not copied to followers are lost', 'Nothing, acks=1 is as safe as acks=all', 'All partitions go offline', 'Consumers receive every message twice'], 0, 'acks=1 only waits for the leader. A new leader is elected from the followers, which may not have the latest writes.'),
      q('What does the idempotent producer prevent?', ['Duplicate (and reordered) messages caused by producer retries', 'Consumers processing a message twice', 'Brokers running out of disk', 'Schema changes'], 0, 'It numbers batches per partition so the broker discards a retried batch it already has. Consumer-side duplicates need idempotent processing or transactions.'),
    ],
    try: { kind: 'incident', id: 'kafka-producer-throughput', label: 'Speed up an overloaded log pipeline' },
  },
  {
    id: 'kafka-consumers', phase: 'phase-6', title: 'Consumer groups, offsets and lag', minutes: 8, tags: ['ConsumerGroup', 'KafkaTopic'],
    summary: 'How consumers share partitions, why more consumers than partitions sit idle, and what lag tells you.',
    body: [
      { p: 'Consumers that share the work of reading a topic form a consumer group. Kafka assigns each partition to exactly one consumer in the group, so the work splits by partition. A group can therefore have at most as many busy consumers as the topic has partitions; any extra consumers sit idle as hot spares.' },
      { p: 'The split is by partition, not by message, so it is only as even as the partition count allows. Twelve partitions over five consumers means some consumers own three partitions and some two; the busiest set the pace. Partition counts with many divisors (12, 24, 48) and a few more partitions than consumers spread the work better, especially after a consumer dies.' },
      { p: 'Lag is how far a group is behind the end of the log: the newest offset minus the committed offset, per partition. Growing lag means consumers process slower than producers write. A little lag after a spike is normal; Kafka absorbs the burst and consumers catch up. Lag that keeps growing means you need more consumers (up to the partition count), more partitions, or faster processing.' },
      { ul: ['Alert on lag in time ("data is 2 minutes old"), not only in messages.', 'Autoscale consumers on lag, capped at the partition count.', 'Each consumer group has its own offsets, so groups never slow each other down.', 'A consumer that processes one message at a time with a 100 ms call can do about 10 a second; parallelise inside the consumer or add partitions.'] },
    ],
    key_points: ['One partition is read by one consumer per group.', 'Busy consumers per group ≤ partitions.', 'Lag that keeps growing means the group cannot keep up.'],
    quiz: [
      q('A topic has 6 partitions. The team scales its consumer group from 6 to 12 consumers. What happens?', ['Six consumers sit idle; throughput does not improve', 'Throughput doubles', 'Each partition is read by two consumers', 'Kafka creates six more partitions'], 0, 'Within a group each partition goes to one consumer. More parallelism needs more partitions.'),
      q('Consumer lag grows steadily all day. What does it mean?', ['Consumers process messages slower than they are produced', 'Producers are failing', 'The topic is losing messages', 'Retention is too long'], 0, 'Lag is the gap between the end of the log and what the group has processed. A steady rise means the group\'s capacity is below the incoming rate.'),
      q('Why do two consumer groups reading the same topic not slow each other down?', ['Each group keeps its own offsets and reads independently', 'Kafka copies the topic for each group', 'Groups take turns', 'Only one group may read at a time'], 0, 'Offsets are tracked per group, and reading does not change the log, so groups are fully independent.'),
    ],
    try: { kind: 'incident', id: 'kafka-idle-consumers', label: 'Fix a group that will not scale' },
  },

  // ---- Phase 7 ---------------------------------------------------------------
  {
    id: 'kafka-replication', phase: 'phase-7', title: 'Replication, in-sync replicas and durability', minutes: 9, tags: ['KafkaTopic'],
    summary: 'Leaders and followers, the replication factor, min.insync.replicas, and how to lose no data when a broker dies.',
    body: [
      { p: 'Every partition has a leader replica that handles writes and reads, and followers on other brokers that copy it. The replication factor is the number of copies; 3 is the usual choice. The in-sync replicas (ISR) are the copies that are fully caught up. If the leader dies, Kafka elects a new leader from the ISR, so no confirmed write is lost.' },
      { p: 'With acks=all, a write is confirmed once every in-sync replica has it. min.insync.replicas sets how small the ISR may get before writes are refused. With 3 copies and min.insync.replicas 2, one broker can be down and writes continue on two copies; if two are down, writes are refused instead of being stored on a single copy. That refusal is the point: availability is traded for durability.' },
      { p: 'Common mistakes: replication factor 1 (a broker failure loses data), acks=1 (a leader crash loses recent writes), min.insync.replicas 1 (acks=all quietly becomes acks=1 when followers fall behind), and min.insync.replicas equal to the replication factor (every broker restart stops writes). Spread replicas across availability zones so a zone outage does not take every copy.' },
      { ul: ['Standard durable setup: replication factor 3, min.insync.replicas 2, acks=all.', 'Unclean leader election (off by default) lets an out-of-date replica become leader: availability at the cost of losing data. Keep it off.', 'Rack awareness (broker.rack) places copies in different zones.', 'Watch under-replicated partitions: it is the first sign of a struggling broker.'] },
    ],
    key_points: ['RF 3 + min.insync.replicas 2 + acks=all survives one broker loss with no data loss.', 'min.insync.replicas equal to RF turns every restart into a write outage.', 'Spread copies across zones.'],
    quiz: [
      q('Replication factor 3, min.insync.replicas 2, acks=all. One broker dies. What happens to writes?', ['They continue on the two remaining in-sync copies', 'They all fail until the broker returns', 'They continue but may be lost', 'Kafka switches to acks=1'], 0, 'Two copies are still in sync, which meets min.insync.replicas, so writes are accepted and stored on two brokers.'),
      q('A team sets min.insync.replicas to 3 with replication factor 3. What is the risk?', ['Any single broker restart makes acks=all writes fail', 'Data loss on broker failure', 'Duplicate messages', 'Consumers cannot read'], 0, 'With all three copies required, the cluster cannot accept writes while any one broker is down, including during rolling restarts.'),
      q('Why is replication factor 1 dangerous even with acks=all?', ['There is only one copy, so a broker failure makes its partitions unavailable and can lose their data', 'acks=all does not work with one copy', 'Consumers cannot join', 'Compression is disabled'], 0, 'With one copy there is nothing to fail over to. acks=all cannot add copies that do not exist.'),
    ],
    try: { kind: 'incident', id: 'kafka-lost-orders-broker', label: 'Stop losing orders when a broker dies' },
  },
  {
    id: 'kafka-delivery', phase: 'phase-7', title: 'At-most-once, at-least-once and exactly-once', minutes: 9, tags: ['ConsumerGroup', 'StreamProcessor', 'KafkaTopic'],
    summary: 'How the order of "process" and "commit" sets the guarantee, how to make replays harmless, and what Kafka transactions really give you.',
    body: [
      { p: 'A consumer reads a batch, processes it and commits its offset. If it commits before processing and then crashes, the messages it was working on are never processed: at-most-once, which loses data. If it commits after processing and crashes, the batch since the last commit is processed again: at-least-once, which duplicates. At-least-once is the normal choice, because duplicates can be made harmless and lost data cannot be recovered.' },
      { p: 'Make processing idempotent: handling the same message twice has the same effect as once. Upsert by event id instead of insert, keep a table of processed message ids, or use the order id as an idempotency key with your payment provider. Then replays after crashes and rebalances do no harm.' },
      { p: 'Kafka transactions give exactly-once processing for read-process-write inside Kafka: consuming from one topic, updating state and producing to another topic commit together, and consumers with isolation.level=read_committed never see half-done work. Kafka Streams enables this with processing.guarantee=exactly_once_v2. It does not cover side effects outside Kafka, such as charging a card or sending an email; those still need idempotency.' },
      { ul: ['Producer duplicates: the idempotent producer removes them.', 'Consumer duplicates: idempotent processing, or transactions when the output is Kafka.', 'Exactly-once costs some throughput (often 10–20%).', 'Never auto-commit before your processing finishes if losing messages matters.'] },
    ],
    key_points: ['Commit after processing (at-least-once) and make processing idempotent.', 'At-most-once silently loses messages on every crash.', 'Transactions give exactly-once only within Kafka.'],
    quiz: [
      q('A consumer commits offsets right after polling, before processing. It crashes mid-batch. What happens?', ['The unprocessed messages in that batch are skipped forever', 'They are processed twice', 'Kafka redelivers them to another group', 'The broker rolls the offsets back'], 0, 'The committed offset says those messages are done, so after the restart the consumer continues after them: at-most-once.'),
      q('A payments consumer is at-least-once. How do you avoid double charges after a crash?', ['Make the charge idempotent, for example with the order id as the idempotency key', 'Switch to at-most-once', 'Add more partitions', 'Increase retention'], 0, 'Replays are expected with at-least-once. An idempotency key makes the second attempt a no-op.'),
      q('What do Kafka transactions guarantee?', ['Reading, state updates and writing to Kafka topics commit atomically, so read_committed consumers see each result once', 'Emails and card charges happen exactly once', 'Messages are never duplicated anywhere in the company', 'Consumers never lag'], 0, 'Exactly-once covers the Kafka read-process-write loop. External side effects still need idempotency.'),
    ],
    try: { kind: 'incident', id: 'kafka-double-charges', label: 'Stop double charges after a crash' },
  },
  {
    id: 'kafka-retention', phase: 'phase-7', title: 'Retention, compaction and replay', minutes: 8, tags: ['KafkaTopic'],
    summary: 'How long Kafka keeps data, what compaction keeps instead, tiered storage, and how slow consumers lose data.',
    body: [
      { p: 'With the delete cleanup policy, Kafka removes whole log segments once they are older than retention.ms (or the partition is bigger than retention.bytes). Data is deleted whether or not anyone has read it. A consumer group that falls further behind than the retention time loses the oldest unread messages and jumps ahead (auto.offset.reset decides where).' },
      { p: 'Retention is also what makes replay possible: a new consumer can read the past, and a buggy consumer can be fixed and rewound to reprocess. Size retention for your longest plausible consumer outage (a long weekend is a good minimum) plus the history you want to replay. Tiered storage moves old segments to object storage, making long retention cheap.' },
      { p: 'With the compact cleanup policy, Kafka keeps the latest message for every key and removes older versions, so the topic behaves like a table that never forgets the current state. A message with a null value (a tombstone) deletes the key. Compacted topics suit current state: product catalogs, user settings, the changelog of a stream processor\'s state.' },
      { ul: ['delete: keep everything for a while. compact: keep the latest per key, forever.', 'compact,delete combines both: latest per key, but not older than the retention.', 'Retention protects against slow or stopped consumers only up to its length.', 'Monitor lag in time against retention: "the slowest group is 2 days behind, retention is 3 days".'] },
    ],
    key_points: ['Messages older than retention are deleted, read or not.', 'Size retention for your worst consumer outage.', 'Compaction keeps the latest value per key forever.'],
    quiz: [
      q('Retention is 24 hours. Consumers are down from Friday night to Monday morning. What happens?', ['Messages older than 24 hours are deleted before consumers return, so they are lost', 'Kafka pauses retention while consumers are down', 'Producers are blocked', 'Nothing, consumers read from replicas'], 0, 'Retention deletes data by age regardless of whether it was read. Keep retention longer than your longest outage.'),
      q('What does a compacted topic keep?', ['The latest message for each key', 'Only the first message for each key', 'Messages for exactly 7 days', 'Nothing after it is read'], 0, 'Compaction removes older versions of a key and keeps the newest, so the topic always holds the current state.'),
      q('Why is replay a big advantage of Kafka over a classic queue?', ['A consumer can rewind its offset and reprocess past events, for example after fixing a bug', 'Replay makes producers faster', 'Queues cannot store messages at all', 'Replay removes the need for backups'], 0, 'Because the log is kept, you can reprocess history: rebuild a search index, backfill a new service or fix bad output.'),
    ],
    try: { kind: 'incident', id: 'kafka-retention-loss', label: 'Recover from a weekend outage' },
  },
  {
    id: 'kafka-rebalancing', phase: 'phase-7', title: 'Rebalances, poison messages and dead letter topics', minutes: 9, tags: ['ConsumerGroup', 'StreamProcessor', 'KafkaConnect'],
    summary: 'Why groups rebalance, how rebalance storms happen, and how to keep one bad message from blocking a partition.',
    body: [
      { p: 'A rebalance reassigns partitions in a group. It happens when a consumer joins, leaves or is considered dead. A consumer is considered dead when it stops heartbeating for session.timeout.ms, or when it takes longer than max.poll.interval.ms between two polls. With the old eager protocol every consumer stops during a rebalance; the cooperative protocol only pauses the partitions that move, and static membership lets a restarting consumer keep its partitions.' },
      { p: 'A rebalance storm starts when one poll\'s worth of work takes longer than max.poll.interval.ms, for example 500 records that each wait on a slow database. Kafka evicts the consumer, rebalances, the batch is replayed, it is slow again, and so on: lots of activity, no progress, many duplicates. Fix it with smaller polls (max.poll.records), a longer interval, or faster processing.' },
      { p: 'A poison message is one that always fails: malformed, or triggering a bug. If the consumer retries it forever, its partition stops moving, and so does every other partition that consumer owns. Catch the failure, retry a few times (perhaps through retry topics with delays), then publish the message to a dead letter topic and move on. Alert on the dead letter topic and replay it once the cause is fixed.' },
      { ul: ['max.poll.records × time per record must stay well under max.poll.interval.ms.', 'Use cooperative rebalancing and static membership to make deploys painless.', 'Every consumer needs a dead letter path for messages it cannot handle.', 'Kafka Streams and Connect have built-in handlers (errors.tolerance, DLQ, deserialization exception handlers).'] },
    ],
    key_points: ['A slow batch beyond max.poll.interval.ms causes rebalance storms.', 'A poison message blocks its partition unless it goes to a dead letter topic.', 'Cooperative rebalancing and static membership reduce disruption.'],
    quiz: [
      q('Each record takes 1 s to process; max.poll.records is 500 and max.poll.interval.ms is 300,000. What happens?', ['The consumer is evicted mid-batch over and over: a rebalance storm', 'Nothing, 500 records take 5 minutes, which is fine', 'Kafka slows the producer', 'The broker drops 200 records'], 0, '500 × 1 s = 500 s, beyond the 300 s limit. Kafka thinks the consumer died and rebalances; the batch is replayed and the cycle repeats.'),
      q('One malformed message makes a consumer throw an exception every time. Without special handling, what happens?', ['The consumer retries it forever and its partitions stop moving', 'Kafka deletes the message', 'The message is sent to another group', 'The producer is told to resend it'], 0, 'Kafka does not skip messages for you. The consumer must catch the failure and park the message in a dead letter topic.'),
      q('What does the cooperative rebalancing protocol improve?', ['Only the partitions that move pause, instead of every consumer stopping', 'It removes the need for partitions', 'It guarantees exactly-once', 'It makes consumers idempotent'], 0, 'Eager rebalancing revokes everything from everyone. Cooperative (incremental) rebalancing moves only what must move.'),
    ],
    try: { kind: 'incident', id: 'kafka-rebalance-storm', label: 'End a rebalance storm' },
  },

  // ---- Phase 8 ---------------------------------------------------------------
  {
    id: 'kafka-schemas', phase: 'phase-8', title: 'Schemas, Schema Registry and evolution', minutes: 7, tags: ['SchemaRegistry', 'KafkaTopic'],
    summary: 'Why events need a contract, how a Schema Registry enforces compatible changes, and which changes are safe.',
    body: [
      { p: 'Kafka stores bytes; it does not know what is inside a message. Producers and consumers must agree on the format, and that agreement is a schema, written in Avro, Protobuf or JSON Schema. Without one, a producer that renames a field or changes a type breaks every consumer the moment it deploys.' },
      { p: 'A Schema Registry stores every version of each topic\'s schema. Producers register the schema they write and tag each message with the schema id; consumers fetch the schema by id and decode. Clients cache schemas, so the registry is rarely called. Most importantly, the registry checks every new version against a compatibility rule and rejects breaking changes before any message is written.' },
      { p: 'Backward compatibility (the default) means new consumers can read old data: you may add fields with defaults and delete fields. Forward compatibility means old consumers can read new data. Full means both. Renaming a field, changing its type or adding a required field without a default are breaking changes.' },
      { ul: ['Add new fields with defaults; never reuse a field name for a different meaning.', 'Upgrade consumers first under backward compatibility, producers first under forward.', 'Use the _TRANSITIVE variants to check against all versions, not just the last.', 'Treat event schemas as public APIs: other teams depend on them.'] },
    ],
    key_points: ['A schema is the contract between producers and consumers.', 'The registry rejects incompatible changes before they are used.', 'Add fields with defaults; do not rename or retype.'],
    quiz: [
      q('A producer renames "price" to "amount" and deploys. With a Schema Registry set to backward compatibility, what happens?', ['The registry rejects the new schema, so the change cannot ship and consumers keep working', 'Consumers read "amount" automatically', 'The broker converts old messages', 'Nothing, renames are always safe'], 0, 'A rename is a breaking change: old data has no "amount", new data has no "price". The registry refuses it.'),
      q('Which change is backward compatible?', ['Adding an optional field with a default value', 'Changing a field from string to integer', 'Renaming a field', 'Adding a required field without a default'], 0, 'New readers can fill the default when reading old data, so nothing breaks.'),
      q('Why do clients rarely call the Schema Registry?', ['They cache schemas by id after the first lookup', 'Schemas are sent inside every message', 'The broker answers for the registry', 'They only call it at night'], 0, 'Each message carries a small schema id; the client fetches the schema once and caches it.'),
    ],
    try: { kind: 'incident', id: 'kafka-schema-break', label: 'Survive a breaking schema deploy' },
  },
  {
    id: 'kafka-connect-cdc', phase: 'phase-8', title: 'Kafka Connect, CDC and the outbox pattern', minutes: 9, tags: ['KafkaConnect', 'Database'],
    summary: 'Moving data in and out of Kafka without custom code, streaming database changes, and why dual writes go wrong.',
    body: [
      { p: 'Kafka Connect runs connectors: source connectors bring data into Kafka, sink connectors write topics into other systems such as search indexes, object storage or databases. A connector runs as a number of tasks that split the work by partition, and Connect stores offsets so a restarted task continues where it stopped. Errors can be tolerated and routed to a dead letter topic.' },
      { p: 'Change data capture (CDC) is a source connector, Debezium being the best known, that reads the database\'s transaction log and publishes every committed insert, update and delete as an event. It adds no work to the write path and sees exactly what was committed, in commit order.' },
      { p: 'A dual write is when a service writes to its database and publishes to Kafka as two separate steps. If it crashes between them, or one step fails, the database and the events disagree forever. The transactional outbox fixes this: the service writes the event into an outbox table in the same database transaction as the data change, and CDC streams the outbox to Kafka. Either both happen or neither does.' },
      { ul: ['Prefer connectors to hand-written consumers for plain copying.', 'CDC gives events for free from systems that cannot publish them.', 'Never dual-write; use the outbox (or listen-to-yourself: publish first, and update the database from the topic).', 'Sink connectors should write idempotently (upsert by key) so replays are safe.'] },
    ],
    key_points: ['Connect runs source and sink connectors as parallel tasks.', 'CDC turns committed database changes into events.', 'The outbox pattern avoids dual writes.'],
    quiz: [
      q('A service saves an order and then publishes OrderPlaced. It crashes between the two steps. What happens?', ['The order exists but no event was published, so downstream services never hear about it', 'Kafka publishes the event automatically', 'The database rolls back', 'The event is published twice'], 0, 'That is the dual-write problem. With an outbox table written in the same transaction and streamed by CDC, both happen or neither does.'),
      q('How does a CDC connector such as Debezium learn about changes?', ['It reads the database\'s transaction log', 'It polls every table with SELECT * every second', 'The application calls it after each write', 'It compares nightly backups'], 0, 'Reading the log costs the database almost nothing and captures every committed change in order.'),
      q('What limits how many tasks of a sink connector can work in parallel?', ['The number of partitions of the topics it reads', 'The number of brokers', 'The retention time', 'The schema format'], 0, 'Sink tasks are consumers in a group, so each partition is handled by one task at a time.'),
    ],
    try: { kind: 'incident', id: 'kafka-dual-write', label: 'Fix events that disagree with the database' },
  },
  {
    id: 'kafka-streams', phase: 'phase-8', title: 'Stream processing: Kafka Streams, ksqlDB and Flink', minutes: 9, tags: ['StreamProcessor'],
    summary: 'Filtering, joining and aggregating streams in real time, state stores and changelogs, windows, and exactly-once.',
    body: [
      { p: 'Stream processors read topics continuously, transform the events and write results to new topics. Stateless steps (filter, map, route) look at one event at a time. Stateful steps (counts, sums, joins, deduplication) remember things, and keep that memory in a local state store, usually RocksDB, on the processing instance.' },
      { p: 'Every state store is backed up to a compacted changelog topic. If an instance dies, another rebuilds the store by replaying the changelog, which can take minutes for big state; standby replicas keep a warm copy on another instance so failover takes seconds. Like consumer groups, the parallelism of a stream app is set by the number of input partitions.' },
      { p: 'Windows group events by time: tumbling windows are fixed, non-overlapping buckets (per minute); hopping windows overlap (5-minute windows every minute); session windows close after a gap of inactivity. Event time (when it happened) differs from processing time (when it arrived), so windows allow a grace period for late events. Joins combine streams (clicks with impressions) or a stream with a table (orders with customer details).' },
      { ul: ['Kafka Streams is a Java library embedded in your app; ksqlDB adds SQL; Flink is a separate cluster for big or complex jobs.', 'Turn on exactly-once (exactly_once_v2) when results must not double count.', 'Co-partition joined topics: same key, same partition count.', 'Standby replicas cut failover time for stateful apps.'] },
    ],
    key_points: ['Stateful processing keeps state locally, backed by a changelog topic.', 'Standby replicas make failover fast.', 'Windows group events by time; plan for late events.'],
    quiz: [
      q('An instance of a stateful Kafka Streams app dies. How does another instance get its state?', ['It replays the state store\'s changelog topic, or takes over a warm standby replica', 'It asks the database for the state', 'The state is lost and starts from zero', 'The broker sends it the RocksDB files'], 0, 'Every store has a compacted changelog topic. Replaying it rebuilds the store; standby replicas avoid the wait.'),
      q('Which window counts page views per minute, with each view in exactly one window?', ['A tumbling window of one minute', 'A session window', 'A hopping window of 5 minutes every minute', 'No window'], 0, 'Tumbling windows are fixed and non-overlapping, so each event belongs to exactly one.'),
      q('What limits how many instances of a stream processing app can do useful work?', ['The number of partitions of its input topics', 'The number of brokers', 'The size of its state store', 'The number of output topics'], 0, 'Work is divided into tasks by input partition, exactly like consumers in a group.'),
    ],
    try: { kind: 'problem', id: 'bank_ledger', label: 'Build an exactly-once ledger' },
  },
  {
    id: 'kafka-operations', phase: 'phase-8', title: 'Running Kafka: sizing, monitoring, security and regions', minutes: 9, tags: ['KafkaTopic', 'MirrorMaker'],
    summary: 'How much a cluster can carry, the metrics that matter, locking a cluster down, and spanning regions.',
    body: [
      { p: 'Size a cluster from bytes, not messages: incoming megabytes per second times the replication factor is what the brokers\' disks and network must carry. A broker typically handles tens of megabytes per second of writes comfortably. Leave headroom for spikes, rebalancing traffic and a broker being down, and remember that compression and batching change the numbers a lot.' },
      { p: 'Watch these metrics: consumer lag (in time) per group, under-replicated and offline partitions, ISR shrink and expand rates, request latency for produce and fetch, and disk usage against retention. Under-replicated partitions are usually the first sign that a broker, disk or network link is struggling.' },
      { p: 'Secure every cluster: TLS for traffic, authentication for every client (SASL/SCRAM, mTLS or OAuth), and ACLs so each service can only read and write the topics it needs. Quotas stop one noisy client from starving the rest. Across regions, MirrorMaker 2 (or Cluster Linking) copies topics between clusters for disaster recovery or aggregation, and can translate consumer offsets so consumers can fail over; replication across regions is asynchronous, so a regional failure can lose the last few seconds.' },
      { ul: ['Bytes in × replication factor ≈ what the cluster must write.', 'Alert on under-replicated partitions and lag in time.', 'TLS + authentication + ACLs + quotas for every production cluster.', 'Cross-region mirroring is asynchronous: plan for a small loss window on regional failover.'] },
    ],
    key_points: ['Size from bytes × replication factor.', 'Lag, under-replicated partitions and request latency are the key signals.', 'Mirroring between regions is asynchronous.'],
    quiz: [
      q('Producers send 30 MB/s to a topic with replication factor 3. Roughly how much do the brokers write in total?', ['About 90 MB/s', 'About 30 MB/s', 'About 10 MB/s', 'About 270 MB/s'], 0, 'Every byte is stored on three brokers, so the cluster writes about three times the incoming volume.'),
      q('Which metric usually warns first that a broker is in trouble?', ['Under-replicated partitions', 'The number of topics', 'Consumer group names', 'Schema versions'], 0, 'When a broker cannot keep up, its followers drop out of the ISR and partitions become under-replicated.'),
      q('Why can failing over to a mirrored cluster in another region lose a few seconds of data?', ['Mirroring between clusters is asynchronous, so the newest messages may not have been copied yet', 'MirrorMaker deletes messages', 'Consumers cannot read mirrored topics', 'acks=all does not work across regions'], 0, 'The mirror lags slightly behind the source. Anything written but not yet copied is missing after failover.'),
    ],
    try: { kind: 'problem', id: 'iot_telemetry', label: 'Mirror telemetry across regions' },
  },
];

// ================================================================ content: roadmap
// Learning roadmap: five system design phases, then three Kafka phases (kafka_roadmap.js),
// four topics each. Quiz answers stay on the server.


const DESIGN_PHASES = [
  { id: 'phase-1', number: 1, title: 'Building blocks', summary: 'Requests, latency and the two components almost every system starts with.' },
  { id: 'phase-2', number: 2, title: 'How systems work', summary: 'Databases, replication, queues and the edge.' },
  { id: 'phase-3', number: 3, title: 'Handling scale', summary: 'Scaling out, planning capacity, limiting abuse and splitting data.' },
  { id: 'phase-4', number: 4, title: 'Distributed data', summary: 'Consistency, invalidation, delivery guarantees and search.' },
  { id: 'phase-5', number: 5, title: 'Production', summary: 'Resilience patterns, observability, redundancy and security.' },
];

const DESIGN_TOPICS = [
  // ---- Phase 1 ---------------------------------------------------------------
  {
    id: 'client-server', phase: 'phase-1', title: 'Requests, responses and statelessness', minutes: 6, tags: ['Server', 'general'],
    summary: 'What happens between a click and a response, and why stateless servers are the foundation of scaling.',
    body: [
      { p: 'A request travels from the client through DNS, a TCP and TLS handshake, possibly a CDN and a load balancer, to a server that does the work and answers. Each hop adds latency, and each is a place where things can fail.' },
      { p: 'A server is stateless when it keeps nothing about a user between requests: sessions, carts and uploads live in a shared store (a cache, database or object storage). Any instance can then serve any request, which is what lets you add servers behind a load balancer or replace a dead one without users noticing.' },
      { ul: ['Keep request handling idempotent where you can: clients and proxies retry.', 'Use status codes deliberately: 4xx means the caller must change something, 5xx means you failed, 429 means slow down.', 'Measure server time separately from network time; they have different fixes.'] },
    ],
    key_points: ['Every hop adds latency and a failure point.', 'Stateless servers are interchangeable, which makes horizontal scaling possible.', 'State belongs in shared stores, not in server memory.'],
    quiz: [
      q('What makes an application server stateless?', ['It keeps no per-user data between requests; that data lives in a shared store', 'It never talks to a database', 'It only serves GET requests', 'It runs without a load balancer'], 0, 'Statelessness is about where session data lives. If it is in a shared store, any instance can handle any request.'),
      q('A client receives HTTP 429. What should it do?', ['Back off and retry later, ideally honouring Retry-After', 'Retry immediately in a tight loop', 'Treat it as a permanent failure', 'Switch to HTTP/1.0'], 0, '429 Too Many Requests means you are being rate limited. Back off, respecting Retry-After if present.'),
      q('Why do stateless servers make failover easier?', ['Any healthy instance can pick up a dead instance\'s users without losing their session', 'They never crash', 'They do not need health checks', 'They store backups locally'], 0, 'Because no user state is trapped in the failed instance, traffic can simply move to the others.'),
    ],
    try: { kind: 'problem', id: 'url_shortener', label: 'Design a URL shortener' },
  },
  {
    id: 'latency-throughput', phase: 'phase-1', title: 'Latency, throughput and utilization', minutes: 7, tags: ['general'],
    summary: 'The three numbers that describe any component, and why latency explodes as utilization approaches 100%.',
    body: [
      { p: 'Latency is how long one request takes. Throughput is how many requests a component completes per second. Utilization is throughput divided by capacity. They are linked: as utilization climbs, requests start waiting in queues, and latency grows slowly at first and then very fast past roughly 80–90%.' },
      { p: 'Little\'s law ties them together: the number of requests in flight equals arrival rate times time in the system (L = λ × W). A server handling 1,000 requests per second at 50 ms has about 50 requests in flight. If latency doubles, so does concurrency, which is how slowness turns into exhausted thread pools.' },
      { ul: ['Report percentiles (p95, p99), not averages: the tail is what users remember.', 'Keep steady-state utilization well below 100% to leave room for spikes.', 'A slow dependency raises concurrency everywhere upstream.'] },
    ],
    key_points: ['Latency rises sharply as utilization nears 100%.', 'Little\'s law: in-flight requests = rate × latency.', 'Tail percentiles matter more than averages.'],
    quiz: [
      q('A service receives 2,000 requests per second and each takes 100 ms. Roughly how many requests are in flight?', ['200', '20', '2,000', '20,000'], 0, 'Little\'s law: 2,000/s × 0.1 s = 200 requests in flight.'),
      q('Why do teams watch p99 latency rather than the average?', ['Averages hide the slow tail that a meaningful share of users experience', 'p99 is always lower than the average', 'Averages cannot be computed for HTTP', 'p99 measures throughput'], 0, 'One request in a hundred is a lot of users at scale, and averages smooth those slow requests away.'),
      q('What typically happens to latency when a component goes from 70% to 95% utilization?', ['It increases many times over as queues build up', 'It stays the same until 100%', 'It decreases because caches warm up', 'It increases by exactly 25%'], 0, 'Queueing delay grows non-linearly; near saturation, small load increases cause large latency jumps.'),
    ],
    try: { kind: 'incident', id: 'signup-launch-day', label: 'Fix an overloaded launch' },
  },
  {
    id: 'load-balancing', phase: 'phase-1', title: 'Load balancers', minutes: 6, tags: ['LoadBalancer'],
    summary: 'How traffic is spread across servers, how dead servers are removed, and which algorithm to pick.',
    body: [
      { p: 'A load balancer sits in front of identical backends and spreads requests across them. Layer 4 balancers route connections by IP and port; layer 7 balancers understand HTTP and can route by path, header or cookie.' },
      { p: 'Health checks are what make a load balancer a reliability tool: a backend that fails its checks is removed from rotation within seconds. The balancer itself must not become a single point of failure, so run several instances or use a managed one.' },
      { ul: ['Round robin assumes identical backends and similar requests.', 'Least connections adapts to uneven request cost and backend size.', 'Consistent hashing keeps a key on the same backend, useful for caches.', 'Sticky sessions simplify stateful apps but unbalance load and lose sessions on failure.'] },
    ],
    key_points: ['Health checks remove dead backends automatically.', 'Pick the algorithm for your backends and request mix.', 'Run more than one balancer instance.'],
    quiz: [
      q('Two backends differ greatly in size. Which algorithm avoids overloading the small one?', ['Least connections', 'Round robin', 'Random', 'DNS round robin'], 0, 'Least connections sends work to whichever backend has the fewest requests in flight, which naturally favours the larger one.'),
      q('What happens when health checks are turned off and a backend dies?', ['The balancer keeps sending it a share of traffic, and those requests fail', 'Traffic automatically shifts to healthy backends', 'The balancer restarts the backend', 'Nothing, health checks are only for metrics'], 0, 'Without health checks the balancer cannot know a backend is dead.'),
      q('What is the main drawback of sticky sessions?', ['Load becomes uneven and sessions are lost when the pinned server dies', 'They make TLS impossible', 'They double latency', 'They require a CDN'], 0, 'Pinning users to servers ties capacity and state to individual machines.'),
    ],
    try: { kind: 'incident', id: 'uneven-backends', label: 'Balance uneven backends' },
  },
  {
    id: 'caching', phase: 'phase-1', title: 'Caching basics', minutes: 8, tags: ['Cache'],
    summary: 'Where caches go, how hit ratio translates into database load, and what TTLs and eviction do.',
    body: [
      { p: 'A cache keeps recent answers in memory so repeated reads skip the slow store. In the cache-aside pattern the application checks the cache first, reads the database on a miss and writes the result back. In read-through, the cache itself fetches misses.' },
      { p: 'The arithmetic is simple and worth doing: with a 90% hit ratio, only 10% of reads reach the database, but every write still does. A workload that is 95% reads drops from 100% of traffic at the database to 0.95 × 10% + 5% ≈ 14.5%.' },
      { ul: ['TTL bounds how stale data can be; too short and the hit ratio collapses.', 'Eviction (LRU, LFU) decides what to drop when memory is full.', 'Caches help skewed, repeated reads most. Unique or write-heavy traffic gains little.'] },
    ],
    key_points: ['Database load after a cache = read misses + all writes.', 'TTL trades freshness against hit ratio.', 'Caches shine on skewed read traffic.'],
    quiz: [
      q('A workload is 90% reads at 10,000 rps. With a 90% cache hit ratio, roughly how many requests per second reach the database?', ['1,900', '1,000', '9,000', '100'], 0, 'Reads: 9,000 × 10% = 900 misses. Writes: 1,000 all reach the database. Total ≈ 1,900.'),
      q('What is the effect of setting a 2-second TTL on keys that are re-read every 30 seconds?', ['Almost every read misses, so the cache barely helps', 'The hit ratio improves', 'Writes become faster', 'The database load halves'], 0, 'Entries expire long before they are reused, so the effective hit ratio is close to zero.'),
      q('In cache-aside, who loads data into the cache on a miss?', ['The application, after reading the database', 'The database pushes it automatically', 'The CDN', 'Nobody; the cache is filled at startup only'], 0, 'Cache-aside means the application owns reading from the store and populating the cache.'),
    ],
    try: { kind: 'incident', id: 'bookstore-read-storm', label: 'Stop a read storm' },
  },

  // ---- Phase 2 ---------------------------------------------------------------
  {
    id: 'databases', phase: 'phase-2', title: 'Choosing and using a database', minutes: 8, tags: ['Database'],
    summary: 'Relational versus NoSQL, indexes, transactions and connection pools.',
    body: [
      { p: 'Relational databases give you schemas, joins and ACID transactions: the right default when data is related and correctness matters. NoSQL stores (key-value, document, wide-column) trade some of that for simpler scaling and flexible schemas; they shine when access patterns are known and simple.' },
      { p: 'Indexes are the biggest performance lever. Without one, a query reads every row; with one, it jumps to the rows it needs. Indexes cost write speed and storage, so add them for the queries you actually run.' },
      { ul: ['Use transactions for invariants that must never break (no double-booked seat).', 'Size connection pools deliberately: too few starve the app, too many overwhelm the database.', 'Large blobs belong in object storage, not in rows.'] },
    ],
    key_points: ['Pick the store for your access patterns and consistency needs.', 'Indexes turn scans into lookups.', 'Keep blobs out of rows.'],
    quiz: [
      q('A query filters on a column with no index. What happens as the table grows?', ['Each query scans more rows, so latency and load grow with the table', 'Nothing changes', 'The database adds an index automatically', 'Writes slow down but reads do not'], 0, 'Without an index the database must read the whole table to find matches.'),
      q('Where should 10 MB file uploads be stored?', ['Object storage, with metadata in the database', 'In a database BLOB column', 'In the cache', 'On the app server\'s disk'], 0, 'Object storage is built for large, immutable objects; databases are built for queryable records.'),
      q('Which requirement most strongly favours a relational database?', ['Multi-row updates that must succeed or fail together', 'Storing images', 'Very simple key lookups', 'Schema-less documents'], 0, 'ACID transactions across rows are the relational database\'s core strength.'),
    ],
    try: { kind: 'problem', id: 'pastebin', label: 'Design a pastebin' },
  },
  {
    id: 'replication', phase: 'phase-2', title: 'Replication and failover', minutes: 8, tags: ['Database'],
    summary: 'Why replicas help reads and availability but not writes, and what replication lag means for users.',
    body: [
      { p: 'A primary accepts writes and streams them to replicas, which serve reads. Replicas add read capacity and a standby to promote if the primary dies. They do nothing for write throughput: every write still lands on the primary.' },
      { p: 'With asynchronous replication the primary does not wait for replicas, so writes are fast but replicas lag slightly behind. A user who updates their profile and immediately reads from a replica may see the old value. Fixes include reading your own writes from the primary or pinning a session to the primary briefly.' },
      { ul: ['Synchronous replication never loses acknowledged writes but makes every write slower.', 'Automatic failover needs a replica to promote and health checks to decide when.', 'Replicas copy mistakes too; backups are still required.'] },
    ],
    key_points: ['Replicas scale reads and availability, not writes.', 'Async replication means lag and stale reads.', 'Failover needs replicas plus automation; recovery needs backups.'],
    quiz: [
      q('Write traffic saturates the primary. Will adding read replicas help?', ['No, writes still all go to the primary', 'Yes, replicas share the writes', 'Yes, but only with sync replication', 'Only if failover is enabled'], 0, 'Replicas take reads; scaling writes needs more primaries (sharding) or fewer writes.'),
      q('A user saves a change and immediately reloads, but sees the old value. What is the likely cause?', ['The read went to an asynchronous replica that had not caught up', 'The cache was too large', 'The load balancer used round robin', 'The write was rate limited'], 0, 'Replication lag makes reads from replicas briefly stale.'),
      q('Why keep backups when you already have replicas?', ['An accidental delete or corruption replicates instantly; backups let you go back in time', 'Backups are faster to query', 'Replicas cannot be promoted', 'Backups replace failover'], 0, 'Replication protects against hardware loss, not against bad changes.'),
    ],
    try: { kind: 'incident', id: 'single-database-outage', label: 'Survive a database failure' },
  },
  {
    id: 'queues', phase: 'phase-2', title: 'Queues and asynchronous work', minutes: 7, tags: ['Queue', 'Worker'],
    summary: 'Decoupling producers from consumers to absorb bursts and keep slow work off the request path.',
    body: [
      { p: 'A queue lets a producer hand off work and move on. Consumers (workers) pull messages at their own pace. This keeps slow tasks such as sending email, transcoding or fan-out out of the request, and absorbs bursts: the queue grows during a spike and drains afterwards.' },
      { p: 'Queues hide problems as well as solve them. If consumers fall behind, nothing fails right away; the backlog just grows until the queue is full and producers start getting errors. Monitor the backlog and especially the age of the oldest message.' },
      { ul: ['Most queues deliver at least once, so consumers must be idempotent.', 'Dead-letter queues park messages that keep failing.', 'Strict ordering limits parallelism to one consumer per partition.'] },
    ],
    key_points: ['Queues smooth bursts and take slow work off the request path.', 'A growing backlog is the main failure signal.', 'Design consumers for at-least-once delivery.'],
    quiz: [
      q('Producers add 500 messages/s and consumers process 300/s. The queue holds 120,000 messages. Roughly when does it fill up?', ['After about 10 minutes', 'Never', 'After 4 minutes', 'Immediately'], 0, 'Backlog grows by 200/s; 120,000 / 200 = 600 s = 10 minutes.'),
      q('Which metric best detects a stalled consumer early?', ['Age of the oldest message in the queue', 'Producer CPU', 'Number of queues', 'Message size'], 0, 'If the oldest message keeps getting older, nothing is draining the queue.'),
      q('Why does a queue make a checkout faster when sending a receipt email?', ['The request only enqueues a message; the email is sent afterwards by a worker', 'Queues make email providers faster', 'The email is skipped', 'The database writes become async'], 0, 'The user no longer waits for the email provider.'),
    ],
    try: { kind: 'incident', id: 'order-emails-stopped', label: 'Unstick a stalled queue' },
  },
  {
    id: 'cdn', phase: 'phase-2', title: 'CDNs and the edge', minutes: 6, tags: ['CDN'],
    summary: 'Serving cacheable content close to users, and keeping it fresh.',
    body: [
      { p: 'A content delivery network caches responses at edge locations near users. Static assets, images, video segments and other public, cacheable responses can be answered without touching your origin, which cuts latency and origin load at the same time.' },
      { p: 'Freshness is controlled by cache headers and TTLs. For assets, the standard trick is fingerprinting: put a content hash in the file name so each build gets a new URL, then cache it for a year. For data that changes, use short TTLs or explicit purges, and never cache personalised responses publicly.' },
      { ul: ['Hit ratio at the edge directly divides origin traffic.', 'A full purge sends everything to the origin at once; size for it or use an origin shield.', 'Mark user-specific responses private.'] },
    ],
    key_points: ['The edge answers cacheable requests near users.', 'Fingerprinted assets make long TTLs safe.', 'Never publicly cache personalised data.'],
    quiz: [
      q('How do you ship a new JS bundle that the CDN caches for a year?', ['Give it a new, content-hashed file name', 'Purge the CDN on every deploy', 'Set the TTL to zero', 'Disable the CDN for JS'], 0, 'A new URL is a new cache entry; old URLs can stay cached forever.'),
      q('With a CDN hit ratio of 80%, how much traffic reaches the origin?', ['About 20%', 'About 80%', 'None', 'All of it'], 0, 'Only misses go to the origin.'),
      q('Why is caching account pages at the CDN dangerous?', ['One user\'s personalised page could be served to another', 'It makes pages slower', 'CDNs cannot cache HTML', 'It disables TLS'], 0, 'Public caches must only hold responses that are the same for everyone.'),
    ],
    try: { kind: 'incident', id: 'static-assets-from-app', label: 'Move static files to the edge' },
  },

  // ---- Phase 3 ---------------------------------------------------------------
  {
    id: 'horizontal-scaling', phase: 'phase-3', title: 'Scaling out', minutes: 6, tags: ['Server', 'LoadBalancer'],
    summary: 'Adding machines instead of bigger machines, and what has to be true for that to work.',
    body: [
      { p: 'Vertical scaling (a bigger machine) is simple but hits a ceiling and keeps a single point of failure. Horizontal scaling (more machines) has no hard ceiling and adds redundancy, but needs stateless services and a load balancer.' },
      { p: 'Plan for N+1: enough instances that losing one still leaves headroom. If three servers each run at 80%, losing one pushes the other two to 120% and they fall over too. Scaling out only moves the bottleneck; the next limit is usually the database.' },
      { ul: ['Externalise sessions and uploads before scaling out.', 'Size so that N−1 instances can carry the peak.', 'Expect the bottleneck to move downstream.'] },
    ],
    key_points: ['Horizontal scaling needs stateless services.', 'Plan for losing an instance at peak (N+1).', 'Scaling one tier exposes the next bottleneck.'],
    quiz: [
      q('Three servers run at 80% at peak. One dies. What happens?', ['The remaining two go to about 120% and start failing', 'Nothing, they absorb it easily', 'The load balancer adds a server automatically', 'Latency improves'], 0, 'Total load is 240% of one server; split over two that is 120% each.'),
      q('What must be true before you can add app servers behind a load balancer?', ['The servers keep no per-user state locally', 'The servers have identical IP addresses', 'The database is sharded', 'There is a CDN'], 0, 'Statelessness lets any server handle any request.'),
      q('After scaling the app tier, latency is still high. What is the most likely next bottleneck?', ['The database', 'The load balancer\'s algorithm', 'DNS', 'The browser'], 0, 'More app servers send more queries to the same database.'),
    ],
    try: { kind: 'problem', id: 'url_shortener', label: 'Scale a URL shortener' },
  },
  {
    id: 'capacity-planning', phase: 'phase-3', title: 'Capacity planning and autoscaling', minutes: 7, tags: ['Server', 'general'],
    summary: 'Back-of-the-envelope estimates, headroom, and why autoscaling is slower than spikes.',
    body: [
      { p: 'Start with arithmetic: users × requests per user = requests per second. Divide by per-instance capacity to get instance counts, then add headroom for spikes and for losing an instance. Do the same for each tier, remembering that caches and CDNs divide the traffic behind them.' },
      { p: 'Autoscaling adds instances as load rises, which saves money most of the time, but new instances take minutes to boot and warm up. A spike that arrives in seconds lands on whatever is already running. Keep a minimum that covers sudden bursts, scale on leading signals, and set the maximum high enough to matter.' },
      { ul: ['Typical autoscaling targets keep utilization around 50–70%.', 'An autoscaling ceiling is still a ceiling.', 'Check every tier: databases rarely autoscale.'] },
    ],
    key_points: ['users × rate = rps; divide by capacity per instance.', 'Autoscaling lags fast spikes.', 'Plan headroom per tier.'],
    quiz: [
      q('500,000 users each poll every 20 seconds. Roughly what is the request rate?', ['25,000 rps', '10 million rps', '2,500 rps', '500,000 rps'], 0, '500,000 × (1/20) = 25,000 requests per second.'),
      q('Traffic jumps 5× within ten seconds. What protects you in the first minute?', ['Capacity that is already running (a higher minimum) and caches in front', 'The autoscaler', 'Adding database replicas', 'Lowering the TTL'], 0, 'Autoscalers react in minutes; the first minute is served by what is already there.'),
      q('Why might an autoscaled service still fail at a mega spike?', ['It reached its maximum instance count or the database behind it saturated', 'Autoscaling never adds instances', 'Autoscaling disables health checks', 'It always scales too fast'], 0, 'Autoscaling stops at its ceiling and cannot fix a downstream bottleneck.'),
    ],
    try: { kind: 'problem', id: 'live_scores', label: 'Plan for a 10× spike' },
  },
  {
    id: 'rate-limiting', phase: 'phase-3', title: 'Rate limiting', minutes: 7, tags: ['RateLimiter', 'APIGateway'],
    summary: 'Protecting a system from abusive or runaway clients without hurting everyone else.',
    body: [
      { p: 'A rate limiter caps how fast a client may call you. The token bucket algorithm refills tokens at a steady rate and lets clients spend them in bursts; sliding windows count requests in a moving time window. Rejected requests get HTTP 429, ideally with a Retry-After header.' },
      { p: 'What you key the limit on matters: per API key or user for customers, per IP for anonymous traffic. A single global limit punishes everyone equally and does little against a few heavy abusers. With several limiter instances, counters must be shared (commonly in Redis), otherwise a client spread across instances gets several times its quota.' },
      { ul: ['Limit at the edge so rejected requests are cheap.', 'Allow bursts; real clients are bursty.', 'Decide what happens if the limiter itself is down (fail open or closed).'] },
    ],
    key_points: ['Key limits by client, not globally.', 'Share counters across limiter instances.', 'Reject early and cheaply with 429.'],
    quiz: [
      q('Why is one global limit a poor defence against a few abusive clients?', ['It rejects legitimate clients as often as abusive ones', 'Global limits cannot be implemented', 'It makes the abusive clients faster', 'It disables TLS'], 0, 'Per-client limits target the heavy hitters; a global cap spreads the pain to everyone.'),
      q('Four limiter instances each keep counters in local memory. A client\'s requests are spread evenly across them. What quota does it effectively get?', ['About four times its limit', 'Exactly its limit', 'A quarter of its limit', 'Zero'], 0, 'Each instance allows the full limit independently.'),
      q('Which algorithm naturally allows short bursts while capping the average rate?', ['Token bucket', 'Fixed window', 'Round robin', 'Least connections'], 0, 'Accumulated tokens allow bursts; the refill rate caps the average.'),
    ],
    try: { kind: 'incident', id: 'bank-credential-stuffing', label: 'Stop a login botnet' },
  },
  {
    id: 'sharding', phase: 'phase-3', title: 'Sharding and partitioning', minutes: 8, tags: ['Database'],
    summary: 'Splitting data across primaries to scale writes, and choosing a shard key you can live with.',
    body: [
      { p: 'Sharding splits a dataset across several primaries, each owning a slice of the keys. Writes and storage scale with the number of shards, which is the main reason to do it: replicas cannot scale writes.' },
      { p: 'The shard key decides everything. Hashing a key spreads load evenly but makes range queries span every shard; range partitioning keeps ranges together but risks hot spots (all of today\'s orders on one shard). Queries that need data from several shards become slower and harder, and rebalancing data when you add shards needs care; consistent hashing limits how much moves.' },
      { ul: ['Pick a key that appears in most queries and spreads writes evenly.', 'Watch for hot keys: one celebrity can overload one shard.', 'Avoid cross-shard transactions where you can.'] },
    ],
    key_points: ['Sharding scales writes and storage.', 'The shard key determines balance and query cost.', 'Hot keys and cross-shard queries are the main pains.'],
    quiz: [
      q('Which problem does sharding solve that read replicas do not?', ['Write throughput beyond one primary', 'Stale reads', 'TLS termination', 'Cache stampedes'], 0, 'Each shard has its own primary, so writes spread across machines.'),
      q('You shard orders by creation date range. What risk does that create?', ['All current writes land on the newest shard', 'Range queries become impossible', 'Reads become stale', 'Replicas stop working'], 0, 'Time-based ranges concentrate today\'s writes on one shard.'),
      q('Why is consistent hashing used when adding shards?', ['It moves only a small fraction of keys instead of reshuffling almost everything', 'It encrypts the keys', 'It removes the need for a shard key', 'It makes queries faster'], 0, 'With plain modulo hashing, changing the shard count remaps most keys.'),
    ],
    try: { kind: 'incident', id: 'dinner-rush-writes', label: 'Scale writes at dinner rush' },
  },

  // ---- Phase 4 ---------------------------------------------------------------
  {
    id: 'consistency', phase: 'phase-4', title: 'Consistency and the CAP trade-off', minutes: 8, tags: ['Database', 'general'],
    summary: 'Strong versus eventual consistency, and choosing per feature instead of per system.',
    body: [
      { p: 'Strong consistency means every read sees the latest write. Eventual consistency means replicas converge if writes stop, but a read may briefly see old data. During a network partition, a distributed system must choose between staying available (and risking stale or conflicting data) and staying consistent (and refusing some requests): that is the CAP trade-off.' },
      { p: 'Most products mix both. A like counter or a feed can be eventually consistent; a payment, a seat reservation or a balance cannot. Even without partitions there is a latency trade: waiting for replicas makes writes safer but slower.' },
      { ul: ['Decide consistency per feature.', 'Read-your-writes covers the most noticeable staleness.', 'Use transactions or conditional writes for invariants like "no seat sold twice".'] },
    ],
    key_points: ['CAP: during a partition, choose availability or consistency.', 'Choose consistency per feature.', 'Protect invariants with transactions or conditional writes.'],
    quiz: [
      q('Which feature most needs strong consistency?', ['Reserving the last seat for a concert', 'Counting likes on a post', 'Showing a news feed', 'Displaying a profile photo'], 0, 'Selling the same seat twice breaks a hard invariant; the others tolerate brief staleness.'),
      q('What does eventual consistency guarantee?', ['Replicas converge to the same value once writes stop', 'Every read sees the latest write', 'Writes never fail', 'Reads are always fast'], 0, 'It promises convergence, not immediate visibility.'),
      q('During a network partition, a strongly consistent system typically does what?', ['Rejects some requests rather than return possibly stale data', 'Returns stale data', 'Duplicates every write', 'Disables replication permanently'], 0, 'Choosing consistency means sacrificing availability on the minority side of a partition.'),
    ],
    try: { kind: 'problem', id: 'ticket_booking', label: 'Sell tickets without overselling' },
  },
  {
    id: 'cache-invalidation', phase: 'phase-4', title: 'Cache invalidation and stampedes', minutes: 8, tags: ['Cache'],
    summary: 'Keeping caches correct, and keeping a cold cache from knocking over the database.',
    body: [
      { p: 'Caches go stale when the source changes. Options include short TTLs (simple, bounded staleness), deleting the key on every write (fresh, but every writer must remember), and versioned keys (write a new key and let old ones expire).' },
      { p: 'The dangerous moment is a cold cache. After a restart or the expiry of a hot key, many requests miss at once and all hit the database for the same data: a stampede, or thundering herd. Request coalescing (one request refills a key while the others wait), several cache nodes, and jittered TTLs keep a cache blip from turning into a database outage.' },
      { ul: ['Delete on write, then let the next read refill.', 'Add jitter so hot keys do not expire together.', 'Size the database for a realistic miss rate, not the best case.'] },
    ],
    key_points: ['Pick an invalidation strategy deliberately.', 'Cold caches cause stampedes.', 'Coalescing, multiple nodes and jitter prevent them.'],
    quiz: [
      q('A single cache node restarts and the database falls over. What is this called?', ['A cache stampede (thundering herd)', 'Replication lag', 'A hot partition', 'A retry storm'], 0, 'Many simultaneous misses for the same keys overwhelm the store behind the cache.'),
      q('What does request coalescing do?', ['Lets one request refill a missing key while other requests for it wait', 'Combines writes into batches', 'Compresses cache entries', 'Spreads keys across nodes'], 0, 'Also called single-flight: one database read instead of hundreds.'),
      q('Why add random jitter to TTLs?', ['So many hot keys do not expire at the same moment', 'To make entries live forever', 'To encrypt the cache', 'To reduce memory use'], 0, 'Synchronised expiry causes synchronised misses.'),
    ],
    try: { kind: 'incident', id: 'cache-restart-stampede', label: 'Prevent a cache stampede' },
  },
  {
    id: 'idempotency', phase: 'phase-4', title: 'Idempotency and delivery guarantees', minutes: 7, tags: ['Queue', 'Worker', 'ThirdPartyAPI'],
    summary: 'Why messages arrive twice and how to make that harmless.',
    body: [
      { p: 'At-most-once delivery may lose messages; at-least-once delivery may duplicate them; exactly-once is expensive and usually means at-least-once plus deduplication. Retries, crashed consumers and network timeouts all create duplicates, so a system that cannot tolerate them will eventually double-charge someone.' },
      { p: 'An operation is idempotent when doing it twice has the same effect as doing it once. Common techniques are idempotency keys (the client sends a unique key; the server stores the result and returns it on repeats), unique constraints, and conditional updates. The outbox pattern writes a business change and its outgoing event in one transaction, so neither is lost.' },
      { ul: ['Assume every message can arrive twice.', 'Store idempotency keys with the result of the first attempt.', 'Use a dead-letter queue for messages that keep failing.'] },
    ],
    key_points: ['At-least-once delivery means duplicates.', 'Idempotency keys make retries safe.', 'The outbox pattern keeps data and events in step.'],
    quiz: [
      q('A worker charges a card, then crashes before acknowledging the message. What happens with at-least-once delivery?', ['The message is redelivered and the card may be charged again unless the worker is idempotent', 'The message is lost', 'The charge is automatically refunded', 'Nothing'], 0, 'Redelivery after a crash is exactly what at-least-once means.'),
      q('What does an idempotency key let a payment API do?', ['Recognise a repeated request and return the original result instead of charging again', 'Encrypt the payment', 'Speed up the charge', 'Skip authentication'], 0, 'The key identifies the logical operation across retries.'),
      q('Which pattern guarantees a database change and its event are both recorded?', ['Transactional outbox', 'Round robin', 'Cache-aside', 'Sharding'], 0, 'The event is written to an outbox table in the same transaction, then published.'),
    ],
    try: { kind: 'problem', id: 'notification_service', label: 'Design a notification service' },
  },
  {
    id: 'search', phase: 'phase-4', title: 'Search indexes', minutes: 6, tags: ['SearchIndex'],
    summary: 'Why LIKE queries do not scale and how search clusters stay fresh.',
    body: [
      { p: 'A database query like LIKE \'%term%\' cannot use a normal index, so it scans every row. A search engine builds an inverted index instead: for each term, the list of documents that contain it. That makes full-text, fuzzy, prefix and faceted queries fast, with relevance ranking built in.' },
      { p: 'The database stays the source of truth; the index is a derived copy. Changes flow to the index through a pipeline, often change data capture or a queue, so the index is eventually consistent, usually by seconds. Search clusters scale with shards (data size and indexing speed) and replica copies (query throughput and resilience).' },
      { ul: ['Never treat the search index as the system of record.', 'Add replica copies for read throughput and availability.', 'Expect a short delay before new data is searchable.'] },
    ],
    key_points: ['Inverted indexes make text search fast.', 'The index is derived and eventually consistent.', 'Shards for size, replicas for queries.'],
    quiz: [
      q('Why is LIKE \'%phone%\' slow on a large table?', ['It cannot use a normal index, so every row is scanned', 'LIKE locks the table', 'It only works on small tables', 'It forces a write'], 0, 'A leading wildcard defeats ordinary B-tree indexes.'),
      q('How do search indexes usually receive updates?', ['Through a pipeline (change data capture or a queue) from the source database', 'Users write to them directly', 'They query the database on every search', 'They are rebuilt only once a year'], 0, 'The index is a projection kept up to date asynchronously.'),
      q('Search queries are slow but the index is small. What helps most?', ['More replica copies', 'More shards', 'A longer refresh interval', 'Removing the database'], 0, 'Replicas add query capacity; shards mostly help with data size and indexing.'),
    ],
    try: { kind: 'problem', id: 'search_autocomplete', label: 'Build search autocomplete' },
  },

  // ---- Phase 5 ---------------------------------------------------------------
  {
    id: 'resilience-patterns', phase: 'phase-5', title: 'Timeouts, retries and circuit breakers', minutes: 9, tags: ['Service', 'ThirdPartyAPI', 'Server'],
    summary: 'Containing slow and failing dependencies so they do not take you down with them.',
    body: [
      { p: 'Every remote call needs a timeout. Without one, a slow dependency holds threads until the caller has none left, and slowness turns into an outage upstream. Set timeouts a little above the dependency\'s normal p99.' },
      { p: 'Retries fix transient blips but multiply load on a dependency that is already struggling. Use few retries, exponential backoff with jitter, and a retry budget. A circuit breaker watches failures and, when they pass a threshold, stops calling for a while and fails fast, giving the dependency room to recover. For optional features, return a fallback (cached data, a default, or nothing) instead of failing the whole request.' },
      { ul: ['Timeouts bound waiting; breakers stop doomed calls entirely.', 'Retries without backoff turn blips into storms.', 'Bulkheads (separate pools) stop one dependency from consuming all threads.'] },
    ],
    key_points: ['Always set timeouts.', 'Retry sparingly with backoff and jitter.', 'Break the circuit and degrade gracefully.'],
    quiz: [
      q('A dependency becomes 50× slower and callers have no timeout. What typically happens?', ['Caller threads pile up waiting until the caller stops serving any request', 'Callers get faster', 'Only that feature slows down', 'The dependency recovers automatically'], 0, 'Waiting requests hold resources; once they are exhausted, everything fails.'),
      q('What is the main risk of aggressive retries?', ['They multiply load on a failing dependency and make recovery harder', 'They reduce latency', 'They lose data', 'They disable caching'], 0, 'Three retries can quadruple traffic to a service that is already overloaded.'),
      q('What does an open circuit breaker do?', ['Fails calls immediately without contacting the dependency for a while', 'Retries calls faster', 'Routes calls to a cache automatically', 'Encrypts the calls'], 0, 'It sheds doomed work and periodically probes whether the dependency has recovered.'),
    ],
    try: { kind: 'incident', id: 'slow-recommendations', label: 'Contain a slow dependency' },
  },
  {
    id: 'observability', phase: 'phase-5', title: 'Observability and SLOs', minutes: 7, tags: ['general'],
    summary: 'Knowing what your system is doing and agreeing on what "good enough" means.',
    body: [
      { p: 'Metrics tell you that something is wrong, logs tell you what happened, and traces show where time went across services. For request-driven services, watch rate, errors and duration (RED). For resources, watch utilization, saturation and errors (USE).' },
      { p: 'A service level objective (SLO) states a target such as "99.9% of requests succeed in under 300 ms over 30 days". The gap between 100% and the SLO is the error budget: room for deploys and experiments. Alert on symptoms users feel (error rate, latency, queue age), not on every CPU blip.' },
      { ul: ['Alert on SLO burn rate rather than raw thresholds.', 'Track queue age and consumer lag for async systems.', 'Make dashboards answer "is it the user, us, or a dependency?"'] },
    ],
    key_points: ['Metrics, logs and traces answer different questions.', 'SLOs and error budgets define good enough.', 'Alert on user-facing symptoms.'],
    quiz: [
      q('A 99.9% monthly availability SLO allows roughly how much downtime per 30 days?', ['About 43 minutes', 'About 4 hours', 'About 4 minutes', 'About 7 hours'], 0, '0.1% of 43,200 minutes is about 43 minutes.'),
      q('Which signal should page an engineer at night?', ['Error rate burning the error budget quickly', 'One server at 75% CPU for a minute', 'A new deploy finished', 'Disk at 40%'], 0, 'Page on symptoms that threaten the SLO.'),
      q('Which tool shows where a slow request spent its time across services?', ['Distributed tracing', 'A CPU graph', 'A database backup', 'A load balancer log only'], 0, 'Traces follow one request through every hop.'),
    ],
    try: { kind: 'incident', id: 'order-emails-stopped', label: 'Catch a silent failure' },
  },
  {
    id: 'redundancy', phase: 'phase-5', title: 'Redundancy and failure domains', minutes: 7, tags: ['general', 'Database', 'LoadBalancer'],
    summary: 'Finding single points of failure and making sure losing one thing never loses everything.',
    body: [
      { p: 'A single point of failure is any component whose loss stops the system: one database, one load balancer, one cache node in front of a fragile database. Redundancy means having more than one, plus the mechanism that switches over: health checks, failover and retries.' },
      { p: 'Think in failure domains. Two instances in the same rack or availability zone can fail together. Spread replicas across zones, keep N+1 capacity so the survivors can carry the load, and test failure on purpose (chaos testing) so you discover gaps before your users do.' },
      { ul: ['Redundancy without automatic failover is a slow manual recovery.', 'Survivors need headroom, or one failure cascades.', 'Practise failures regularly.'] },
    ],
    key_points: ['Find and remove single points of failure.', 'Spread copies across failure domains.', 'Test failover before you need it.'],
    quiz: [
      q('Which design still has a single point of failure?', ['Two app servers behind one load balancer instance', 'Two load balancer instances in front of two app servers', 'A database with a replica and automatic failover', 'Two cache nodes with coalescing'], 0, 'The single load balancer instance takes everything down if it fails.'),
      q('Why spread replicas across availability zones?', ['So a single zone outage does not take out every copy', 'To make writes faster', 'To reduce cost', 'Because replicas cannot share a zone'], 0, 'Copies in the same failure domain can fail together.'),
      q('What is the purpose of chaos testing?', ['Deliberately inject failures to verify the system copes before a real incident', 'Randomise deploy times', 'Load test the CDN', 'Delete old data'], 0, 'You learn whether failover works when it is safe to find out.'),
    ],
    try: { kind: 'incident', id: 'single-database-outage', label: 'Remove a single point of failure' },
  },
  {
    id: 'security', phase: 'phase-5', title: 'Security fundamentals', minutes: 7, tags: ['APIGateway', 'general'],
    summary: 'Authentication, encryption, least privilege and recoverability: the baseline every design needs.',
    body: [
      { p: 'Authentication proves who a caller is; authorization decides what they may do. Centralise authentication at a gateway or shared library, and check authorization close to the data. Encrypt traffic in transit with TLS and data at rest in databases, object storage and backups.' },
      { p: 'Assume something will go wrong: keys leak, a bug deletes data, a client misbehaves. Rate limiting contains abuse, least-privilege credentials limit what a stolen key can do, secrets belong in a secret manager rather than in code, and point-in-time backups let you recover from destructive mistakes.' },
      { ul: ['Never expose data stores directly to clients.', 'Rotate credentials and log access to sensitive data.', 'Treat backups as a security control, not just an operations one.'] },
    ],
    key_points: ['Authenticate at the edge, authorize near the data.', 'Encrypt in transit and at rest.', 'Plan for leaks and mistakes: limits, least privilege, backups.'],
    quiz: [
      q('What is the difference between authentication and authorization?', ['Authentication proves identity; authorization decides permitted actions', 'They are the same thing', 'Authorization happens first', 'Authentication only applies to admins'], 0, 'Who you are, then what you may do.'),
      q('Why should clients never connect directly to the database?', ['They would bypass business rules and access checks', 'Databases cannot accept connections', 'It is slower than going through a CDN', 'TLS does not work with databases'], 0, 'The application tier enforces rules and permissions.'),
      q('Which control helps most after an accidental mass delete?', ['Point-in-time backups', 'TLS', 'Rate limiting', 'A CDN'], 0, 'Backups let you restore data as it was before the mistake.'),
    ],
    try: { kind: 'incident', id: 'bank-credential-stuffing', label: 'Defend a login endpoint' },
  },
];

export const TRACKS = [
  { id: 'design', title: 'System design', summary: 'From building blocks to production: the ideas behind every large system.' },
  { id: 'kafka', title: 'Apache Kafka', summary: 'Everything about Kafka: the log, partitions, producers and consumers, durability, delivery guarantees, the ecosystem and operations.' },
];
export const PHASES = [...DESIGN_PHASES.map((ph) => ({ track: 'design', ...ph })), ...KAFKA_PHASES];
export const TOPICS = [...DESIGN_TOPICS, ...KAFKA_TOPICS];

export function getTopic(id) {
  return TOPICS.find((t) => t.id === id) || null;
}

// ================================================================ content: kafka incidents
// Broken Kafka systems for Design & Fix. Goals are checked by the simulator; `solution` is the
// reference fix the content tests apply to prove each incident can be solved.

export const KAFKA_INCIDENTS = [
  {
    id: 'kafka-consumer-lag',
    track: 'kafka',
    title: 'Black Friday: order confirmations arrive an hour late',
    category: 'kafka', difficulty: 'easy',
    product: 'An online shop',
    headline: 'Orders go through, but confirmation emails and order-status updates arrive later and later. The dashboard says consumer lag is in the millions and growing.',
    symptoms: ['The order-events topic has 12 partitions.', 'The Confirmations group runs 4 consumers, each handling about 300 messages a second.', '3,000 order events arrive every second.'],
    traffic: { requests_per_second: 3000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Shoppers', { users: 3000, requests_per_user: 1 });
      node('Service', 'orders', 'Order Service', { replicas: 4 });
      node('KafkaTopic', 'events', 'order-events', { partitions: 12 });
      node('ConsumerGroup', 'confirm', 'Confirmations', { replicas: 4, capacity: 300 });
      node('Database', 'statusdb', 'Status DB', { read_ratio: 0, capacity: 6000, read_replicas: 1, failover: true });
      chain('users', 'orders', 'events', 'confirm', 'statusdb');
    }),
    goals: [
      { type: 'lag_below', value: 1, text: 'Consumer lag stops growing' },
      { type: 'freshness_below', value: 10000, text: 'Confirmations are less than 10 s behind' },
    ],
    constraints: { max_new_components: 0, config_max: [['confirm', 'capacity', 300, 'Each consumer\'s speed comes from its code; scale out instead of making it faster.']], note: 'Fix it with the components you have.' },
    root_cause: { question: 'Which component cannot keep up?', answer: 'confirm', explanation: 'Four consumers handle about 1,200 messages a second, but 3,000 arrive. The topic has 12 partitions, so up to 12 consumers could work in parallel.' },
    hints: ['Compare what the group can process with what arrives each second.', 'Twelve partitions allow up to twelve consumers in one group.'],
    debrief: {
      question: 'You scaled the group to 12 consumers. Next year traffic doubles. What do you need besides more consumers?',
      options: ['More partitions, because a group cannot use more consumers than the topic has partitions', 'A longer retention time', 'acks=0 on the producer', 'Nothing, consumers scale without limit'],
      answer: 0,
      explanation: 'Each partition is read by one consumer per group. Beyond 12 consumers, extra ones sit idle, so plan partitions for your future peak.',
    },
    lesson: 'Lag that keeps growing means the group is slower than the producers. Add consumers up to the partition count, then add partitions.',
    solution: (g) => setParam(g, 'confirm', 'replicas', 12),
  },
  {
    id: 'kafka-idle-consumers',
    track: 'kafka',
    title: 'Food delivery: scaling the consumers changed nothing',
    category: 'kafka', difficulty: 'easy',
    product: 'A food delivery app',
    headline: 'Courier position updates lag by minutes. The team tripled the consumers from 4 to 12 overnight, and nothing improved.',
    symptoms: ['The courier-updates topic has 3 partitions.', '9 of the 12 consumers report that they have no partitions assigned.', '2,000 updates arrive every second; each consumer handles about 300.'],
    traffic: { requests_per_second: 2000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Courier Apps', { users: 2000, requests_per_user: 1 });
      node('Service', 'api', 'Courier API', { replicas: 3 });
      node('KafkaTopic', 'updates', 'courier-updates', { partitions: 3, message_kb: 0.3 });
      node('ConsumerGroup', 'tracker', 'Tracking Updater', { replicas: 12, capacity: 300 });
      node('Cache', 'positions', 'Live Positions', { replicas: 2, hit_ratio: 1 });
      chain('users', 'api', 'updates', 'tracker', 'positions');
    }),
    goals: [
      { type: 'lag_below', value: 1, text: 'Consumer lag stops growing' },
      { type: 'freshness_below', value: 5000, text: 'Positions are less than 5 s old' },
    ],
    constraints: { max_new_components: 0, config_max: [['tracker', 'replicas', 12, 'Keep the Tracking Updater at 12 consumers or fewer: that is the budget.'], ['tracker', 'capacity', 300, 'Each consumer\'s speed comes from its code; it cannot get faster today.']], note: 'The budget allows 12 consumers. Fix it with what you have.' },
    root_cause: { question: 'Which component limits the consumers?', answer: 'updates', explanation: 'With 3 partitions, at most 3 consumers in a group can work. The other 9 sit idle, so the group processes about 900 updates a second.' },
    hints: ['Why would most consumers have nothing assigned?', 'Within a group, each partition goes to exactly one consumer.'],
    debrief: {
      question: 'Why is 12 a better partition count than 10 for 12 consumers that might lose one or two members?',
      options: ['12 divides evenly over 12, 6, 4 or 3 consumers, so work stays balanced as consumers come and go', '12 partitions use less disk', 'Kafka requires a multiple of 12', 'There is no difference'], answer: 0,
      explanation: 'Work is split by partition. Counts with many divisors keep the split even; with 10 partitions over 12 consumers, two consumers would idle and losing one would give someone double work.',
    },
    lesson: 'Partitions cap a consumer group\'s parallelism. Consumers beyond the partition count sit idle.',
    solution: (g) => setParam(g, 'updates', 'partitions', 12),
  },
  {
    id: 'kafka-hot-partition',
    track: 'kafka',
    title: 'SaaS audit log: one tenant drowns the pipeline',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A business software platform',
    headline: 'Audit search is minutes behind for every customer. One partition is far busier than the rest, and one consumer is pinned at 100% while the others idle.',
    symptoms: ['audit-events is keyed by tenant id.', 'The biggest customer produces 45% of all audit events.', 'Adding consumers or partitions made no difference.'],
    traffic: { requests_per_second: 4000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Tenants', { users: 4000, requests_per_user: 1 });
      node('Service', 'app', 'Platform API', { replicas: 5 });
      node('KafkaTopic', 'audit', 'audit-events', { partitions: 24, partition_key: 'skewed', hot_key_share: 0.45 });
      node('ConsumerGroup', 'indexer', 'Audit Indexer', { replicas: 24, capacity: 400 });
      node('SearchIndex', 'search', 'Audit Search', { shards: 4, replicas: 1 });
      chain('users', 'app', 'audit', 'indexer', 'search');
    }),
    goals: [
      { type: 'lag_below', value: 1, text: 'Consumer lag stops growing' },
      { type: 'freshness_below', value: 10000, text: 'Audit search is less than 10 s behind' },
    ],
    constraints: { max_new_components: 0, config_max: [['indexer', 'capacity', 400, 'Each indexer\'s speed comes from its code; it cannot get faster today.']], note: 'Fix it with the components you have.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'audit', explanation: 'Keyed by tenant, the biggest tenant\'s 45% of events always land on one partition, so one consumer must handle them alone.' },
    hints: ['One partition is busy, the rest are not. What decides which partition a message goes to?', 'Audit events only need ordering per audited object, not per tenant.'],
    debrief: {
      question: 'Re-keying by object id spreads the load. What do you give up compared with keying by tenant?',
      options: ['Order across a whole tenant: only events for the same object stay in order', 'Durability', 'The ability to have more than one consumer group', 'Compression'], answer: 0,
      explanation: 'Kafka orders events per partition. A finer key spreads load better but only keeps order for that key, which is usually all you need.',
    },
    lesson: 'A key with one huge value makes a hot partition that no amount of scaling fixes. Key by something with many evenly used values.',
    solution: (g) => setParam(g, 'audit', 'partition_key', 'id'),
  },
  {
    id: 'kafka-producer-throughput',
    track: 'kafka',
    title: 'Log pipeline: producers time out at peak',
    category: 'kafka', difficulty: 'intermediate',
    product: 'An observability team',
    headline: 'Every evening at peak, log shippers report produce timeouts and drop logs. The three brokers are busy all the time.',
    symptoms: ['Producers send each log line as soon as it arrives (linger.ms = 0).', 'Batches are not compressed.', '45,000 lines of about 1 KB arrive every second.'],
    traffic: { requests_per_second: 45000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Log Shippers', { users: 45000, requests_per_user: 1 });
      node('Service', 'collect', 'Collectors', { replicas: 4, capacity: 20000, base_latency: 5 });
      node('KafkaTopic', 'logs', 'logs', { partitions: 12, linger_ms: 0, compression: 'none' });
      node('KafkaConnect', 'sink', 'Search Sink', { mode: 'sink', connector: 'search', replicas: 12, capacity: 6000 });
      node('SearchIndex', 'search', 'Log Search', { shards: 4, replicas: 1 });
      chain('users', 'collect', 'logs', 'sink');
      link('sink', 'search', { ratio: 0.1 });
    }),
    goals: [
      { type: 'error_below', value: 0.01, text: 'Under 1% of log batches fail' },
      { type: 'node_load_below', node: 'logs', value: 0.85, text: 'The logs topic under 85% busy' },
    ],
    constraints: { max_new_components: 0, config_max: [['logs', 'brokers', 3, 'Keep the cluster at 3 brokers: there is no budget for more.'], ['logs', 'capacity', 5000, 'A partition\'s raw speed is set by the hardware.']], note: 'No new brokers this quarter.' },
    root_cause: { question: 'Which component is the bottleneck?', answer: 'logs', explanation: 'Each line goes out in its own tiny request, uncompressed, and is stored three times. The brokers spend their time on request overhead and raw bytes.' },
    hints: ['Look at the producer settings on the topic, not the number of brokers.', 'Waiting a few milliseconds fills batches; compression shrinks them.'],
    debrief: {
      question: 'You set linger.ms to 10. What does that cost?',
      options: ['Up to 10 ms more latency per message, in exchange for far more throughput', 'Durability: batched messages can be lost', 'Ordering within a partition', 'Nothing at all'], answer: 0,
      explanation: 'The producer waits up to linger.ms to fill a batch. For logs, a few milliseconds is invisible, and the throughput gain is large.',
    },
    lesson: 'Batching and compression are the cheapest throughput wins in Kafka: same brokers, several times the messages.',
    solution: (g) => { setParam(g, 'logs', 'linger_ms', 10); return setParam(g, 'logs', 'compression', 'lz4'); },
  },
  {
    id: 'kafka-lost-orders-broker',
    track: 'kafka',
    title: 'Marketplace: orders vanish after a broker crash',
    category: 'kafka', difficulty: 'easy',
    product: 'An online marketplace',
    headline: 'A broker crashed last night. For ten minutes a third of checkouts failed, and some orders that customers saw confirmed never reached the warehouse.',
    symptoms: ['The orders topic keeps one copy of each partition.', 'Producers use acks=1.', 'The cluster has three brokers.'],
    traffic: { requests_per_second: 1500 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Shoppers', { users: 1500, requests_per_user: 1 });
      node('Service', 'checkout', 'Checkout', { replicas: 3 });
      node('KafkaTopic', 'orders', 'orders', { replication_factor: 1, min_insync_replicas: 1, acks: '1' });
      node('ConsumerGroup', 'warehouse', 'Warehouse', { replicas: 3, capacity: 800, idempotent: true });
      node('Database', 'wms', 'Warehouse DB', { read_ratio: 0.2, read_replicas: 1, failover: true });
      chain('users', 'checkout', 'orders', 'warehouse', 'wms');
    }),
    goals: [
      { type: 'survives_failure', prefer: ['KafkaTopic'], max_error: 0.01, text: 'Checkouts keep working when a broker dies' },
      { type: 'guarantee', requirement: 'no-loss', prefer: ['KafkaTopic'], text: 'No confirmed order is lost when a broker dies' },
    ],
    constraints: { max_new_components: 0, note: 'Change the topic\'s settings.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'orders', explanation: 'With one copy per partition, the crashed broker\'s partitions were simply gone. With acks=1, writes were confirmed before any copy existed elsewhere.' },
    hints: ['How many copies of each partition exist?', 'When does a producer with acks=1 consider a write done?'],
    debrief: {
      question: 'Replication factor 3, min.insync.replicas 2, acks=all. Two of three brokers die at once. What happens?',
      options: ['Writes are refused until a broker returns, so nothing confirmed is lost', 'Writes continue on one copy', 'Kafka loses every partition', 'Consumers start reading duplicates'], answer: 0,
      explanation: 'With only one in-sync copy left, acks=all writes fail rather than being stored on a single broker. Kafka chooses durability over availability here.',
    },
    lesson: 'Replication factor 3, min.insync.replicas 2 and acks=all let any single broker fail without losing a confirmed write.',
    solution: (g) => { setParam(g, 'orders', 'replication_factor', 3); setParam(g, 'orders', 'min_insync_replicas', 2); return setParam(g, 'orders', 'acks', 'all'); },
  },
  {
    id: 'kafka-isr-write-outage',
    track: 'kafka',
    title: 'Payments: a routine broker restart stops every payment',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A payment processor',
    headline: 'During a planned rolling restart, every payment failed with NOT_ENOUGH_REPLICAS until the broker came back.',
    symptoms: ['The payments topic has 3 copies of every partition.', 'min.insync.replicas is set to 3.', 'Producers use acks=all.'],
    traffic: { requests_per_second: 2000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Merchants', { users: 2000, requests_per_user: 1 });
      node('Service', 'api', 'Payments API', { replicas: 3 });
      node('KafkaTopic', 'payments', 'payments', { min_insync_replicas: 3 });
      node('StreamProcessor', 'ledger', 'Ledger', { replicas: 3, processing_guarantee: 'exactly_once' });
      node('Database', 'db', 'Ledger DB', { read_ratio: 0.1, capacity: 4000, read_replicas: 1, failover: true });
      chain('users', 'api', 'payments', 'ledger', 'db');
    }),
    goals: [
      { type: 'survives_failure', prefer: ['KafkaTopic'], max_error: 0.01, text: 'Payments keep working while a broker restarts' },
      { type: 'guarantee', requirement: 'no-loss', prefer: ['KafkaTopic'], text: 'No confirmed payment can be lost' },
    ],
    constraints: { max_new_components: 0, note: 'Change the topic\'s settings.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'payments', explanation: 'acks=all with min.insync.replicas equal to the replication factor needs every copy online, so losing any one broker stops all writes.' },
    hints: ['How many in-sync copies does a write need, and how many are left when one broker is down?', 'There is a setting that is just as safe in practice and tolerates one broker being down.'],
    debrief: {
      question: 'Someone suggests acks=1 instead, which also keeps payments flowing during restarts. Why is that worse?',
      options: ['acks=1 confirms writes on the leader alone, so a leader crash can lose confirmed payments', 'acks=1 is slower', 'acks=1 breaks ordering', 'acks=1 needs more brokers'], answer: 0,
      explanation: 'min.insync.replicas 2 with acks=all keeps every confirmed write on two brokers while tolerating one broker down. acks=1 gives up durability entirely.',
    },
    lesson: 'Set min.insync.replicas to replication factor − 1: durable, and still writable with one broker down.',
    solution: (g) => setParam(g, 'payments', 'min_insync_replicas', 2),
  },
  {
    id: 'kafka-rebalance-storm',
    track: 'kafka',
    title: 'Shipping labels: the consumer group never makes progress',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A logistics company',
    headline: 'The label printer group is busy all day but the lag only grows. Logs show the group rebalancing every few minutes, and some labels print twice.',
    symptoms: ['Each message calls a carrier API that takes about 800 ms.', 'The consumers fetch 500 messages per poll.', 'max.poll.interval.ms is 5 minutes.'],
    traffic: { requests_per_second: 300 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Warehouses', { users: 300, requests_per_user: 1 });
      node('Service', 'api', 'Shipping API', { replicas: 2 });
      node('KafkaTopic', 'shipments', 'shipments', { partitions: 6 });
      node('ConsumerGroup', 'labels', 'Label Printer', { replicas: 6, capacity: 200 });
      node('ThirdPartyAPI', 'carrier', 'Carrier API', { provider: 'other', base_latency: 800, capacity: 2000 });
      chain('users', 'api', 'shipments', 'labels', 'carrier');
    }),
    goals: [
      { type: 'lag_below', value: 1, text: 'Consumer lag stops growing' },
      { type: 'freshness_below', value: 30000, text: 'Labels are less than 30 s behind' },
    ],
    constraints: { max_new_components: 0, config_max: [['labels', 'capacity', 200, 'Each consumer\'s speed comes from its code; it cannot get faster today.']], note: 'Change settings only.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'labels', explanation: '500 messages × 0.8 s is 400 s per poll, longer than max.poll.interval.ms (300 s). Kafka assumes the consumer died, rebalances and replays the batch, over and over.' },
    hints: ['Multiply the messages per poll by the time per message, and compare it with max.poll.interval.ms.', 'You can fetch fewer messages per poll, or allow more time between polls.'],
    debrief: {
      question: 'Why did some labels print twice during the storm?',
      options: ['A consumer evicted mid-batch had printed labels it never committed, so the batch was replayed', 'The producer sent duplicates', 'The carrier API retried on its own', 'Compaction duplicated messages'], answer: 0,
      explanation: 'At-least-once consumers commit after processing. An eviction before the commit means the next owner processes the same messages again, so printing should be idempotent too.',
    },
    lesson: 'Keep max.poll.records × time per message well under max.poll.interval.ms, or a slow dependency turns into an endless rebalance storm.',
    solution: (g) => setParam(g, 'labels', 'max_poll_records', 100),
  },
  {
    id: 'kafka-poison-pill',
    track: 'kafka',
    title: 'Fitness app: one bad workout upload froze the feed',
    category: 'kafka', difficulty: 'easy',
    product: 'A fitness app',
    headline: 'A third of users stopped seeing new workouts in their feed. The feed builder logs the same parsing error thousands of times a minute.',
    symptoms: ['One uploaded workout has a corrupted payload.', 'The consumer retries a failing message forever.', 'The stuck consumer owns two of the six partitions.'],
    traffic: { requests_per_second: 1200 },
    kafka: { poison_pill: true },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Athletes', { users: 1200, requests_per_user: 1 });
      node('Service', 'api', 'Workout API', { replicas: 2 });
      node('KafkaTopic', 'workouts', 'workouts', { partitions: 6 });
      node('ConsumerGroup', 'feed', 'Feed Builder', { replicas: 3, capacity: 800 });
      node('Cache', 'feeds', 'Feed Store', { replicas: 2, hit_ratio: 1 });
      chain('users', 'api', 'workouts', 'feed', 'feeds');
    }),
    goals: [
      { type: 'no_stuck', text: 'No partition is stuck' },
      { type: 'freshness_below', value: 10000, text: 'Feeds are less than 10 s behind' },
    ],
    constraints: { max_new_components: 0, note: 'Change settings only.' },
    root_cause: { question: 'Which component is stuck?', answer: 'feed', explanation: 'The consumer cannot process the corrupted message and retries it forever, so its partitions, and every message behind the bad one, stop moving.' },
    hints: ['Kafka never skips a message for you. What should the consumer do with one it cannot handle?', 'Look for a setting that moves failing messages aside.'],
    debrief: {
      question: 'Bad messages now go to a dead letter topic. What else should you do?',
      options: ['Alert on the dead letter topic, fix the cause, and replay the parked messages', 'Delete the dead letter topic every night', 'Turn off retries everywhere', 'Nothing, the messages are handled'], answer: 0,
      explanation: 'A dead letter topic keeps the pipeline moving, but the parked messages are real data. Someone has to look at them and replay them once fixed.',
    },
    lesson: 'Every consumer needs a dead letter path, or one bad message stops a partition.',
    solution: (g) => setParam(g, 'feed', 'dead_letter_topic', true),
  },
  {
    id: 'kafka-double-charges',
    track: 'kafka',
    title: 'Subscriptions: customers charged twice after a deploy',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A video streaming service',
    headline: 'After every deploy, a few hundred customers are billed twice for the same month. Support is overwhelmed.',
    symptoms: ['The Billing group commits offsets after processing.', 'Deploys restart consumers one at a time.', 'Charging the same invoice twice creates two charges.'],
    traffic: { requests_per_second: 800 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Renewals', { users: 800, requests_per_user: 1 });
      node('Service', 'subs', 'Subscription Service', { replicas: 2 });
      node('KafkaTopic', 'invoices', 'invoices', { partitions: 6 });
      node('ConsumerGroup', 'billing', 'Billing', { replicas: 3, capacity: 500 });
      node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 1500 });
      chain('users', 'subs', 'invoices', 'billing', 'pay');
    }),
    goals: [
      { type: 'guarantee', requirement: 'no-duplicates', prefer: ['ConsumerGroup'], text: 'A consumer restart never charges anyone twice' },
      { type: 'guarantee', requirement: 'no-loss', prefer: ['ConsumerGroup'], text: 'A consumer restart never skips an invoice' },
    ],
    constraints: { max_new_components: 0, note: 'Change settings only.' },
    root_cause: { question: 'Which component causes the double charges?', answer: 'billing', explanation: 'At-least-once consumers process messages again after a restart (everything since the last commit), and charging is not idempotent.' },
    hints: ['What happens to messages processed after the last commit when a consumer restarts?', 'Committing earlier skips messages instead. What makes a repeat harmless?'],
    debrief: {
      question: 'Why not commit offsets before charging (at-most-once) instead?',
      options: ['A crash after the commit but before the charge would skip that invoice: lost revenue and confused customers', 'It would be slower', 'Kafka does not allow it', 'It makes charges twice as likely'], answer: 0,
      explanation: 'At-most-once trades duplicates for losses. Keep at-least-once and make the side effect idempotent, for example with the invoice id as the payment idempotency key.',
    },
    lesson: 'At-least-once plus idempotent processing gives exactly-once effects, even for side effects outside Kafka.',
    solution: (g) => setParam(g, 'billing', 'idempotent', true),
  },
  {
    id: 'kafka-retention-loss',
    track: 'kafka',
    title: 'Analytics: Monday\'s reports are missing Saturday',
    category: 'kafka', difficulty: 'easy',
    product: 'A retail analytics platform',
    headline: 'The warehouse loader crashed on Friday night and nobody noticed until Monday. When it restarted, a whole day of sales events was gone.',
    symptoms: ['The sales-events topic keeps messages for 24 hours.', 'The loader was down for about 60 hours.', 'The producers never had an error.'],
    traffic: { requests_per_second: 1000 },
    kafka: { horizon_hours: 60 },
    overrides: { loader: { down: true } },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Stores', { users: 1000, requests_per_user: 1 });
      node('Service', 'pos', 'Sales API', { replicas: 2 });
      node('KafkaTopic', 'sales', 'sales-events', { retention_hours: 24 });
      node('KafkaConnect', 'loader', 'Warehouse Loader', { mode: 'sink', connector: 'jdbc' });
      node('Database', 'dw', 'Data Warehouse', { read_ratio: 0.3, capacity: 5000, read_replicas: 1, failover: true });
      chain('users', 'pos', 'sales', 'loader', 'dw');
    }),
    goals: [
      { type: 'no_data_loss', text: 'No sales event expires before the loader reads it' },
      { type: 'error_below', value: 0.01, text: 'Stores can still record sales' },
    ],
    constraints: { max_new_components: 0, note: 'The loader stays down for the whole weekend; change how long Kafka keeps the data.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'sales', explanation: 'Kafka deletes messages older than the retention time whether or not they were read. A 60-hour outage with 24 hours of retention loses 36 hours of events.' },
    hints: ['Kafka kept accepting sales the whole weekend. Where did they go?', 'How long does the topic keep messages, and how long was the loader down?'],
    debrief: {
      question: 'Longer retention prevented the loss. What else should have caught this on Friday?',
      options: ['An alert on consumer lag in time ("the loader is 1 hour behind")', 'A bigger data warehouse', 'acks=all', 'More partitions'], answer: 0,
      explanation: 'Retention buys time; lag alerts make sure someone uses it. Alert when a group falls more than a set time behind.',
    },
    lesson: 'Size retention for your longest consumer outage, and alert on lag long before it reaches the retention time.',
    solution: (g) => setParam(g, 'sales', 'retention_hours', 168),
  },
  {
    id: 'kafka-schema-break',
    track: 'kafka',
    title: 'Ride app: a producer deploy broke every consumer',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A ride-hailing app',
    headline: 'The trips team renamed a field and deployed. Within a minute, pricing and fraud detection stopped processing trips.',
    symptoms: ['Trip events are JSON with no schema checks.', 'Both consumer groups throw parsing errors on every new message.', 'The trips team did not know who reads their events.'],
    traffic: { requests_per_second: 2000 },
    kafka: { schema_break: true },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Riders', { users: 2000, requests_per_user: 1 });
      node('Service', 'trips', 'Trips Service', { replicas: 3 });
      node('KafkaTopic', 'events', 'trip-events', { partitions: 12 });
      node('ConsumerGroup', 'pricing', 'Pricing', { replicas: 6, capacity: 600, idempotent: true });
      node('ConsumerGroup', 'fraud', 'Fraud Detection', { replicas: 6, capacity: 600, idempotent: true });
      node('Database', 'db', 'Trips Analytics DB', { read_ratio: 0.2, capacity: 6000, read_replicas: 1, failover: true });
      chain('users', 'trips', 'events', 'pricing', 'db');
      link('events', 'fraud');
      link('fraud', 'db');
    }),
    goals: [
      { type: 'no_stuck', text: 'Consumers keep processing trips' },
      { type: 'freshness_below', value: 10000, text: 'Pricing and fraud are less than 10 s behind' },
    ],
    constraints: { max_new_components: 1, allowed_new: ['SchemaRegistry'], note: 'You may add one Schema Registry.' },
    root_cause: { question: 'Which component let the breaking change through?', answer: 'trips', explanation: 'Nothing checked the new message format before the Trips Service started writing it, so every consumer met messages it could not read.' },
    hints: ['Who could have rejected the renamed field before any consumer saw it?', 'A registry with a compatibility rule checks each new schema version.'],
    debrief: {
      question: 'With backward compatibility, which change would the registry accept?',
      options: ['Adding a new optional field with a default value', 'Renaming a field', 'Changing a field from number to text', 'Removing a field with no default'], answer: 0,
      explanation: 'Readers can fill in a default for an added optional field. Renames and type changes break readers, so the registry rejects them.',
    },
    lesson: 'Event formats are contracts. A Schema Registry with a compatibility rule stops breaking changes before they ship.',
    solution: (g) => {
      addNode(g, fresh('SchemaRegistry', 'schemas', 'Schema Registry'));
      addEdge(g, 'trips', 'schemas');
      return addEdge(g, 'pricing', 'schemas');
    },
  },
  {
    id: 'kafka-dual-write',
    track: 'kafka',
    title: 'Hotel bookings: confirmed stays missing downstream',
    category: 'kafka', difficulty: 'intermediate',
    product: 'A hotel booking site',
    headline: 'Some confirmed bookings never reached the hotel\'s system, and a few cancelled bookings were sent anyway. The booking database and the event stream disagree.',
    symptoms: ['The Booking Service saves to its database, then publishes to Kafka in a separate step.', 'Bookings go missing after service crashes and deploys.', 'Rolled-back database transactions sometimes still publish an event.'],
    traffic: { requests_per_second: 1000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Guests', { users: 1000, requests_per_user: 1 });
      node('Service', 'booking', 'Booking Service', { replicas: 3 });
      node('Database', 'db', 'Bookings DB', { read_ratio: 0.6, read_replicas: 1, failover: true });
      node('KafkaTopic', 'events', 'booking-events', {});
      node('ConsumerGroup', 'hotels', 'Hotel Sync', { replicas: 3, idempotent: true });
      node('ThirdPartyAPI', 'pms', 'Hotel Systems', { provider: 'other', capacity: 2000 });
      chain('users', 'booking', 'db');
      link('booking', 'events', { ratio: 0.4 });
      chain('events', 'hotels', 'pms');
    }),
    goals: [
      { type: 'guarantee', requirement: 'consistent', text: 'The database and the events can never disagree' },
      { type: 'node_reached', node: 'events', text: 'Booking events still reach Kafka' },
    ],
    constraints: { max_new_components: 1, allowed_new: ['KafkaConnect'], note: 'You may add one Kafka Connect connector.' },
    root_cause: { question: 'Which component causes the disagreement?', answer: 'booking', explanation: 'It writes the database and publishes to Kafka as two separate steps. A crash or failure between them leaves one done and the other not.' },
    hints: ['What happens if the service crashes after the database commit but before publishing?', 'Could the events come from the database itself, after the commit?'],
    debrief: {
      question: 'With the outbox pattern, where does the Booking Service write the event?',
      options: ['To an outbox table, in the same database transaction as the booking', 'Directly to Kafka, before the database', 'To a log file', 'Nowhere; Kafka Connect invents it'], answer: 0,
      explanation: 'The event commits or rolls back with the booking. A CDC connector then streams the outbox table to Kafka, so both always agree.',
    },
    lesson: 'Never dual-write. Save the event with the data (outbox) and let change data capture publish it.',
    solution: (g) => {
      removeEdge(g, 'booking', 'events');
      addNode(g, fresh('KafkaConnect', 'cdc', 'Outbox CDC', { advanced: { mode: 'source', connector: 'cdc' } }));
      addEdge(g, 'db', 'cdc', { kind: 'async' });
      return addEdge(g, 'cdc', 'events', { kind: 'async' });
    },
  },
];

// ================================================================ content: incidents
// Goals are machine-checked against the simulator. `solution` is the reference fix used by
// the content tests to prove every incident is broken at baseline and fixable.

const DESIGN_INCIDENTS = [
  {
    id: 'weather-cache-ttl',
    title: 'Weather app: the cache forgets everything in five seconds',
    category: 'caching', difficulty: 'easy',
    product: 'A weather app',
    headline: 'A storm is coming and everyone is checking the forecast. The forecast cache was added last month, yet the database is drowning and a third of requests fail.',
    symptoms: ['The cache reports a hit ratio of about 15%.', 'Entries are set to expire after 5 seconds.', 'Most traffic is for the same few hundred cities.'],
    traffic: { requests_per_second: 3000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'App Users', { users: 3000, requests_per_user: 1 });
      node('Server', 'api', 'Forecast API', { capacity: 4000 });
      node('Cache', 'cache', 'Forecast Cache', { hit_ratio: 0.9, ttl_seconds: 5 });
      node('Database', 'db', 'Forecast DB', { capacity: 800, read_ratio: 0.95, read_replicas: 1, failover: true });
      chain('users', 'api', 'cache', 'db');
    }),
    goals: [
      { type: 'node_load_below', node: 'db', value: 1, text: 'Forecast DB under 100% load' },
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
    ],
    constraints: { max_new_components: 0, note: 'Fix it with the components you already have.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'cache', explanation: 'The cache is in the right place, but a 5-second TTL expires each city\'s forecast before most repeat requests arrive, so almost everything misses.' },
    hints: ['Look at the cache\'s settings, not its size.', 'Forecasts change every few minutes. How long could a cached forecast safely live?'],
    debrief: {
      question: 'You raised the TTL to 5 minutes. A new forecast is published one minute after a city was cached. What do users see?',
      options: ['The old forecast for up to 4 more minutes, unless publishing also invalidates the cache entry', 'The new forecast immediately, always', 'An error until the entry expires', 'A mix of both forecasts'],
      answer: 0,
      explanation: 'A TTL bounds staleness; it does not remove it. Pair a longer TTL with invalidation when the source data changes if users must see updates immediately.',
    },
    lesson: 'A cache is only as good as its effective hit ratio. TTLs much shorter than the reuse interval quietly turn a cache into an extra hop.',
    solution: (g) => setParam(g, 'cache', 'ttl_seconds', 300),
  },
  {
    id: 'bookstore-read-storm',
    title: 'Bookstore: a newsletter link floods product pages',
    category: 'caching', difficulty: 'easy',
    product: 'An online bookstore',
    headline: 'A newsletter featuring one bestseller went out to two million subscribers. Product pages time out and the database sits at 200% load.',
    symptoms: ['Every page view runs the same product query.', 'App servers are mostly idle, waiting on the database.', 'Ninety-five percent of queries are reads.'],
    traffic: { requests_per_second: 4000 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Shoppers', { users: 4000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'app', 'Store App', { replicas: 6 });
      node('Database', 'db', 'Catalog DB', { read_ratio: 0.95 });
      chain('users', 'lb', 'app', 'db');
    }),
    goals: [
      { type: 'node_load_below', node: 'db', value: 1, text: 'Catalog DB under 100% load' },
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
    ],
    constraints: { max_new_components: 1, allowed_new: ['Cache'], note: 'You may add one cache. Configuration changes are allowed.' },
    root_cause: { question: 'Which component is the bottleneck?', answer: 'db', explanation: 'Every page view becomes a database read, and the same few rows are read thousands of times a second.' },
    hints: ['The same product is being read over and over. Does each read need to reach the database?', 'A cache next to the app servers, checked before the database, absorbs repeated reads.'],
    debrief: {
      question: 'Why does a cache usually help more than a bigger database for this kind of traffic?',
      options: ['Hot reads are answered from memory in about a millisecond, and the database only sees misses and writes', 'Caches are always more durable than databases', 'Databases cannot handle reads', 'A cache makes writes faster'],
      answer: 0,
      explanation: 'Read-heavy, skewed traffic is exactly what caches are for. A bigger database still pays full cost for every repeated read.',
    },
    lesson: 'When a few keys dominate reads, a cache turns thousands of identical queries into one.',
    solution: (g) => {
      addNode(g, fresh('Cache', 'cache', 'Product Cache', { advanced: { hit_ratio: 0.85 } }));
      return addEdge(g, 'app', 'cache');
    },
  },
  {
    id: 'signup-launch-day',
    title: 'Launch day: the sign-up page times out',
    category: 'scalability', difficulty: 'easy',
    product: 'A new productivity app',
    headline: 'The launch went viral. Sign-ups are pouring in and most of them time out.',
    symptoms: ['One app server is pinned at 100% CPU.', 'The load balancer has only one backend.', 'The database is barely busy.'],
    traffic: { requests_per_second: 2500 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Visitors', { users: 2500, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'app', 'Sign-up App');
      node('Database', 'db', 'Accounts DB', { capacity: 3000, read_ratio: 0.7, read_replicas: 1, failover: true });
      chain('users', 'lb', 'app', 'db');
    }),
    goals: [
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
      { type: 'latency_below', value: 300, text: 'p95 latency under 300 ms' },
    ],
    constraints: { max_new_components: 0, note: 'Scale what you have.' },
    root_cause: { question: 'Which component is overloaded?', answer: 'app', explanation: 'A single app server can serve about 1,000 requests per second; launch traffic is two and a half times that.' },
    hints: ['The load balancer is ready to spread traffic, but it only has one backend.', 'Add instances or let the app autoscale.'],
    debrief: {
      question: 'You enabled autoscaling (2–10 instances). Traffic jumps 5× in ten seconds. What happens in the first minute?',
      options: ['New instances take time to boot, so the existing ones run hot or shed load until they are ready', 'Capacity appears instantly', 'The database scales with it', 'Requests queue at the CDN'],
      answer: 0,
      explanation: 'Autoscaling reacts in minutes, not seconds. Keep a minimum instance count that covers sudden spikes, and scale on leading signals.',
    },
    lesson: 'Stateless app servers scale horizontally; the load balancer just needs more backends.',
    solution: (g) => setParam(g, 'app', 'replicas', 4),
  },
  {
    id: 'bank-credential-stuffing',
    title: 'Bank login: a botnet floods sign-in',
    category: 'security', difficulty: 'easy',
    product: 'A retail bank',
    headline: 'A botnet is trying leaked passwords against the login endpoint. Real customers cannot sign in.',
    symptoms: ['Seventy percent of login attempts come from a few thousand IPs.', 'Each attempt costs a slow password hash.', 'Customers see timeouts and errors.'],
    traffic: { requests_per_second: 6000, abusive_share: 0.7 },
    graph: design(({ node, chain }) => {
      node('User', 'users', 'Customers and Bots', { users: 6000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
      node('Server', 'auth', 'Auth Service', { replicas: 3, base_latency: 80 });
      node('Database', 'db', 'Users DB', { capacity: 4000, read_ratio: 0.9, read_replicas: 1, failover: true });
      chain('users', 'lb', 'auth', 'db');
    }),
    goals: [
      { type: 'error_below', value: 0.01, text: 'Real customers see under 1% errors' },
      { type: 'security_check', check: 'SEC_RATE_LIMITING', text: 'Rate limiting is in place' },
    ],
    constraints: { max_new_components: 1, allowed_new: ['RateLimiter', 'APIGateway'], note: 'You may add one rate limiter or gateway.' },
    root_cause: { question: 'Which component is being overwhelmed?', answer: 'auth', explanation: 'Every bot attempt reaches the auth service and burns an expensive password hash, leaving no capacity for customers.' },
    hints: ['Bots send many attempts per client; customers send a few.', 'Reject excess attempts per client before they reach the auth service.'],
    debrief: {
      question: 'Why limit per client instead of capping total login attempts?',
      options: ['A global cap rejects real customers just as often as bots; per-client limits only hit the noisy clients', 'Global limits are harder to implement', 'Per-client limits make hashing faster', 'There is no difference'],
      answer: 0,
      explanation: 'Abuse is concentrated in a few clients. Limits keyed by client, IP or account target them and leave everyone else untouched.',
    },
    lesson: 'Rate limiting at the edge is the cheapest defence: rejected requests never cost you a password hash.',
    solution: (g) => {
      addNode(g, fresh('RateLimiter', 'rl', 'Login Limiter', { advanced: { limit_per_client_rps: 2 } }));
      removeEdge(g, 'users', 'lb');
      addEdge(g, 'users', 'rl');
      return addEdge(g, 'rl', 'lb');
    },
  },
  {
    id: 'order-emails-stopped',
    title: 'Order emails stopped arriving',
    category: 'async', difficulty: 'easy',
    product: 'A home goods shop',
    headline: 'Customers are complaining that order confirmations never arrive, and now some checkouts fail too.',
    symptoms: ['The email queue grows by about 150 messages per second.', 'The email worker is running but idle.', 'A recent deploy renamed the queue subscription.'],
    traffic: { requests_per_second: 1500 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Shoppers', { users: 1500, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'app', 'Checkout App', { replicas: 3 });
      node('Database', 'db', 'Orders DB', { capacity: 3000, read_ratio: 0.6, read_replicas: 1, failover: true });
      node('Queue', 'q', 'Email Queue', { max_depth: 50000 });
      node('Worker', 'mailer', 'Email Worker', { idempotent: true });
      node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 1000, circuit_breaker: true });
      chain('users', 'lb', 'app', 'db');
      link('app', 'q', { ratio: 0.1 });
      chain('mailer', 'email');
    }),
    goals: [
      { type: 'backlog_below', value: 1, text: 'Email backlog stops growing' },
      { type: 'error_below', value: 0.01, text: 'Checkout errors under 1%' },
    ],
    constraints: { max_new_components: 0, note: 'Everything you need is already on the canvas.' },
    root_cause: { question: 'Which component is broken?', answer: 'q', explanation: 'Nothing consumes the email queue any more, so messages pile up until the queue is full and checkouts start failing to enqueue.' },
    hints: ['Follow the path of an email message. Where does it stop?', 'Connect the queue to the worker that should consume it.'],
    debrief: {
      question: 'The queue was filling for 20 minutes before anyone noticed. Which alert would have caught it earliest?',
      options: ['Age of the oldest message in the queue', 'CPU of the checkout servers', 'Database disk usage', 'Number of load balancer instances'],
      answer: 0,
      explanation: 'Queue depth and especially the age of the oldest message reveal a stalled consumer within minutes, long before the queue is full.',
    },
    lesson: 'A queue decouples producers from consumers, which also means a dead consumer is silent until you watch the backlog.',
    solution: (g) => addEdge(g, 'q', 'mailer'),
  },
  {
    id: 'checkout-email-hang',
    title: 'Checkout hangs while the receipt email sends',
    category: 'async', difficulty: 'easy',
    product: 'A ticketing marketplace',
    headline: 'The email provider is having a slow day. Checkout now takes seconds and conversion is dropping.',
    symptoms: ['The email provider takes 1.5 seconds per call.', 'Checkout waits for the email before confirming the order.', 'Payments and the database are healthy.'],
    traffic: { requests_per_second: 1000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Buyers', { users: 1000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'checkout', 'Checkout Service', { replicas: 4 });
      node('Database', 'db', 'Orders DB', { capacity: 3000, read_ratio: 0.5, read_replicas: 1, failover: true });
      node('ThirdPartyAPI', 'pay', 'Payment Provider', { provider: 'payment', capacity: 2000, base_latency: 150 });
      node('ThirdPartyAPI', 'email', 'Email Provider', { provider: 'email', capacity: 2000, base_latency: 1500 });
      chain('users', 'lb', 'checkout', 'db');
      link('checkout', 'pay');
      link('checkout', 'email');
    }),
    goals: [
      { type: 'latency_below', value: 600, text: 'Checkout p95 under 600 ms' },
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
      { type: 'node_reached', node: 'email', text: 'Receipts are still sent' },
    ],
    constraints: { max_new_components: 2, allowed_new: ['Queue', 'Worker'], note: 'You may add a queue and a worker.' },
    root_cause: { question: 'Which call makes checkout slow?', answer: 'email', explanation: 'The receipt email is sent synchronously, so every checkout waits 1.5 seconds for a provider that has nothing to do with confirming the order.' },
    hints: ['Does the buyer need the email to be sent before seeing "order confirmed"?', 'Publish an event to a queue and let a worker send the email.'],
    debrief: {
      question: 'After your change the email provider is down for an hour. What happens to receipts?',
      options: ['They wait in the queue (or dead-letter queue) and are sent when the provider recovers', 'They are lost', 'Checkout fails again', 'They are sent twice'],
      answer: 0,
      explanation: 'The queue absorbs the outage. With retries and a dead-letter queue, receipts go out late instead of not at all.',
    },
    lesson: 'Anything the user does not need to see before the response belongs off the request path.',
    solution: (g) => {
      removeEdge(g, 'checkout', 'email');
      addNode(g, fresh('Queue', 'q', 'Receipt Queue'));
      addNode(g, fresh('Worker', 'mailer', 'Receipt Worker', { advanced: { idempotent: true, replicas: 8 } }));
      addEdge(g, 'checkout', 'q', { kind: 'async' });
      addEdge(g, 'q', 'mailer');
      return addEdge(g, 'mailer', 'email');
    },
  },
  {
    id: 'single-database-outage',
    title: 'The database died and took the app with it',
    category: 'reliability', difficulty: 'easy',
    product: 'An internal admin console',
    headline: 'Last night the only database server lost its disk. The app was down for six hours while someone restored a week-old dump.',
    symptoms: ['There is exactly one database server.', 'No replica, no automatic failover.', 'The last usable backup was a manual dump.'],
    traffic: { requests_per_second: 2000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Staff', { users: 2000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'app', 'Console App', { replicas: 4 });
      node('Cache', 'cache', 'Session Cache', { hit_ratio: 0.8, replicas: 2, request_coalescing: true });
      node('Database', 'db', 'Console DB', { read_ratio: 0.8 });
      chain('users', 'lb', 'app', 'cache');
      link('app', 'db');
    }),
    goals: [
      { type: 'survives_failure', prefer: ['Database'], max_error: 0.05, text: 'Under 5% errors when the database primary fails' },
      { type: 'config_equals', node: 'db', key: 'backups', value: true, text: 'Point-in-time backups are enabled' },
    ],
    constraints: { max_new_components: 0, note: 'Configure the database you have.' },
    root_cause: { question: 'Which component is the single point of failure?', answer: 'db', explanation: 'Everything depends on one database copy. When it fails there is nothing to promote and nothing recent to restore.' },
    hints: ['Look at the database\'s replica and failover settings.', 'Replicas cover a dead server; backups cover a bad change that replicas copy faithfully.'],
    debrief: {
      question: 'Replicas copy every change within seconds. Why do you still need backups?',
      options: ['A bad DELETE or corruption is replicated too; backups let you restore to before it happened', 'Replicas are slower than backups', 'Backups make failover faster', 'You do not, replicas are enough'],
      answer: 0,
      explanation: 'Replication protects against losing a machine, not against losing data through a mistake. Point-in-time backups cover the second case.',
    },
    lesson: 'Redundancy and recoverability are different properties: replicas give you the first, backups give you the second.',
    solution: (g) => {
      setParam(g, 'db', 'read_replicas', 1);
      setParam(g, 'db', 'failover', true);
      return setParam(g, 'db', 'backups', true);
    },
  },
  {
    id: 'cache-restart-stampede',
    title: 'A cache restart knocked over the database',
    category: 'caching', difficulty: 'intermediate',
    product: 'A social Q&A site',
    headline: 'A routine cache upgrade restarted the only cache node. Within seconds the database was at 190% and the site went down for twenty minutes.',
    symptoms: ['The cache normally answers 95% of reads.', 'There is a single cache node.', 'After the restart, thousands of requests missed the same keys at once.'],
    traffic: { requests_per_second: 5000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Readers', { users: 5000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'app', 'Web App', { autoscaling: true, autoscale_min: 4, autoscale_max: 20 });
      node('Cache', 'cache', 'Page Cache', { hit_ratio: 0.95 });
      node('Database', 'db', 'Posts DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
      chain('users', 'lb', 'app', 'cache');
      link('app', 'db');
    }),
    goals: [
      { type: 'survives_failure', prefer: ['Cache'], max_error: 0.02, text: 'Under 2% errors when a cache node fails' },
      { type: 'failure_node_load_below', prefer: ['Cache'], node: 'db', value: 1, text: 'Posts DB stays under 100% during the cache failure' },
    ],
    constraints: { max_new_components: 0, note: 'Fix it with configuration.' },
    root_cause: { question: 'Which component\'s configuration turned a restart into an outage?', answer: 'cache', explanation: 'A single cache node without request coalescing means one restart sends every read, duplicated many times over, straight to the database.' },
    hints: ['What if only part of the cache disappeared when a node restarts?', 'Request coalescing lets one request refill a hot key while the others wait.'],
    debrief: {
      question: 'What does request coalescing do when 500 requests miss the same key at once?',
      options: ['One request reads from the database and the other 499 wait for its result', 'All 500 read from the database in parallel', 'The 500 requests fail fast', 'The key is deleted'],
      answer: 0,
      explanation: 'Coalescing (also called single-flight) collapses duplicate misses into one database read, which is what keeps a cold cache from stampeding the database.',
    },
    lesson: 'Plan for the cache being cold. Several nodes and request coalescing keep a cache blip from becoming a database outage.',
    solution: (g) => {
      setParam(g, 'cache', 'replicas', 2);
      return setParam(g, 'cache', 'request_coalescing', true);
    },
  },
  {
    id: 'slow-recommendations',
    title: 'A slow recommendation service froze the whole storefront',
    category: 'reliability', difficulty: 'intermediate',
    product: 'A fashion retailer',
    headline: 'A bad deploy made the recommendation service take more than a second per call. The storefront could render without recommendations, yet every page now hangs and errors are climbing.',
    symptoms: ['Recommendations take about 1.2 seconds per call today.', 'Storefront servers have no dependency timeout.', 'Thread pools on the storefront are exhausted.'],
    traffic: { requests_per_second: 3000 },
    overrides: { recs: { latencyMult: 60 } },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Shoppers', { users: 3000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'web', 'Storefront', { replicas: 6, timeout_ms: 0 });
      node('Service', 'recs', 'Recommendations', { replicas: 4, retry_limit: 0 });
      node('Cache', 'cache', 'Catalog Cache', { hit_ratio: 0.9, replicas: 2, request_coalescing: true });
      node('Database', 'db', 'Catalog DB', { read_ratio: 0.95, read_replicas: 1, failover: true });
      chain('users', 'lb', 'web', 'cache');
      link('web', 'db');
      link('web', 'recs', { optional: true });
    }),
    goals: [
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
      { type: 'latency_below', value: 600, text: 'p95 latency under 600 ms' },
    ],
    constraints: { max_new_components: 0, keep_edges: [['web', 'recs']], note: 'Keep calling recommendations; just stop them from hurting the storefront.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'web', explanation: 'The storefront waits forever on an optional dependency. Each slow call holds a thread, so the storefront runs out of threads and starts failing every request.' },
    hints: ['The page can render without recommendations. So why is the whole page failing?', 'Give the storefront a timeout a little above the normal recommendation latency, and consider a circuit breaker.'],
    debrief: {
      question: 'Why did a slow optional dependency cause errors on pages that did not need it?',
      options: ['Waiting requests held threads until the storefront had none left for any request', 'The database was overloaded', 'Recommendations returned errors', 'The load balancer removed the storefront'],
      answer: 0,
      explanation: 'Without timeouts, latency turns into resource exhaustion. A timeout bounds how long a thread waits; a circuit breaker stops calling while the dependency is unhealthy.',
    },
    lesson: 'Timeouts and fallbacks turn a slow dependency into a degraded page instead of an outage.',
    solution: (g) => {
      setParam(g, 'web', 'timeout_ms', 150);
      return setParam(g, 'web', 'circuit_breaker', true);
    },
  },
  {
    id: 'dinner-rush-writes',
    title: 'Dinner rush: order writes overwhelm the primary',
    category: 'databases', difficulty: 'intermediate',
    product: 'A food delivery app',
    headline: 'Every evening at 7 pm orders spike and the orders database falls over. Last week the team added two read replicas; nothing improved.',
    symptoms: ['Half of all database traffic is writes (orders and status updates).', 'Replicas are nearly idle; the primary is at 100%.', 'Menu reads are already cached.'],
    traffic: { requests_per_second: 4000 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Diners', { users: 4000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'api', 'Orders API', { autoscaling: true, autoscale_min: 4, autoscale_max: 20 });
      node('Cache', 'menu', 'Menu Cache', { hit_ratio: 0.9, replicas: 2, request_coalescing: true });
      node('Database', 'db', 'Orders DB', { read_ratio: 0.5, read_replicas: 2, failover: true });
      chain('users', 'lb', 'api', 'menu');
      link('api', 'db');
    }),
    goals: [
      { type: 'node_load_below', node: 'db', value: 0.9, text: 'Orders DB under 90% load' },
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
    ],
    constraints: { max_new_components: 2, note: 'Configuration changes are allowed; you may add up to two components.' },
    root_cause: { question: 'Which component is the bottleneck?', answer: 'db', explanation: 'All writes land on the single primary. Read replicas only take reads, so adding them did not touch the bottleneck.' },
    hints: ['Replicas copy data for reads. Where do writes go?', 'Split the data across several primaries, for example by restaurant or region.'],
    debrief: {
      question: 'Why did adding two read replicas not help?',
      options: ['Every write still goes to the one primary; replicas only spread reads', 'Replicas were misconfigured', 'Replicas slow down writes by half', 'The cache was too small'],
      answer: 0,
      explanation: 'Read replicas scale reads. Write throughput scales with the number of primaries, which is what sharding gives you.',
    },
    lesson: 'Diagnose the bottleneck before you scale: read replicas fix read pressure, sharding fixes write pressure.',
    solution: (g) => setParam(g, 'db', 'shards', 3),
  },
  {
    id: 'static-assets-from-app',
    title: 'App servers are busy serving JavaScript',
    category: 'edge', difficulty: 'easy',
    product: 'A team collaboration tool',
    headline: 'After a redesign the web bundle doubled in size. App servers are at 100% and API calls are timing out, even though the API itself is cheap.',
    symptoms: ['Seventy percent of requests are for JS, CSS and images.', 'Those files are identical for every user.', 'There is no CDN in front of the app.'],
    traffic: { requests_per_second: 6500 },
    graph: design(({ node, chain, link }) => {
      node('User', 'users', 'Team Members', { users: 6500, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer', { https: true });
      node('Server', 'web', 'Web Servers', { replicas: 6 });
      node('Database', 'db', 'Workspace DB', { capacity: 3000, read_ratio: 0.9, read_replicas: 1, failover: true });
      chain('users', 'lb', 'web');
      link('web', 'db', { ratio: 0.3 });
    }),
    goals: [
      { type: 'node_load_below', node: 'web', value: 0.7, text: 'Web servers under 70% load' },
      { type: 'error_below', value: 0.01, text: 'Error rate under 1%' },
    ],
    constraints: { max_new_components: 1, allowed_new: ['CDN'], keep_config: [['web', 'replicas', 6]], note: 'Budget is frozen: no new web servers. You may add one CDN.' },
    root_cause: { question: 'Which component is wasting its capacity?', answer: 'web', explanation: 'The web servers spend most of their time sending identical static files that an edge cache could serve.' },
    hints: ['Static files are the same for everyone. Who else could serve them?', 'Put a CDN between users and the load balancer.'],
    debrief: {
      question: 'Your JS bundle is cached at the CDN for a year. How do you ship a new version?',
      options: ['Put a content hash in the file name, so each build gets a new URL', 'Purge the whole CDN every deploy', 'Set the TTL to zero', 'Ask users to clear their cache'],
      answer: 0,
      explanation: 'Fingerprinted file names make long TTLs safe: old URLs stay cached forever and new builds are fetched on first use.',
    },
    lesson: 'Static content belongs at the edge. App servers should spend their capacity on work only they can do.',
    solution: (g) => {
      addNode(g, fresh('CDN', 'cdn', 'CDN', { advanced: { cache_hit_ratio: 0.7 } }));
      removeEdge(g, 'users', 'lb');
      addEdge(g, 'users', 'cdn');
      return addEdge(g, 'cdn', 'lb');
    },
  },
  {
    id: 'uneven-backends',
    title: 'Fleet migration: half the requests land on the small server',
    category: 'load-balancing', difficulty: 'intermediate',
    product: 'A ride-hailing back office',
    headline: 'During a hardware migration one large and one small server sit behind the load balancer. The small one is drowning while the large one is half idle.',
    symptoms: ['The new server is about four times bigger than the old one.', 'The load balancer splits requests evenly.', 'Errors come only from the small server.'],
    traffic: { requests_per_second: 3000 },
    graph: design(({ node, link }) => {
      node('User', 'users', 'Operators', { users: 3000, requests_per_user: 1 });
      node('LoadBalancer', 'lb', 'Load Balancer');
      node('Server', 'big', 'New Server (large)', { capacity: 3000 });
      node('Server', 'small', 'Old Server (small)', { capacity: 800 });
      node('Database', 'db', 'Ops DB', { capacity: 4000, read_ratio: 0.8, read_replicas: 1, failover: true });
      link('users', 'lb');
      link('lb', 'big');
      link('lb', 'small');
      link('big', 'db');
      link('small', 'db');
    }),
    goals: [{ type: 'error_below', value: 0.01, text: 'Error rate under 1%' }],
    constraints: { max_new_components: 0, keep_config: [['big', 'replicas', 1], ['small', 'replicas', 1]], note: 'No extra instances during the migration.' },
    root_cause: { question: 'Which component is configured badly?', answer: 'lb', explanation: 'Round robin sends half of the traffic to each backend regardless of size, so the small server gets far more than it can handle.' },
    hints: ['The two backends are not the same size. Does the load balancer know that?', 'Try a load balancing algorithm that sends more work to backends with more free capacity.'],
    debrief: {
      question: 'Which algorithm suits requests that vary wildly in cost (some 5 ms, some 5 s)?',
      options: ['Least connections (or least outstanding requests)', 'Round robin', 'Random', 'IP hash'],
      answer: 0,
      explanation: 'Least connections balances by work in flight rather than request count, so slow requests do not pile up on one backend.',
    },
    lesson: 'Round robin assumes identical backends and identical requests. When either differs, balance by load.',
    solution: (g) => setParam(g, 'lb', 'algorithm', 'least_connections'),
  },
];

export const INCIDENTS = [...DESIGN_INCIDENTS.map((inc) => ({ track: 'design', ...inc })), ...KAFKA_INCIDENTS];

export function getIncident(id) {
  return INCIDENTS.find((i) => i.id === id) || null;
}

export function publicIncident(inc) {
  const { solution, root_cause: rc, debrief, hints, ...rest } = inc;
  const shuffled = shuffleQuestion(`${inc.id}:debrief`, debrief.options, debrief.answer);
  return {
    ...rest,
    root_cause: { question: rc.question, options: inc.graph.nodes.filter((n) => n.type !== 'User').map((n) => ({ id: n.id, label: n.label, type: n.type })) },
    debrief: { question: debrief.question, options: shuffled.options },
    hint_count: hints.length,
  };
}

export function incidentSummary(inc) {
  return { id: inc.id, title: inc.title, category: inc.category, difficulty: inc.difficulty, product: inc.product, headline: inc.headline };
}

const kafkaOptions = (inc) => ({ kafka: inc.kafka, horizonSec: inc.kafka?.horizon_hours ? inc.kafka.horizon_hours * 3600 : undefined });

function failureRun(graph, inc, prefer) {
  const node = graph.nodes.filter((n) => prefer.includes(n.type)).sort((a, b) => (param(b, 'replicas') || 1) - (param(a, 'replicas') || 1))[0];
  if (!node) return null;
  return simulate(graph, { traffic: inc.traffic, overrides: { ...(inc.overrides || {}), ...failureOverrides(node) }, ...kafkaOptions(inc) });
}

function pct(v) {
  return `${(v * 100).toFixed(1)}%`;
}

export function checkIncident(inc, graphInput) {
  const { graph } = normalizeGraph(graphInput);
  const sim = simulate(graph, { traffic: inc.traffic, overrides: inc.overrides || {}, ...kafkaOptions(inc) });
  const byId = new Map(sim.node_metrics.map((n) => [n.id, n]));
  let rules = null;

  const goals = inc.goals.map((g) => {
    let pass = false;
    let detail = '';
    switch (g.type) {
      case 'error_below':
        pass = sim.error_rate < g.value;
        detail = `${pct(sim.error_rate)} of requests fail`;
        break;
      case 'latency_below':
        pass = sim.latency_ms.p95 < g.value;
        detail = `p95 is ${Math.round(sim.latency_ms.p95)} ms`;
        break;
      case 'node_load_below': {
        const n = byId.get(g.node);
        pass = !!n && n.status !== 'down' && n.utilization < g.value;
        detail = n ? `${n.label} at ${Math.round(Math.min(n.utilization, 9.99) * 100)}%` : 'that component was removed';
        break;
      }
      case 'backlog_below':
        pass = sim.async.backlog_rps < g.value && !sim.async.overflowing.length;
        detail = sim.async.backlog_rps > 0 ? `backlog grows by ${Math.round(sim.async.backlog_rps)} msg/s` : 'no backlog';
        break;
      case 'node_reached': {
        const n = byId.get(g.node);
        pass = !!n && n.demand_rps > 0;
        detail = n ? (n.demand_rps > 0 ? `${n.label} receives ${Math.round(n.demand_rps)} rps` : `${n.label} receives no traffic`) : 'that component was removed';
        break;
      }
      case 'survives_failure': {
        const run = failureRun(graph, inc, g.prefer);
        pass = !!run && run.error_rate < g.max_error;
        detail = run ? `${pct(run.error_rate)} errors during the failure` : 'no component of that type to fail';
        break;
      }
      case 'failure_node_load_below': {
        const run = failureRun(graph, inc, g.prefer);
        const n = run?.node_metrics.find((x) => x.id === g.node);
        pass = !!n && n.utilization < g.value;
        detail = n ? `${n.label} at ${Math.round(Math.min(n.utilization, 9.99) * 100)}% during the failure` : 'not measurable';
        break;
      }
      case 'config_equals': {
        const n = graph.nodes.find((x) => x.id === g.node);
        pass = !!n && param(n, g.key) === g.value;
        detail = n ? `${g.key} is ${JSON.stringify(param(n, g.key))}` : 'that component was removed';
        break;
      }
      case 'security_check': {
        rules = rules || analyzeDesign(graph);
        const check = rules.security.checks.find((c) => c.code === g.check);
        pass = !!check?.passed;
        detail = pass ? 'in place' : 'missing';
        break;
      }
      case 'freshness_below': {
        const f = sim.kafka?.freshness_ms;
        pass = !!sim.kafka && f !== null && f < g.value;
        detail = !sim.kafka ? 'no Kafka topic in the design' : f === null ? 'consumers never catch up' : `consumed data is ${fmtDuration(f / 1000)} old`;
        break;
      }
      case 'lag_below': {
        const lag = sim.kafka?.lag_rps ?? 0;
        pass = !!sim.kafka && lag < g.value;
        detail = lag >= 0.5 ? `lag grows by ${Math.round(lag)} messages a second` : 'consumers keep up';
        break;
      }
      case 'no_stuck': {
        const stuck = sim.kafka?.stuck_rate ?? 0;
        pass = !!sim.kafka && stuck === 0;
        detail = stuck > 0 ? `${pct(stuck)} of messages are stuck` : 'nothing is stuck';
        break;
      }
      case 'no_data_loss': {
        const lost = sim.kafka?.data_loss_rate ?? 0;
        pass = !!sim.kafka && lost === 0;
        detail = lost > 0 ? `${pct(lost)} of messages expire unread` : 'every message is kept until it is read';
        break;
      }
      case 'guarantee': {
        const run = g.prefer ? failureRun(graph, inc, g.prefer) : sim;
        const broken = run ? guaranteeViolations(run, [g.requirement]) : [];
        pass = !!run?.kafka && broken.length === 0;
        detail = !run ? 'no component of that type to fail' : !run.kafka ? 'no Kafka topic in the design' : broken.length ? broken[0].text : 'kept';
        break;
      }
      default:
        detail = 'unknown goal';
    }
    return { text: g.text, pass, detail };
  });

  const original = new Map(inc.graph.nodes.map((n) => [n.id, n]));
  const added = graph.nodes.filter((n) => !original.has(n.id));
  const removed = inc.graph.nodes.filter((n) => !graph.nodes.some((x) => x.id === n.id));
  const problems = [];
  const c = inc.constraints || {};
  if (removed.length) problems.push(`Keep the existing components (${removed.map((n) => n.label).join(', ')} ${removed.length === 1 ? 'was' : 'were'} removed).`);
  if (c.max_new_components !== undefined && added.length > c.max_new_components) {
    problems.push(c.max_new_components === 0 ? 'Solve this without adding components.' : `Add at most ${c.max_new_components} component${c.max_new_components === 1 ? '' : 's'}.`);
  }
  if (c.allowed_new) {
    const bad = added.filter((n) => !c.allowed_new.includes(n.type));
    if (bad.length) problems.push(`Only these new components are allowed here: ${c.allowed_new.join(', ')}.`);
  }
  for (const [src, dst] of c.keep_edges || []) {
    if (!graph.edges.some((e) => e.source === src && e.target === dst)) problems.push(`Keep the connection from ${original.get(src)?.label || src} to ${original.get(dst)?.label || dst}.`);
  }
  for (const [id, key, max] of c.keep_config || []) {
    const n = graph.nodes.find((x) => x.id === id);
    if (n && (Number(param(n, key)) > max || param(n, 'autoscaling'))) problems.push(`${n.label} must stay at ${max} instance${max === 1 ? '' : 's'} with autoscaling off.`);
  }
  for (const [id, key, max, why] of c.config_max || []) {
    const n = graph.nodes.find((x) => x.id === id);
    if (n && Number(param(n, key)) > max) problems.push(why);
  }

  return {
    goals,
    constraint: { ok: problems.length === 0, problems },
    solved: goals.every((g) => g.pass) && problems.length === 0,
    metrics: summarizeSim(sim),
    sim,
  };
}

// ================================================================ content: interview bank
// Facts about a submitted design that question templates can test against.
export function designFacts(graph) {
  const idx = new GraphIndex(graph);
  const ofType = (t) => graph.nodes.filter((n) => n.type === t);
  const users = idx.users().map((u) => u.id);
  const syncReach = new Set();
  const stack = [...users];
  while (stack.length) {
    const id = stack.pop();
    if (syncReach.has(id)) continue;
    syncReach.add(id);
    for (const e of idx.out(id)) if (e.kind !== 'async' && !['Queue', 'KafkaTopic'].includes(idx.type(e.target))) stack.push(e.target);
  }
  return {
    has: (t) => ofType(t).length > 0,
    label: (t) => ofType(t)[0]?.label || t,
    nodes: ofType,
    some: (t, fn) => ofType(t).some((n) => fn((k) => param(n, k), n)),
    onPath: (t) => ofType(t).filter((n) => syncReach.has(n.id)),
    idx,
  };
}

const T = (id, applies, question, concepts, model) => ({ id, applies, question, concepts, model });
const C = (label, keywords) => ({ label, keywords });

const readsTopic = (f) => f.has('ConsumerGroup') || f.has('StreamProcessor') || f.has('KafkaConnect');

export const TRADEOFF_TEMPLATES = [
  T('kafka-keys', (f) => f.has('KafkaTopic'),
    (f) => `How did you choose the message key and the number of partitions for ${f.label('KafkaTopic')}? What happens to ordering and load if one key becomes very popular?`,
    [C('Key by the entity whose order matters', ['key', 'order id', 'user id', 'device id', 'entity', 'same partition']),
      C('Order only holds within a partition', ['within a partition', 'per partition', 'per key', 'ordering']),
      C('Partitions set consumer parallelism', ['partition count', 'consumers', 'parallel', 'throughput', 'scale']),
      C('Hot keys and how to split them', ['hot', 'skew', 'popular', 'split', 'salt', 'suffix'])],
    'Key every message by the entity whose events must stay in order (an order, user or device id): the same key always lands on the same partition, and Kafka only keeps order within a partition. Size the partition count for peak throughput and the largest consumer group you expect, since a group cannot use more consumers than partitions, and remember partitions can be added but never removed. A key with one huge value creates a hot partition that caps the topic; pick a finer key or split the hot key with a suffix and recombine downstream.'),
  T('kafka-delivery', readsTopic,
    () => 'What delivery guarantee do your consumers give? Walk through what happens to the messages a consumer is working on when it crashes.',
    [C('Commit after processing (at-least-once)', ['at-least-once', 'at least once', 'commit after', 'offset']),
      C('Idempotent processing makes replays harmless', ['idempot', 'dedup', 'upsert', 'idempotency key', 'processed ids']),
      C('Exactly-once and its limits', ['exactly-once', 'exactly once', 'transaction', 'read_committed', 'inside kafka']),
      C('Dead letter topic for messages that keep failing', ['dead letter', 'dlq', 'dlt', 'poison', 'retry topic'])],
    'Consumers commit offsets only after processing, so a crash replays the messages since the last commit (at-least-once) instead of skipping them. Processing is idempotent (upserts keyed by event id, or a table of handled ids, or an idempotency key with external providers), so a replay changes nothing. Kafka transactions give exactly-once for read-process-write inside Kafka, but side effects such as charging a card still need idempotency. Messages that keep failing go to a dead letter topic after a few retries so they cannot block a partition.'),
  T('kafka-durability', (f) => f.has('KafkaTopic'),
    () => 'A broker in your Kafka cluster dies. What happens to producers, to consumers, and to data that was already written?',
    [C('Replication factor and in-sync replicas', ['replication factor', 'replica', 'isr', 'in-sync', 'copies']),
      C('acks=all with min.insync.replicas', ['acks=all', 'acks all', 'min.insync', 'min insync']),
      C('Leader election and client failover', ['leader', 'election', 'failover', 'new leader', 'retry']),
      C('Monitoring and spreading across zones', ['under-replicated', 'monitor', 'alert', 'rack', 'zone'])],
    'Each partition has copies on three brokers (replication factor 3). Producers use acks=all with min.insync.replicas 2, so every confirmed write is already on at least two brokers. When a broker dies, partitions it led elect a new leader from the in-sync replicas; producers and consumers retry against the new leaders after a few seconds, and nothing confirmed is lost. Writes keep working with two copies; if a second broker died, writes would be refused rather than kept on one copy. Copies are spread across zones, and under-replicated partitions are alerted on.'),
  T('kafka-lag', readsTopic,
    () => 'How would you know that your consumers are falling behind, and what would you do about it?',
    [C('Measure lag, ideally in time', ['lag', 'behind', 'offset', 'seconds', 'age']),
      C('Scale consumers up to the partition count', ['add consumers', 'autoscal', 'scale out', 'more consumers', 'partition count']),
      C('Add partitions when consumers are maxed out', ['add partitions', 'more partitions', 'repartition']),
      C('Retention as the safety margin', ['retention', 'expire', 'deleted', 'replay', 'catch up'])],
    'Monitor consumer lag per group and partition, and alert on lag in time ("2 minutes behind") rather than messages. A short rise after a spike is fine because Kafka absorbs the burst; a steady rise means the group is too slow. Add consumers (or autoscale on lag) up to the partition count, then add partitions, or make each message cheaper to process (batch writes, parallelise calls). Retention is the safety margin: lag must never approach it, or unread messages are deleted.'),
  T('kafka-outbox', (f) => f.has('KafkaTopic') && f.has('Database'),
    () => 'Your service changes data in its database and publishes an event about it. How do you make sure the two never disagree?',
    [C('Avoid dual writes', ['dual write', 'two writes', 'two steps', 'crash between', 'inconsisten']),
      C('Transactional outbox', ['outbox', 'same transaction', 'outbox table']),
      C('Change data capture', ['cdc', 'change data capture', 'debezium', 'transaction log', 'binlog', 'wal']),
      C('Idempotent consumers downstream', ['idempot', 'dedup', 'duplicate', 'at-least-once'])],
    'Writing the database and then publishing (a dual write) breaks when the service crashes in between or one side fails. Use the transactional outbox: write the event to an outbox table in the same transaction as the data change, and let a CDC connector such as Debezium stream committed outbox rows to Kafka from the database log. The event exists if and only if the data change committed. CDC delivers at least once, so consumers stay idempotent.'),
  T('cache-consistency', (f) => f.has('Cache'),
    (f) => `Your design reads through ${f.label('Cache')}. When the underlying data changes, how do you keep cached entries correct, and how stale is acceptable for this product?`,
    [C('Invalidate or update entries on writes', ['invalidat', 'delete', 'evict', 'write-through', 'write through', 'purge', 'update the cache']),
      C('Use TTL as a bound on staleness', ['ttl', 'expire', 'expiry', 'time to live']),
      C('State what staleness the product tolerates', ['stale', 'seconds', 'minutes', 'acceptable', 'fresh', 'eventual']),
      C('Protect the database from stampedes', ['stampede', 'coalesc', 'thundering', 'jitter', 'single flight', 'single-flight', 'lock'])],
    'Delete or update the cache entry whenever the source changes, and keep a TTL as a safety net that bounds staleness if an invalidation is missed. Decide per data type how stale is acceptable: seconds for listings and counts, never for balances or inventory decisions, which should read the source of truth. Add TTL jitter and request coalescing so mass expiry or a restart cannot stampede the database.'),
  T('replica-lag', (f) => f.some('Database', (p) => p('read_replicas') > 0 && p('replication') === 'async'),
    (f) => `${f.label('Database')} serves reads from asynchronous replicas. A user updates something and immediately reloads the page. What might they see, and how would you handle it?`,
    [C('Replication lag causes stale reads', ['lag', 'stale', 'behind', 'old value', 'not yet replicated']),
      C('Read-your-writes from the primary', ['read-your-writes', 'read your writes', 'primary', 'leader', 'master']),
      C('Session stickiness or version tokens', ['sticky', 'session', 'version', 'timestamp', 'token', 'lsn']),
      C('Accept eventual consistency where it is harmless', ['eventual', 'acceptable', 'tolerat', 'trade'])],
    'They may see the old value because the read hit a replica that has not caught up. Route a user\'s reads to the primary for a short window after they write (read-your-writes), or pass a version/LSN so the read waits for a replica that has it. For data where brief staleness is harmless, accept eventual consistency and keep reading from replicas.'),
  T('queue-semantics', (f) => f.has('Queue'),
    (f) => `You put ${f.label('Queue')} between producers and consumers. Which delivery guarantee do you rely on, and how do you stop a redelivered message from causing duplicate side effects?`,
    [C('Name the delivery guarantee', ['at-least-once', 'at least once', 'at-most-once', 'exactly-once', 'exactly once']),
      C('Make consumers idempotent', ['idempot', 'dedup', 'duplicate', 'idempotency key', 'unique']),
      C('Handle poison messages', ['dead letter', 'dlq', 'poison', 'retry limit', 'backoff']),
      C('Watch the backlog', ['backlog', 'lag', 'depth', 'age', 'monitor', 'alert'])],
    'Most queues deliver at least once, so a message can be processed twice after a consumer crash or timeout. Give each message an idempotency key and record processed keys (or use conditional writes) so repeats are no-ops. Cap retries with backoff and park failing messages in a dead-letter queue, and alert on backlog age so a stalled consumer is noticed in minutes.'),
  T('third-party-path', (f) => f.onPath('ThirdPartyAPI').length > 0,
    (f) => `Requests wait on ${f.onPath('ThirdPartyAPI')[0].label}. What happens to your system when that provider slows down or fails, and how do you contain it?`,
    [C('Timeouts bound the wait', ['timeout', 'time out', 'deadline']),
      C('Circuit breaker to fail fast', ['circuit', 'breaker', 'fail fast']),
      C('Careful retries', ['retry', 'retries', 'backoff', 'jitter', 'idempot']),
      C('Fallback or async processing', ['fallback', 'degrad', 'queue', 'async', 'cached', 'default'])],
    'Without protection, slow provider calls hold threads until your servers stop serving anything. Set a timeout near the provider\'s normal p99, wrap calls in a circuit breaker that fails fast while the provider is unhealthy, retry sparingly with backoff (and idempotency keys for payments), and move anything the user does not need immediately to a queue, or show a degraded response.'),
  T('shard-key', (f) => f.some('Database', (p) => p('shards') > 1),
    (f) => `${f.label('Database')} is sharded. How would you choose the shard key, and which queries become harder?`,
    [C('Key that spreads load evenly', ['even', 'uniform', 'hash', 'distribut', 'balance']),
      C('Key present in most queries', ['query', 'access pattern', 'lookup', 'user id', 'tenant', 'locality']),
      C('Hot keys and hot shards', ['hot', 'celebrity', 'skew', 'hotspot', 'hot spot']),
      C('Cross-shard queries and rebalancing', ['cross-shard', 'cross shard', 'scatter', 'join', 'rebalanc', 'consistent hashing', 'resharding'])],
    'Pick a key that appears in most queries (so they hit one shard) and spreads writes evenly, such as a hashed user, tenant or event id. Watch for hot keys (a celebrity or a single big event) that overload one shard; split them or cache them. Queries across keys become scatter-gather and joins across shards get expensive, and adding shards needs a rebalancing plan such as consistent hashing.'),
  T('websocket-routing', (f) => f.has('WebSocketServer'),
    (f) => `With several ${f.label('WebSocketServer')} instances, the sender and recipient of a message are often connected to different servers. How does the message reach the recipient?`,
    [C('Pub/sub or message bus between servers', ['pub/sub', 'pubsub', 'publish', 'subscribe', 'broker', 'bus', 'kafka', 'redis']),
      C('Connection registry or presence', ['registry', 'presence', 'which server', 'mapping', 'lookup', 'session']),
      C('Offline delivery', ['offline', 'push', 'store', 'persist', 'inbox']),
      C('Ordering or acknowledgements', ['order', 'ack', 'sequence', 'retry'])],
    'Each server subscribes to a pub/sub channel (per user or per conversation) on a shared broker; the sender\'s server publishes, and whichever server holds the recipient\'s connection delivers it. A presence registry records which server holds each user. If the recipient is offline, the message is persisted and a push notification is queued. Sequence numbers and acknowledgements handle ordering and retries.'),
  T('single-point', (f) => f.some('Database', (p) => p('read_replicas') === 0) || f.some('LoadBalancer', (p) => p('replicas') < 2) || f.nodes('Server').some((n) => param(n, 'replicas') < 2 && !param(n, 'autoscaling')),
    () => 'Which component in your design is a single point of failure, and what would you change so that losing it does not take the system down?',
    [C('Identify the component', ['database', 'load balancer', 'server', 'cache', 'single']),
      C('Add redundancy', ['replica', 'redundan', 'second', 'standby', 'n+1', 'multiple']),
      C('Automatic failover and health checks', ['failover', 'fail over', 'health check', 'promot', 'automatic']),
      C('Spread across failure domains', ['zone', 'region', 'rack', 'availability zone', 'multi-az'])],
    'Name the component (for example the only database copy). Add a second instance with health checks and automatic failover (a replica promoted on failure, or two load balancer instances), place copies in different availability zones, and size so the survivors can carry peak load.'),
  T('cdn-scope', (f) => f.has('CDN'),
    () => 'What should and should not be cached at your CDN for this product, and how do you update content that changes?',
    [C('Cache static and public content', ['static', 'image', 'asset', 'public', 'video', 'segment', 'js', 'css']),
      C('Never publicly cache personalised data', ['personal', 'private', 'user-specific', 'account', 'authenticated']),
      C('Freshness: TTL, purge or versioned URLs', ['ttl', 'purge', 'invalidat', 'version', 'fingerprint', 'hash', 'cache-control']),
      C('Plan for the origin during misses or purges', ['origin', 'shield', 'miss', 'purge', 'headroom'])],
    'Cache static assets, media and public responses that are identical for everyone; mark user-specific responses private. Use fingerprinted file names for assets so they can be cached for a year, and short TTLs or explicit purges for public data that changes. Make sure the origin (or an origin shield) can absorb the misses after a purge.'),
  T('rate-limits', (f) => f.has('RateLimiter') || f.some('APIGateway', (p) => p('rate_limiting')),
    () => 'How do your rate limits work: what do you key them on, which algorithm do you use, and how do they stay accurate across several instances?',
    [C('Key by client identity', ['api key', 'user', 'client', 'ip', 'account', 'tenant']),
      C('Algorithm that allows bursts', ['token bucket', 'leaky', 'sliding window', 'fixed window', 'burst']),
      C('Shared counters', ['redis', 'shared', 'central', 'distributed', 'consistent']),
      C('Client-facing behaviour', ['429', 'retry-after', 'header', 'reject', 'fail open', 'fail closed'])],
    'Key limits by authenticated client (API key or user) and by IP for anonymous traffic. Use a token bucket so short bursts pass but the sustained rate is capped. Keep counters in a shared store such as Redis so every instance agrees, return 429 with Retry-After, and decide whether to fail open or closed if the limiter store is unavailable.'),
  T('search-freshness', (f) => f.has('SearchIndex'),
    (f) => `How does data get into ${f.label('SearchIndex')}, and what do users experience between a write and the moment it becomes searchable?`,
    [C('Pipeline from the source of truth', ['cdc', 'change data capture', 'queue', 'stream', 'pipeline', 'event', 'outbox']),
      C('Eventual consistency of the index', ['eventual', 'delay', 'seconds', 'stale', 'refresh']),
      C('Index is derived, database is the truth', ['source of truth', 'database', 'rebuild', 'reindex', 'derived']),
      C('Scaling the index', ['shard', 'replica', 'copy', 'copies'])],
    'The database stays the source of truth; changes flow to the index through change data capture or a queue, so new data becomes searchable after a short delay (typically seconds, set by the refresh interval). The index can be rebuilt from the database if it is lost. Shards handle data size and indexing rate, replica copies handle query load.'),
  T('ten-x', () => true,
    () => 'If traffic grew 10× tomorrow, which component in your design would break first, and what would you do about it?',
    [C('Identify the first bottleneck', ['bottleneck', 'break first', 'saturat', 'database', 'limit']),
      C('Scale it appropriately', ['shard', 'replica', 'autoscal', 'instances', 'scale out', 'partition']),
      C('Reduce load in front of it', ['cache', 'cdn', 'queue', 'batch', 'rate limit']),
      C('Mention the trade-off', ['cost', 'trade', 'complex', 'consisten', 'but', 'however'])],
    'Work through the request path with 10× traffic: stateless app servers usually scale by adding instances, so the first real limit is typically the database (write throughput) or a provider rate limit. Reduce load in front of it with caching, CDNs and queues; then scale it with replicas for reads and sharding for writes, accepting the added operational complexity and weaker cross-shard consistency.'),
];

export function pickTradeoffs(graph, count = 3) {
  const facts = designFacts(graph);
  const chosen = [];
  for (const t of TRADEOFF_TEMPLATES) {
    if (chosen.length >= count) break;
    if (t.id === 'ten-x') continue;
    if (t.applies(facts)) chosen.push(t);
  }
  if (chosen.length < count) chosen.push(TRADEOFF_TEMPLATES.find((t) => t.id === 'ten-x'));
  return chosen.slice(0, count).map((t) => ({ id: t.id, question: t.question(facts), concepts: t.concepts, model: t.model }));
}

// Five multiple-choice questions drawn from roadmap quizzes that match the design.
export function pickMcqs(graph, problem, seed) {
  const types = new Set(graph.nodes.map((n) => n.type));
  const concepts = new Set(problem?.concepts || []);
  const pool = [];
  for (const topic of TOPICS) {
    const relevant = concepts.has(topic.id) || topic.tags.some((t) => types.has(t));
    topic.quiz.forEach((question, qi) => pool.push({ topic: topic.id, qi, question, weight: relevant ? 2 : 1 }));
  }
  const order = seededPermutation(`${seed}:mcq`, pool.length).map((i) => pool[i]);
  order.sort((a, b) => b.weight - a.weight);
  const picked = [];
  const usedTopics = new Set();
  for (const item of order) {
    if (picked.length >= 5) break;
    if (usedTopics.has(item.topic)) continue;
    usedTopics.add(item.topic);
    picked.push(item);
  }
  return picked.map((item, i) => {
    const shuffled = shuffleQuestion(`${seed}:${item.topic}:${item.qi}`, item.question.options, item.question.answer);
    return { key: `${item.topic}:${item.qi}`, n: i, q: item.question.q, options: shuffled.options, answer: shuffled.answer, explain: item.question.explain, topic: item.topic };
  });
}

// ================================================================ content: index
const stripKeywords = (items) => items.map((r) => r.text);

export function problemSummary(p) {
  return {
    id: p.id, title: p.title, difficulty: p.difficulty, category: p.category, summary: p.summary, track: p.track,
    stages: p.stages.length, peak_rps: p.scale.peak_rps, concepts: p.concepts,
  };
}

export function publicProblem(p) {
  return {
    id: p.id, title: p.title, difficulty: p.difficulty, category: p.category, summary: p.summary, track: p.track,
    kafka: p.kafka || null,
    statement: p.statement,
    functional: stripKeywords(p.functional),
    non_functional: stripKeywords(p.non_functional),
    constraints: p.constraints,
    scale: p.scale,
    traffic: p.traffic,
    required: p.required,
    concepts: p.concepts.map((id) => ({ id, title: getTopic(id)?.title || id })),
    hints: p.hints,
    stages: p.stages,
    tests: p.tests.map(({ id, name, tier, scenario, multiplier, description, prefer, typeOverrides, weight, requires, kafka, ignore_freshness, async_multiplier }) => ({
      id, name, tier, scenario, multiplier: multiplier || 1, description, prefer: prefer || null, typeOverrides: typeOverrides || null, weight: weight ?? 1,
      requires: requires || [], kafka: kafka || null, ignore_freshness: !!ignore_freshness, async_multiplier: async_multiplier || 1,
    })),
  };
}

export function quizView(topic) {
  return topic.quiz.map((item, i) => {
    const s = shuffleQuestion(`${topic.id}:${i}`, item.options, item.answer);
    return { q: item.q, options: s.options, answer: s.answer, explain: item.explain };
  });
}

export function publicTopic(topic) {
  const phase = PHASES.find((p) => p.id === topic.phase);
  const index = TOPICS.indexOf(topic);
  return {
    ...topic,
    phase_title: phase?.title,
    phase_number: phase?.number,
    quiz: quizView(topic).map(({ q, options }) => ({ q, options })),
    prev: index > 0 ? { id: TOPICS[index - 1].id, title: TOPICS[index - 1].title } : null,
    next: index < TOPICS.length - 1 ? { id: TOPICS[index + 1].id, title: TOPICS[index + 1].title } : null,
  };
}

export function debriefView(inc) {
  const s = shuffleQuestion(`${inc.id}:debrief`, inc.debrief.options, inc.debrief.answer);
  return { question: inc.debrief.question, options: s.options, answer: s.answer, explanation: inc.debrief.explanation };
}

export function roadmapOverview() {
  return PHASES.map((ph) => ({
    ...ph,
    topics: TOPICS.filter((t) => t.phase === ph.id).map((t) => ({ id: t.id, title: t.title, summary: t.summary, minutes: t.minutes })),
  }));
}
