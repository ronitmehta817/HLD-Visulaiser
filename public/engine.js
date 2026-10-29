// Kafka Lab simulation engine: the component catalog, the Kafka model, the traffic simulator, design rules,
// scoring, test scenarios, live runs, stress tests and solutions. The server imports it, and the browser loads it
// as /engine.js, so it must not use anything Node-only.

// ================================================================ component catalog
// Component catalog shared by the browser editor and the server engines.
// Every component has `basic`, `advanced` and `security` parameter blocks; the
// simulator reads values through graph.param(), which falls back to these defaults.

const num = (key, label, def, opts = {}) => ({ key, label, type: 'number', default: def, ...opts });
const toggle = (key, label, def, opts = {}) => ({ key, label, type: 'toggle', default: def, ...opts });
const select = (key, label, def, options, opts = {}) => ({ key, label, type: 'select', default: def, options, ...opts });

const capacity = (def, unit = 'rps', help = 'Requests one instance can serve per second before it saturates.') =>
  num('capacity', 'Capacity per instance', def, { min: 1, unit, help });
const latency = (def, help = 'Time one instance spends on a request when it is lightly loaded.') =>
  num('base_latency', 'Base latency', def, { min: 0, unit: 'ms', help });
const replicas = (def = 1, label = 'Instances') =>
  num('replicas', label, def, { min: 1, max: 200, help: 'Identical copies running side by side. Capacity scales with the count, and losing one copy is survivable.' });
const autoscale = () => [
  toggle('autoscaling', 'Autoscaling', false, { help: 'Add instances when utilization passes ~75%, up to the maximum below.' }),
  num('autoscale_min', 'Autoscale min', 1, { min: 1, max: 200 }),
  num('autoscale_max', 'Autoscale max', 10, { min: 1, max: 500 }),
];
const callerResilience = (timeout, retries) => [
  num('timeout_ms', 'Dependency timeout', timeout, { min: 0, unit: 'ms', help: 'Give up on a slow dependency after this long. 0 means wait forever, which lets a slow dependency tie up every thread.' }),
  num('retry_limit', 'Retries', retries, { min: 0, max: 10, help: 'Extra attempts after a failed call. Retries hide blips but multiply load on a struggling dependency.' }),
  toggle('circuit_breaker', 'Circuit breaker', false, { help: 'Stop calling a dependency that is failing and fail fast instead, so it can recover.' }),
];
const auth = () => select('auth', 'Authentication', 'none', ['none', 'api_key', 'jwt', 'oauth'], { help: 'How callers prove who they are before this component does work for them.' });
const tls = (key = 'https', label = 'TLS (HTTPS)') => toggle(key, label, false, { help: 'Encrypt traffic in transit.' });
const encryption = () => toggle('encryption_at_rest', 'Encryption at rest', false, { help: 'Encrypt stored data so a stolen disk or snapshot is unreadable.' });

export const CATEGORIES = [
  { id: 'clients', label: 'Clients' },
  { id: 'edge', label: 'Edge' },
  { id: 'traffic', label: 'Traffic' },
  { id: 'compute', label: 'Compute' },
  { id: 'data', label: 'Data' },
  { id: 'async', label: 'Async' },
  { id: 'kafka', label: 'Kafka' },
  { id: 'external', label: 'External' },
];

const deadLetter = (help) => toggle('dead_letter_topic', 'Dead letter topic', false, { help });

export const COMPONENTS = {
  User: {
    label: 'Users', abbr: 'US', category: 'clients',
    description: 'Where traffic comes from: people or client apps sending requests.',
    basic: [
      num('users', 'Concurrent users', 1000, { min: 0, help: 'People active at the same time.' }),
      num('requests_per_user', 'Requests per user', 1, { min: 0, step: 0.1, unit: 'rps', help: 'Average requests each user sends per second.' }),
    ],
    advanced: [], security: [],
  },
  CDN: {
    label: 'CDN', abbr: 'CD', category: 'edge',
    description: 'Edge caches close to users. Serves cacheable responses without touching your origin.',
    basic: [capacity(100000), latency(8, 'Round trip to the nearest edge location.')],
    advanced: [
      num('cache_hit_ratio', 'Hit ratio', 0.85, { min: 0, max: 1, step: 0.01, help: 'Share of requests answered at the edge. Only misses reach the origin.' }),
      num('ttl_seconds', 'TTL', 3600, { min: 0, unit: 's', help: 'How long the edge keeps a response before revalidating.' }),
      toggle('cache_api_responses', 'Cache API responses', false, { help: 'Caching personalised API responses at the edge can show one user\'s data to another or serve stale data. Keep off for user-specific endpoints.' }),
      toggle('geo_routing', 'Geo routing', true, { help: 'Send users to the nearest edge location.' }),
    ],
    security: [toggle('https', 'TLS at the edge', true, { help: 'Terminate HTTPS at the CDN.' }), toggle('ddos_protection', 'DDoS protection', false, { help: 'Absorb volumetric attacks at the edge.' })],
  },
  LoadBalancer: {
    label: 'Load Balancer', abbr: 'LB', category: 'traffic',
    description: 'Spreads requests across identical backends and stops sending to unhealthy ones.',
    basic: [capacity(50000), latency(2)],
    advanced: [
      replicas(2, 'Instances'),
      select('algorithm', 'Algorithm', 'round_robin', ['round_robin', 'least_connections', 'ip_hash'], { help: 'Round robin and IP hash split evenly. Least connections sends more work to bigger backends.' }),
      toggle('health_checks', 'Health checks', true, { help: 'Detect dead backends and route around them.' }),
      toggle('sticky_sessions', 'Sticky sessions', false, { help: 'Pin a user to one backend. Simplifies session state but unbalances load.' }),
    ],
    security: [tls('https', 'TLS termination')],
  },
  ReverseProxy: {
    label: 'Reverse Proxy', abbr: 'RP', category: 'traffic',
    description: 'A proxy in front of your servers for TLS, compression and static files.',
    basic: [capacity(30000), latency(1)],
    advanced: [replicas(1), toggle('compression', 'Compression', false), toggle('static_file_serving', 'Serve static files', false)],
    security: [tls('https', 'TLS termination'), toggle('ddos_protection', 'DDoS protection', false)],
  },
  RateLimiter: {
    label: 'Rate Limiter', abbr: 'RL', category: 'traffic',
    description: 'Caps how fast each client may call you, so abusive clients cannot starve everyone else.',
    basic: [capacity(60000), latency(1)],
    advanced: [
      num('limit_per_client_rps', 'Limit per client', 10, { min: 0.1, step: 0.1, unit: 'rps', help: 'Sustained requests per second allowed for one client.' }),
      num('burst_multiplier', 'Burst allowance', 2, { min: 1, step: 0.1, unit: '×', help: 'Short bursts above the limit that are still accepted.' }),
      select('algorithm', 'Algorithm', 'token_bucket', ['token_bucket', 'sliding_window', 'fixed_window']),
      select('scope', 'Scope', 'per_client', ['per_client', 'per_ip', 'global']),
      select('store', 'Counter store', 'redis', ['redis', 'in_memory'], { help: 'In-memory counters drift apart once you run several limiter instances.' }),
      replicas(1),
    ],
    security: [],
  },
  APIGateway: {
    label: 'API Gateway', abbr: 'GW', category: 'traffic',
    description: 'Single front door for APIs: routing, authentication and rate limiting in one place.',
    basic: [capacity(25000), latency(4)],
    advanced: [replicas(1), toggle('rate_limiting', 'Built-in rate limiting', false, { help: 'Reject clients that exceed their quota at the gateway.' }), toggle('request_aggregation', 'Request aggregation', false)],
    security: [auth(), tls()],
  },
  Server: {
    label: 'App Server', abbr: 'SV', category: 'compute',
    description: 'Runs your application code: validates requests, applies business rules, calls data stores.',
    basic: [capacity(1000), latency(30)],
    advanced: [replicas(1), ...autoscale(), ...callerResilience(5000, 0)],
    security: [auth(), tls()],
  },
  Service: {
    label: 'Service', abbr: 'SC', category: 'compute',
    description: 'A focused service owning one domain (payments, search, users) behind its own API.',
    basic: [capacity(1200), latency(20)],
    advanced: [replicas(1), ...autoscale(), ...callerResilience(2000, 2)],
    security: [auth()],
  },
  WebSocketServer: {
    label: 'WebSocket Server', abbr: 'WS', category: 'compute',
    description: 'Holds long-lived connections so the server can push messages to clients.',
    basic: [capacity(20000, 'msg/s'), latency(3)],
    advanced: [
      replicas(1),
      num('max_connections', 'Connections per instance', 50000, { min: 1, help: 'Open connections one instance can hold.' }),
      num('heartbeat_sec', 'Heartbeat', 30, { min: 1, unit: 's' }),
      ...autoscale(),
    ],
    security: [auth(), tls()],
  },
  Cache: {
    label: 'Cache', abbr: 'CA', category: 'data',
    description: 'In-memory key-value store that answers hot reads in about a millisecond.',
    basic: [capacity(80000), latency(1)],
    advanced: [
      num('hit_ratio', 'Hit ratio', 0.8, { min: 0, max: 1, step: 0.01, help: 'Share of lookups found in the cache. Misses go to the backing store.' }),
      num('ttl_seconds', 'TTL', 300, { min: 1, unit: 's', help: 'How long entries live. TTLs under ~30 s expire hot keys before they are reused, so the real hit ratio collapses.' }),
      select('eviction', 'Eviction', 'lru', ['lru', 'lfu', 'ttl']),
      num('memory_gb', 'Memory', 4, { min: 1, unit: 'GB' }),
      replicas(1, 'Nodes'),
      toggle('request_coalescing', 'Request coalescing', false, { help: 'When many requests miss the same key at once, let one of them refill it while the others wait. Prevents a stampede on the database.' }),
      toggle('cluster_mode', 'Cluster mode', false),
    ],
    security: [encryption(), toggle('auth', 'Require auth', false)],
  },
  Database: {
    label: 'Database', abbr: 'DB', category: 'data',
    description: 'Durable source of truth. Writes go to the primary; reads can be spread over replicas.',
    basic: [capacity(2000, 'qps'), latency(10)],
    advanced: [
      select('engine', 'Engine', 'sql', ['sql', 'nosql']),
      num('read_ratio', 'Read share', 0.8, { min: 0, max: 1, step: 0.01, help: 'Share of queries that are reads. Writes always land on a primary.' }),
      num('read_replicas', 'Read replicas', 0, { min: 0, max: 15, help: 'Copies that serve reads and can take over if the primary fails.' }),
      num('shards', 'Shards', 1, { min: 1, max: 64, help: 'Split data across primaries. Each shard takes a slice of writes.' }),
      select('replication', 'Replication', 'async', ['async', 'sync'], { help: 'Sync replication never loses acknowledged writes but makes every write slower.' }),
      toggle('failover', 'Automatic failover', false, { help: 'Promote a replica when the primary dies. Needs at least one replica.' }),
      num('connection_pool', 'Connection pool', 100, { min: 1 }),
      toggle('indexed', 'Queries use indexes', true, { help: 'Without indexes every query scans the table: roughly 4× less capacity and 5× more latency.' }),
    ],
    security: [encryption(), toggle('backups', 'Point-in-time backups', false, { help: 'Restore to any moment after accidental deletes or corruption.' })],
  },
  SearchIndex: {
    label: 'Search Index', abbr: 'SI', category: 'data',
    description: 'Inverted index for full-text and filtered search that a database cannot do cheaply.',
    basic: [capacity(3000), latency(12)],
    advanced: [
      num('shards', 'Shards', 1, { min: 1, max: 64 }),
      num('replicas', 'Replica copies', 0, { min: 0, max: 10, help: 'Extra copies of each shard. They add read capacity and survive node loss.' }),
      num('refresh_interval_sec', 'Refresh interval', 1, { min: 0, unit: 's' }),
      toggle('fuzzy', 'Fuzzy matching', false),
    ],
    security: [encryption()],
  },
  BlobStorage: {
    label: 'Blob Storage', abbr: 'BS', category: 'data',
    description: 'Object storage for files, images and video. Cheap and durable, but slow per request.',
    basic: [capacity(10000), latency(40)],
    advanced: [
      num('replication_factor', 'Replication factor', 3, { min: 1, max: 6 }),
      select('storage_class', 'Storage class', 'standard', ['standard', 'infrequent', 'archive'], { help: 'Archive is cheap but retrieval takes minutes.' }),
      toggle('versioning', 'Versioning', false),
    ],
    security: [encryption()],
  },
  Queue: {
    label: 'Queue', abbr: 'QU', category: 'async',
    description: 'Buffers work between producers and consumers so bursts do not hit consumers directly.',
    basic: [capacity(30000, 'msg/s'), latency(3)],
    advanced: [
      num('max_depth', 'Max depth', 100000, { min: 1, help: 'Messages the queue can hold. When it is full, producers get errors.' }),
      select('delivery', 'Delivery', 'at_least_once', ['at_most_once', 'at_least_once', 'exactly_once'], { help: 'At-least-once means a message can be delivered twice after a crash.' }),
      toggle('dead_letter_queue', 'Dead-letter queue', true, { help: 'Park messages that keep failing instead of retrying forever.' }),
      num('retention_hours', 'Retention', 72, { min: 1, unit: 'h' }),
      num('partitions', 'Partitions', 4, { min: 1, max: 256, help: 'With strict ordering, only one consumer per partition can work at a time.' }),
      toggle('fifo', 'Strict ordering', false),
    ],
    security: [encryption()],
  },
  Worker: {
    label: 'Worker', abbr: 'WK', category: 'async',
    description: 'Background process that takes jobs off a queue and does slow work outside the request.',
    basic: [capacity(200, 'jobs/s'), latency(40)],
    advanced: [
      replicas(1),
      num('concurrency', 'Concurrency', 8, { min: 1, max: 256, help: 'Jobs one instance runs at once. Capacity scales with it (8 is the baseline).' }),
      toggle('idempotent', 'Idempotent processing', false, { help: 'Running the same job twice has the same effect as running it once.' }),
      ...autoscale(),
      ...callerResilience(30000, 3),
    ],
    security: [],
  },
  ThirdPartyAPI: {
    label: 'Third-party API', abbr: '3P', category: 'external',
    description: 'Someone else\'s service: payments, email, SMS, maps. You do not control its speed or uptime.',
    basic: [capacity(300, 'rps', 'Rate limit the provider allows you.'), latency(120)],
    advanced: [
      select('provider', 'Provider type', 'payment', ['payment', 'email', 'sms', 'push', 'maps', 'llm', 'other']),
      num('timeout_ms', 'Call timeout', 3000, { min: 0, unit: 'ms', help: 'How long callers wait for this provider. 0 means wait forever.' }),
      num('retry_limit', 'Retries', 1, { min: 0, max: 10 }),
      toggle('circuit_breaker', 'Circuit breaker', false, { help: 'Stop calling the provider while it is failing and fail fast.' }),
      num('error_rate', 'Provider error rate', 0.002, { min: 0, max: 1, step: 0.001, help: 'Share of calls the provider fails on its own.' }),
    ],
    security: [],
  },
  KafkaTopic: {
    label: 'Kafka Topic', abbr: 'KT', category: 'kafka',
    description: 'A named, append-only log split into partitions and copied across brokers. Producers write to it; every consumer group reads all of it at its own pace.',
    basic: [
      num('partitions', 'Partitions', 6, { min: 1, max: 2000, help: 'The topic is split into this many ordered logs. More partitions allow more parallel writes and more consumers working at once.' }),
      capacity(5000, 'msg/s', 'Messages one partition takes per second (1 KB messages, before batching and compression gains). One partition has one leader broker.'),
      latency(5, 'Time for the leader broker to append a write, before waiting for followers.'),
    ],
    advanced: [
      num('brokers', 'Brokers', 3, { min: 1, max: 100, help: 'Servers in the Kafka cluster. Partition leaders and their copies are spread over them.' }),
      num('replication_factor', 'Replication factor', 3, { min: 1, max: 5, help: 'Copies of every partition, each on a different broker. 3 is the usual choice.' }),
      num('min_insync_replicas', 'Min in-sync replicas', 2, { min: 1, max: 5, help: 'With acks=all, a write succeeds only while at least this many copies are up to date. 2 (with 3 copies) survives one broker loss without losing data.' }),
      select('acks', 'Producer acks', 'all', ['all', '1', '0'], { help: 'all: every in-sync copy confirms (safest). 1: only the leader confirms (faster, loses data if the leader dies). 0: nobody confirms.' }),
      toggle('idempotent_producer', 'Idempotent producer', true, { help: 'The producer numbers its messages, so a retry never writes a message twice or out of order.' }),
      select('partition_key', 'Message key', 'id', ['id', 'none', 'skewed'], { help: 'id: keyed by a well-spread id (user, order), so load is even and events for one id stay in order. none: no key, even spread, no ordering. skewed: a key with a few huge values (country, big tenant).' }),
      num('hot_key_share', 'Hottest key share', 0.3, { min: 0, max: 1, step: 0.05, help: 'With a skewed key: the share of all messages that carry the most common key. They all land on one partition.' }),
      num('linger_ms', 'Linger', 5, { min: 0, max: 1000, unit: 'ms', help: 'How long the producer waits to fill a batch. Batching makes Kafka far more efficient; 5–20 ms is typical.' }),
      select('compression', 'Compression', 'lz4', ['none', 'lz4', 'snappy', 'zstd', 'gzip'], { help: 'Compressed batches use less disk and network, so the same brokers carry more messages.' }),
      num('message_kb', 'Message size', 1, { min: 0.1, max: 1024, step: 0.1, unit: 'KB' }),
      num('retention_hours', 'Retention', 168, { min: 0.1, max: 87600, unit: 'h', help: 'How long messages are kept, read or not. A consumer further behind than this loses messages.' }),
      select('cleanup_policy', 'Cleanup policy', 'delete', ['delete', 'compact', 'compact_delete'], { help: 'delete: drop messages older than the retention. compact: keep the latest message for every key forever, like a table.' }),
      toggle('rack_aware', 'Spread copies across zones', true, { help: 'Put the copies of each partition in different availability zones, so losing a zone loses no data.' }),
      toggle('tiered_storage', 'Tiered storage', false, { help: 'Old segments move to cheap object storage, so long retention costs little broker disk.' }),
      select('controller', 'Cluster metadata', 'kraft', ['kraft', 'zookeeper'], { help: 'KRaft keeps cluster metadata inside Kafka. ZooKeeper mode is deprecated and removed in Kafka 4.0.' }),
    ],
    security: [
      toggle('tls', 'TLS encryption', false, { help: 'Encrypt traffic between clients and brokers, and between brokers.' }),
      select('client_auth', 'Client authentication', 'none', ['none', 'sasl_scram', 'mtls', 'oauth'], { help: 'How producers and consumers prove who they are.' }),
      toggle('acls', 'ACLs', false, { help: 'Access control lists: which clients may write to or read from this topic.' }),
    ],
  },
  ConsumerGroup: {
    label: 'Consumer Group', abbr: 'CG', category: 'kafka',
    description: 'Consumers that share the work of reading topics. Each partition goes to one consumer of the group; every group gets every message.',
    basic: [capacity(500, 'msg/s', 'Messages one consumer processes per second.'), latency(20, 'Time to process one message, not counting the calls it makes.')],
    advanced: [
      replicas(3, 'Consumers'),
      ...autoscale(),
      select('commit', 'Offset commit', 'after_processing', ['after_processing', 'before_processing', 'transactional'], { help: 'after: at-least-once (a crash replays a few messages). before: at-most-once (a crash skips a few). transactional: exactly-once inside Kafka.' }),
      toggle('idempotent', 'Idempotent processing', false, { help: 'Handling the same message twice has the same effect as once (for example an upsert keyed by event id), so replays are harmless.' }),
      select('assignment', 'Rebalance protocol', 'cooperative', ['cooperative', 'eager'], { help: 'eager: every consumer stops while partitions are reassigned. cooperative: only the partitions that move pause.' }),
      toggle('static_membership', 'Static membership', false, { help: 'A restarting consumer keeps its partitions, so rolling deploys cause no rebalance.' }),
      num('max_poll_records', 'Max poll records', 500, { min: 1, max: 100000, help: 'Messages fetched per poll. The whole batch must be processed before the next poll.' }),
      num('max_poll_interval_ms', 'Max poll interval', 300000, { min: 1000, unit: 'ms', help: 'If a batch takes longer than this, Kafka assumes the consumer died and rebalances the group.' }),
      deadLetter('Messages that keep failing go to a separate topic, so one bad message cannot block a partition.'),
      ...callerResilience(30000, 3),
    ],
    security: [],
  },
  StreamProcessor: {
    label: 'Stream Processor', abbr: 'SP', category: 'kafka',
    description: 'Kafka Streams, ksqlDB or Flink: reads topics, filters, joins and aggregates them in real time, and writes results to new topics.',
    basic: [capacity(2000, 'msg/s', 'Messages one instance processes per second.'), latency(10)],
    advanced: [
      replicas(2, 'Instances'),
      ...autoscale(),
      select('processing_guarantee', 'Processing guarantee', 'at_least_once', ['at_least_once', 'exactly_once'], { help: 'exactly_once wraps reading, state updates and writing in one Kafka transaction: no duplicates, about 15% less throughput.' }),
      select('state', 'State', 'stateless', ['stateless', 'stateful'], { help: 'Stateful work (counts, joins, windows) keeps a local state store, backed up to a changelog topic.' }),
      num('standby_replicas', 'Standby replicas', 0, { min: 0, max: 3, help: 'Warm copies of each state store on other instances, so a failover does not rebuild state from the changelog.' }),
      select('window', 'Windowing', 'none', ['none', 'tumbling', 'hopping', 'sliding', 'session'], { help: 'How events are grouped in time for aggregations: fixed buckets (tumbling), overlapping buckets (hopping), or per burst of activity (session).' }),
      deadLetter('Records that cannot be read or processed go to a separate topic instead of stopping the app (an exception handler that logs and continues).'),
      ...callerResilience(30000, 3),
    ],
    security: [],
  },
  KafkaConnect: {
    label: 'Kafka Connect', abbr: 'KC', category: 'kafka',
    description: 'Ready-made connectors that move data between Kafka and other systems. Source connectors (such as CDC from a database log) write to topics; sink connectors copy topics into stores.',
    basic: [capacity(3000, 'msg/s', 'Messages one task moves per second.'), latency(15)],
    advanced: [
      select('mode', 'Connector type', 'sink', ['source', 'sink'], { help: 'source: reads changes from a system (for example the database log) into a topic. sink: reads a topic and writes it into a system.' }),
      select('connector', 'Connector', 'cdc', ['cdc', 'jdbc', 'search', 's3', 'http', 'other'], { help: 'cdc (Debezium) streams every committed row change from the database log. search, s3 and jdbc are common sinks.' }),
      replicas(2, 'Tasks'),
      deadLetter('Records the connector cannot write go to a separate topic instead of stopping it.'),
      toggle('exactly_once', 'Exactly-once', false, { help: 'Source connectors write records and their offsets in one transaction, so a restart never duplicates them.' }),
      ...callerResilience(30000, 3),
    ],
    security: [],
  },
  SchemaRegistry: {
    label: 'Schema Registry', abbr: 'SR', category: 'kafka',
    description: 'Stores the schema of every topic and checks that new versions stay compatible, so a producer change cannot break consumers.',
    basic: [capacity(20000, 'req/s'), latency(3)],
    advanced: [
      select('compatibility', 'Compatibility rule', 'backward', ['backward', 'forward', 'full', 'none'], { help: 'backward: new consumers can read old data. forward: old consumers can read new data. full: both. none: anything goes.' }),
      select('format', 'Format', 'avro', ['avro', 'protobuf', 'json_schema']),
      replicas(2),
    ],
    security: [],
  },
  MirrorMaker: {
    label: 'MirrorMaker', abbr: 'MM', category: 'kafka',
    description: 'Copies topics from one Kafka cluster to another, for disaster recovery or to gather many regions into one cluster.',
    basic: [capacity(5000, 'msg/s', 'Messages one task copies per second.'), latency(60, 'Network time between the two clusters.')],
    advanced: [
      replicas(2, 'Tasks'),
      toggle('sync_offsets', 'Sync consumer offsets', true, { help: 'Translate consumer group offsets, so consumers can fail over to the other cluster where they left off.' }),
    ],
    security: [],
  },
};

export const TYPES = Object.keys(COMPONENTS);

const BLOCKS = ['basic', 'advanced', 'security'];

export function paramSpec(type, key) {
  const comp = COMPONENTS[type];
  if (!comp) return null;
  for (const block of BLOCKS) {
    const spec = comp[block].find((p) => p.key === key);
    if (spec) return { ...spec, block };
  }
  return null;
}

export function defaultsFor(type) {
  const comp = COMPONENTS[type];
  const out = { basic: {}, advanced: {}, security: {} };
  if (!comp) return out;
  for (const block of BLOCKS) for (const p of comp[block]) out[block][p.key] = p.default;
  return out;
}

export function coerceParam(spec, value) {
  if (value === undefined || value === null || value === '') return spec.default;
  if (spec.type === 'number') {
    const n = Number(value);
    if (!Number.isFinite(n)) return spec.default;
    let v = n;
    if (spec.min !== undefined) v = Math.max(spec.min, v);
    if (spec.max !== undefined) v = Math.min(spec.max, v);
    return v;
  }
  if (spec.type === 'toggle') {
    if (typeof value === 'boolean') return value;
    return ['true', '1', 'yes', 'on'].includes(String(value).toLowerCase());
  }
  if (spec.type === 'select') return spec.options.includes(value) ? value : spec.default;
  return value;
}

export function publicCatalog() {
  return {
    categories: CATEGORIES,
    components: TYPES.map((type) => ({
      type,
      label: COMPONENTS[type].label,
      abbr: COMPONENTS[type].abbr,
      category: COMPONENTS[type].category,
      description: COMPONENTS[type].description,
      basic: COMPONENTS[type].basic,
      advanced: COMPONENTS[type].advanced,
      security: COMPONENTS[type].security,
    })),
  };
}

// ================================================================ design graph
const MAX_NODES = 200;
const MAX_EDGES = 600;

// Accepts the editor payload ({graph: {...}} or {nodes, edges}), with params either in
// config.basic/advanced/security or flat on config. Returns a clean graph plus issues.
export function normalizeGraph(input) {
  const raw = input && input.graph ? input.graph : input || {};
  const issues = [];
  const nodes = [];
  const seen = new Set();

  for (const n of (Array.isArray(raw.nodes) ? raw.nodes : []).slice(0, MAX_NODES)) {
    if (!n || typeof n !== 'object') continue;
    const id = String(n.id ?? '').trim();
    const type = String(n.type ?? '');
    if (!id || seen.has(id)) {
      issues.push({ code: 'BAD_NODE_ID', message: `Skipped a node with a missing or duplicate id (${id || 'empty'}).` });
      continue;
    }
    if (!COMPONENTS[type]) {
      issues.push({ code: 'UNKNOWN_TYPE', message: `Skipped node "${id}": unknown component type "${type}".` });
      continue;
    }
    seen.add(id);
    const cfgIn = n.config && typeof n.config === 'object' ? n.config : {};
    const config = defaultsFor(type);
    for (const block of ['basic', 'advanced', 'security']) {
      for (const key of Object.keys(config[block])) {
        const spec = paramSpec(type, key);
        const blockVal = cfgIn[block] && typeof cfgIn[block] === 'object' ? cfgIn[block][key] : undefined;
        const value = blockVal !== undefined ? blockVal : cfgIn[key];
        config[block][key] = coerceParam(spec, value);
      }
    }
    const label = String(n.label ?? cfgIn.name ?? COMPONENTS[type].label).slice(0, 60) || COMPONENTS[type].label;
    nodes.push({
      id, type, label,
      x: Number.isFinite(Number(n.x ?? n.position?.x)) ? Number(n.x ?? n.position?.x) : 0,
      y: Number.isFinite(Number(n.y ?? n.position?.y)) ? Number(n.y ?? n.position?.y) : 0,
      config,
    });
  }

  const edges = [];
  const edgeKeys = new Set();
  for (const e of (Array.isArray(raw.edges) ? raw.edges : []).slice(0, MAX_EDGES)) {
    if (!e || typeof e !== 'object') continue;
    const source = String(e.source ?? e.from ?? '');
    const target = String(e.target ?? e.to ?? '');
    if (!seen.has(source) || !seen.has(target)) {
      issues.push({ code: 'DANGLING_EDGE', message: `Skipped a connection that points at a missing component (${source} → ${target}).` });
      continue;
    }
    const key = `${source}->${target}`;
    if (edgeKeys.has(key)) continue;
    edgeKeys.add(key);
    const kindRaw = String(e.kind ?? e.connection_type ?? 'sync').toLowerCase();
    const ratioRaw = e.ratio ?? e.traffic_split_ratio;
    const ratio = ratioRaw === null || ratioRaw === undefined || ratioRaw === '' ? null : Number(ratioRaw);
    const readRaw = e.read_share;
    const readShare = readRaw === null || readRaw === undefined || readRaw === '' ? null : Number(readRaw);
    edges.push({
      id: String(e.id ?? key),
      source, target,
      kind: kindRaw === 'async' ? 'async' : 'sync',
      ratio: ratio !== null && Number.isFinite(ratio) ? Math.max(0, Math.min(ratio, 10)) : null,
      optional: e.optional === true || e.optional === 'true',
      read_share: readShare !== null && Number.isFinite(readShare) ? Math.max(0, Math.min(readShare, 1)) : null,
    });
  }
  return { graph: { nodes, edges }, issues };
}

export function param(node, key) {
  const c = node.config || {};
  for (const block of ['basic', 'advanced', 'security']) {
    if (c[block] && c[block][key] !== undefined) return c[block][key];
  }
  const spec = paramSpec(node.type, key);
  return spec ? spec.default : undefined;
}

export class GraphIndex {
  constructor(graph) {
    this.graph = graph;
    this.byId = new Map(graph.nodes.map((n) => [n.id, n]));
    this.outMap = new Map(graph.nodes.map((n) => [n.id, []]));
    this.inMap = new Map(graph.nodes.map((n) => [n.id, []]));
    for (const e of graph.edges) {
      if (!this.byId.has(e.source) || !this.byId.has(e.target)) continue;
      this.outMap.get(e.source).push(e);
      this.inMap.get(e.target).push(e);
    }
  }

  node(id) { return this.byId.get(id); }
  type(id) { return this.byId.get(id)?.type; }
  out(id) { return this.outMap.get(id) || []; }
  in(id) { return this.inMap.get(id) || []; }
  ofType(type) { return this.graph.nodes.filter((n) => n.type === type); }
  users() { return this.ofType('User'); }

  // Kahn's algorithm. Edges that close a cycle are reported as back edges and
  // ignored by the simulator so traffic never loops forever.
  topo() {
    const back = new Set();
    const color = new Map();
    const visit = (id) => {
      color.set(id, 1);
      for (const e of this.out(id)) {
        const c = color.get(e.target) || 0;
        if (c === 1) back.add(e.id);
        else if (c === 0) visit(e.target);
      }
      color.set(id, 2);
    };
    const roots = [...this.users().map((u) => u.id), ...this.graph.nodes.map((n) => n.id)];
    for (const id of roots) if (!color.get(id)) visit(id);

    const indeg = new Map(this.graph.nodes.map((n) => [n.id, 0]));
    for (const e of this.graph.edges) if (!back.has(e.id) && indeg.has(e.target) && indeg.has(e.source)) indeg.set(e.target, indeg.get(e.target) + 1);
    const queue = this.graph.nodes.filter((n) => indeg.get(n.id) === 0).map((n) => n.id);
    const order = [];
    while (queue.length) {
      const id = queue.shift();
      order.push(id);
      for (const e of this.out(id)) {
        if (back.has(e.id)) continue;
        indeg.set(e.target, indeg.get(e.target) - 1);
        if (indeg.get(e.target) === 0) queue.push(e.target);
      }
    }
    return { order, back };
  }

  reachableFrom(startIds, { syncOnly = false } = {}) {
    const seen = new Set();
    const stack = [...startIds];
    while (stack.length) {
      const id = stack.pop();
      if (seen.has(id)) continue;
      seen.add(id);
      for (const e of this.out(id)) {
        if (syncOnly && e.kind === 'async') continue;
        if (!seen.has(e.target)) stack.push(e.target);
      }
    }
    return seen;
  }
}

export function cloneGraph(graph) {
  return JSON.parse(JSON.stringify(graph));
}

// ================================================================ kafka model
// Kafka model shared by the simulator, the design rules and the live run.
// A KafkaTopic node stands for one topic and the cluster it lives on. Producers append to
// its partitions; every consumer group connected to it reads the whole stream at its own
// pace, and within a group one partition is read by one consumer at a time.

export const KAFKA_CONSUMERS = new Set(['ConsumerGroup', 'StreamProcessor', 'KafkaConnect', 'MirrorMaker']);
// 1 KB messages one broker writes per second, counting the copies it stores for other leaders.
export const BROKER_CAPACITY = 50000;
// Clients cache schemas, so almost no message needs a registry call.
export const SCHEMA_LOOKUP = 0.001;
export const REBALANCE_SEC = 30;
export const RESTORE_SEC = 300;
const EOS_COST = 0.85;
const STORM_EFFICIENCY = 0.15;
const COMPRESSION = { none: 1, gzip: 1.6, snappy: 1.4, lz4: 1.5, zstd: 1.8 };
const ACKS_LATENCY = { 0: 0.3, 1: 1, all: 1.8 };
const REQUIREMENTS = { 'no-loss': 'loss', 'no-duplicates': 'duplicates', ordered: 'order', consistent: 'consistency', 'no-stuck': 'stuck' };

const clamp01 = (v) => Math.min(1, Math.max(0, Number(v) || 0));
const whole = (v, min) => Math.max(min, Math.round(Number(v) || 0));
const pct = (v) => `${Math.round(v * 100)}%`;
const round1 = (v) => Math.round(v * 10) / 10;
const round3 = (v) => Math.round(v * 1000) / 1000;

export function fmtCount(n) {
  if (!Number.isFinite(n)) return '∞';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 10000) return `${Math.round(n / 1000)}k`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return `${Math.round(n)}`;
}

export function fmtDuration(sec) {
  if (!Number.isFinite(sec)) return 'forever';
  if (sec < 1) return `${Math.max(1, Math.round(sec * 1000))} ms`;
  if (sec < 90) return `${Math.round(sec)} s`;
  if (sec < 5400) return `${Math.round(sec / 60)} min`;
  if (sec < 172800) return `${+(sec / 3600).toFixed(1)} h`;
  return `${+(sec / 86400).toFixed(1)} days`;
}

// Tiny batches cost a request each; waiting a few milliseconds fills them.
export function batchingGain(lingerMs) {
  if (lingerMs >= 50) return 1.1;
  if (lingerMs >= 5) return 1;
  if (lingerMs >= 1) return 0.85;
  return 0.6;
}

function choose(n, k) {
  if (k < 0 || k > n) return 0;
  let r = 1;
  for (let i = 1; i <= k; i++) r = (r * (n - k + i)) / i;
  return r;
}

// Share of partitions with at least `k` of their `rf` copies on the `lost` failed brokers.
export function shareAtLeast(brokers, lost, rf, k) {
  if (k <= 0) return 1;
  const total = choose(brokers, rf);
  if (!total) return 0;
  let p = 0;
  for (let x = k; x <= Math.min(rf, lost); x++) p += (choose(lost, x) * choose(brokers - lost, rf - x)) / total;
  return Math.min(1, p);
}

export function topicModel(m, p, ov) {
  const partitions = whole(p('partitions'), 1);
  const brokers = whole(p('brokers'), 1);
  const rf = whole(p('replication_factor'), 1);
  const minIsr = whole(p('min_insync_replicas'), 1);
  const acks = String(p('acks'));
  const key = p('partition_key');
  const kb = Math.max(0.1, Number(p('message_kb')) || 1);
  const linger = Math.max(0, Number(p('linger_ms')) || 0);
  const gain = (COMPRESSION[p('compression')] ?? 1) * batchingGain(linger);
  const lost = Math.min(brokers, Math.max(0, -(ov.instancesDelta ?? 0)));
  Object.assign(m, {
    partitions, brokers, rf, minIsr, acks, key, kb, linger, lost,
    alive: m.down ? 0 : brokers - lost,
    idempotentProducer: !!p('idempotent_producer'),
    hot: key === 'skewed' ? Math.max(1 / partitions, clamp01(p('hot_key_share'))) : 1 / partitions,
    retentionSec: Math.max(360, (Number(p('retention_hours')) || 0) * 3600),
    compact: p('cleanup_policy') !== 'delete',
    perPartition: (m.cap / kb) * gain,
    brokerCap: (BROKER_CAPACITY / (kb * Math.max(1, Math.min(rf, brokers)))) * gain,
    offline: 0, isrShort: 0, writeFail: 0,
  });
  m.lat = m.lat * (ACKS_LATENCY[acks] ?? 1) + linger;
  if (rf > brokers) {
    m.down = true;
    m.notes.push(`A replication factor of ${rf} needs at least ${rf} brokers, so the topic cannot be created on ${brokers}.`);
    return m;
  }
  if (m.alive === 0) m.down = true;
  if (m.down) return m;
  if (lost > 0) {
    m.offline = shareAtLeast(brokers, lost, rf, rf);
    m.notes.push(`Lost ${lost} of ${brokers} brokers. The partitions they led elected new leaders on the survivors.`);
  }
  m.isrShort = acks === 'all' ? shareAtLeast(brokers, lost, rf, rf - minIsr + 1) : m.offline;
  // A keyed message must go to its own partition; unkeyed messages skip partitions with no leader.
  m.writeFail = key === 'none' ? Math.max(0, m.isrShort - m.offline) : m.isrShort;
  if (acks === 'all' && minIsr > rf) {
    m.notes.push(`min.insync.replicas is ${minIsr} but only ${rf === 1 ? 'one copy exists' : `${rf} copies exist`}, so every acks=all write is rejected.`);
  } else {
    if (m.offline > 0) m.notes.push(`${pct(m.offline)} of partitions lost every copy and are offline.`);
    if (m.isrShort > m.offline) m.notes.push(`${pct(m.isrShort - m.offline)} of partitions have fewer than ${minIsr} in-sync copies, so acks=all writes to them are rejected.`);
  }
  if (key === 'skewed' && m.hot > 1.5 / partitions) {
    m.notes.push(`The hottest key carries ${pct(m.hot)} of messages, and one key always lands on one partition, so that partition caps the topic at about ${fmtCount(m.perPartition / m.hot)} msg/s.`);
  }
  return m;
}

export function topicLimits(m) {
  if (m.down) return { capacity: 0, partitionLimit: 0, brokerLimit: 0 };
  const partitionLimit = Math.min(m.partitions * (1 - m.offline) * m.perPartition, m.perPartition / m.hot);
  const brokerLimit = m.alive * m.brokerCap;
  return { capacity: Math.max(0, Math.min(partitionLimit, brokerLimit)), partitionLimit, brokerLimit };
}

// When a consumer commits its offset decides what a crash does: replay (after), skip (before)
// or neither (transactional, exactly-once).
export function consumerModel(m, p) {
  switch (m.type) {
    case 'ConsumerGroup':
      m.commit = p('commit');
      m.idempotent = !!p('idempotent');
      m.dlq = !!p('dead_letter_topic');
      m.cooperative = p('assignment') === 'cooperative';
      m.staticMembers = !!p('static_membership');
      m.maxPoll = whole(p('max_poll_records'), 1);
      m.pollInterval = Math.max(1000, Number(p('max_poll_interval_ms')) || 300000);
      break;
    case 'StreamProcessor':
      m.commit = p('processing_guarantee') === 'exactly_once' ? 'transactional' : 'after_processing';
      m.stateful = p('state') === 'stateful';
      m.standbys = whole(p('standby_replicas'), 0);
      m.dlq = !!p('dead_letter_topic');
      break;
    case 'MirrorMaker':
      // It copies raw bytes without reading them, so bad or re-shaped messages cannot stop it.
      m.opaque = true;
      break;
    case 'KafkaConnect':
      m.source = p('mode') === 'source';
      m.commit = p('exactly_once') ? 'transactional' : 'after_processing';
      m.dlq = !!p('dead_letter_topic');
      break;
    default:
      break;
  }
  if (m.commit === 'transactional') {
    m.idempotent = true;
    m.cap *= EOS_COST;
  }
}

// Called once every model exists: tells each consumer which topics (and partitions) feed it.
export function linkConsumers(models, edges, back, windowSec) {
  for (const e of edges) {
    if (back.has(e.id)) continue;
    const src = models.get(e.source);
    const dst = models.get(e.target);
    if (src?.type === 'KafkaTopic' && dst) (dst.inputTopics ||= []).push(src);
  }
  for (const m of models.values()) {
    if (!m.inputTopics) continue;
    m.inputPartitions = m.inputTopics.reduce((s, t) => s + t.partitions, 0);
    const skewed = m.inputTopics.filter((t) => t.key === 'skewed');
    m.inputHot = skewed.length ? Math.max(...skewed.map((t) => t.hot)) / m.inputTopics.length : 0;
    m.commit ??= 'after_processing';
    m.idempotent ??= false;
    m.dlq ??= false;
    m.cooperative ??= m.type !== 'ConsumerGroup' && KAFKA_CONSUMERS.has(m.type);
    if (m.lostInst > 0) {
      const share = Math.min(1, m.lostInst / Math.max(1, m.instBefore));
      let paused = ((m.cooperative ? share : 1) * REBALANCE_SEC) / windowSec;
      if (!m.cooperative) m.notes.push('Eager rebalancing paused every consumer in the group while partitions moved.');
      if (m.stateful && !m.standbys) {
        paused += share * Math.min(1, RESTORE_SEC / windowSec);
        m.notes.push(`The lost instance's state is rebuilt from its changelog topic (about ${fmtDuration(RESTORE_SEC)}), and its partitions wait meanwhile. Standby replicas make this almost instant.`);
      }
      m.paused = Math.min(0.9, paused);
    }
  }
}

// Capacity of something that reads topics: only as many consumers as partitions can work,
// and the busiest consumer (most partitions, or the hot partition) sets the pace.
export function consumerLoad(m, d, inst, slow, ctx) {
  const P = m.inputPartitions;
  const active = Math.min(inst, P);
  const busiest = active > 0 ? Math.max(Math.ceil(P / active) / P, m.inputHot) : 1;
  const storm = ctx.storm.get(m.id) || 0;
  let stuck = 0;
  if (m.opaque) stuck = 0;
  else if (ctx.kafka.schemaBreak && !ctx.schemaGuard) stuck = 1;
  else if ((m.poison || ctx.kafka.poison) && !m.dlq && active > 0) stuck = Math.ceil(P / active) / P;
  const working = Math.max(0, 1 - stuck - (m.paused || 0));
  const full = active > 0 ? (m.cap * slow * (1 - storm * (1 - STORM_EFFICIENCY))) / busiest : 0;
  const rho = full > 0 ? d / full : d > 0 ? Infinity : 0;
  const drop = d > 0 ? 1 - working * (rho > 1 ? 1 / rho : 1) : 0;
  return { capEff: full * working, full, rho, drop, active, stuck, storm };
}

// A poll whose messages take longer than max.poll.interval.ms gets the consumer thrown out
// of the group, which triggers a rebalance, which replays the batch: a rebalance storm.
export function stormTargets(ctx, downSeen) {
  const storm = new Map();
  for (const m of ctx.models.values()) {
    if (!m.maxPoll || !m.inputPartitions || m.down) continue;
    m.batchMs = m.maxPoll * (m.lat * m.latMult + (downSeen.get(m.id) || 0));
    if (m.batchMs > m.pollInterval) storm.set(m.id, 1);
  }
  return storm;
}

function consumerNotes(m, n, group, note, ctx) {
  const P = m.inputPartitions;
  if (n.inst > P) note(m.id, `${n.inst} consumers share ${P} partitions, so ${n.inst - P} sit idle. Within a group, each partition is read by one consumer at a time.`);
  else if (n.active > 0 && P % n.active !== 0) note(m.id, `${P} partitions over ${n.active} consumers: some own ${Math.ceil(P / n.active)} partitions and some ${Math.floor(P / n.active)}, so the busiest ones set the pace.`);
  if (m.inputHot > Math.ceil(P / Math.max(1, n.active)) / P) note(m.id, `The hot partition goes to a single consumer, which caps the group at about ${fmtCount(n.capEff)} msg/s however many consumers you add.`);
  if (m.autoscaling && n.inst >= P && P < m.maxInst) note(m.id, `Autoscaling stops at ${P} consumers: more than the partition count would sit idle.`);
  if (n.storm > 0.5) {
    note(m.id, `A poll fetches ${fmtCount(m.maxPoll)} messages that take ${fmtDuration(m.batchMs / 1000)} to process, longer than max.poll.interval.ms (${fmtDuration(m.pollInterval / 1000)}). Kafka decides the consumer died and rebalances the group again and again.`);
  }
  if (m.opaque) {
    if (ctx.kafka.schemaBreak || ctx.kafka.poison || m.poison) note(m.id, 'It copies messages as raw bytes without reading them, so bad or re-shaped messages pass straight through.');
  } else if (ctx.kafka.schemaBreak) {
    note(m.id, ctx.schemaGuard
      ? 'The Schema Registry rejected an incompatible schema change, so this consumer never saw it.'
      : `Producers switched to an incompatible schema. This consumer cannot read the new messages${m.dlq ? ', so they all pile up in the dead letter topic' : ' and stops'}.`);
  } else if (m.poison || ctx.kafka.poison) {
    const owned = Math.round((n.stuck || 0) * P);
    note(m.id, m.dlq
      ? 'A message that kept failing was moved to the dead letter topic; everything behind it kept flowing.'
      : `A message that always fails blocks its partition. The consumer retries it forever, so ${owned > 1 ? `all ${owned} partitions it owns stop` : 'that partition stops'} moving.`);
  }
  if (group.behind > 0.001 && group.flow > 0) {
    note(m.id, Number.isFinite(group.age)
      ? `Falls behind by ${fmtCount(group.lag)} msg/s: after ${fmtDuration(group.window)} it is ${fmtCount(group.lag * group.window)} messages (${fmtDuration(group.age)}) behind.`
      : 'Nothing is being processed, so the lag grows with every message.');
  }
}

export function kafkaReport(ctx, st, ev, { windowSec, horizonSec }) {
  const topics = [...ctx.models.values()].filter((m) => m.type === 'KafkaTopic');
  if (!topics.length) return null;
  const notes = new Map();
  const note = (id, text) => {
    if (!notes.has(id)) notes.set(id, []);
    if (!notes.get(id).includes(text)) notes.get(id).push(text);
  };
  const guarantees = { loss: [], duplicates: [], order: [], consistency: [], stuck: [] };
  const flag = (kind, id, text) => {
    if (!guarantees[kind].some((g) => g.text === text)) guarantees[kind].push({ id, text });
  };
  const nodes = new Map();
  const noted = new Set();
  const rows = [];
  let lossRate = 0;
  let stuckRate = 0;
  let behindRate = 0;
  let freshness = 0;
  let lagRps = 0;

  for (const t of topics) {
    const tn = st.nodes.get(t.id);
    const lim = topicLimits(t);
    if (!t.down && tn.rho > 1 && !(t.key === 'skewed' && t.hot > 1.5 / t.partitions)) {
      note(t.id, lim.brokerLimit < lim.partitionLimit
        ? `${t.alive} brokers can write about ${fmtCount(lim.brokerLimit)} msg/s at replication factor ${t.rf}. Add brokers, or batch and compress more.`
        : `${t.partitions} partitions take about ${fmtCount(lim.partitionLimit)} msg/s. Add partitions (and brokers) to write faster.`);
    }
    const row = {
      id: t.id, label: t.label, partitions: t.partitions, brokers: t.brokers, brokers_alive: t.alive,
      replication_factor: t.rf, min_insync: t.minIsr, acks: t.acks, key: t.key,
      hot_share: round3(t.hot), write_fail: round3(t.writeFail), offline: round3(t.offline),
      inflow_rps: round1(tn.served), capacity_rps: round1(lim.capacity), retention_sec: t.retentionSec,
      groups: [],
    };
    for (const e of ctx.outs.get(t.id) || []) {
      const c = ctx.models.get(e.target);
      const cn = st.nodes.get(e.target);
      const flow = st.edgeFlow.get(e.id) || 0;
      const done = flow * (1 - cn.drop);
      const depOk = cn.drop < 1 ? clamp01((ev.S.get(c.id) ?? 1) / (1 - cn.drop)) : 1;
      const behind = flow > 0 ? clamp01((flow - done * depOk) / flow) : 0;
      const lag = flow * behind;
      const age = behind >= 0.999 ? Infinity : (behind * windowSec) / (1 - behind);
      const fresh = t.lat + (ev.L.get(c.id) || 0) + age * 1000;
      // Unread messages older than the retention time are deleted: they are lost for this group.
      const loss = t.compact ? 0 : Math.max(0, behind - t.retentionSec / horizonSec);
      const toLoss = behind > 0.0005 && !t.compact ? t.retentionSec / behind : Infinity;
      const group = { flow, lag, behind, age, window: windowSec };
      row.groups.push({
        id: c.id, label: c.label, consumers: cn.inst, active: cn.active ?? cn.inst,
        flow_rps: round1(flow), lag_rps: round1(lag), behind_share: round3(behind),
        freshness_ms: Number.isFinite(fresh) ? round1(fresh) : null,
        time_to_loss_sec: Number.isFinite(toLoss) ? Math.round(toLoss) : null,
        loss_share: round3(loss), stuck_share: round3(cn.stuck || 0), rebalancing: (cn.storm || 0) > 0.5,
      });
      if (flow > 0) {
        freshness = Math.max(freshness, fresh);
        behindRate = Math.max(behindRate, behind);
      }
      lossRate = Math.max(lossRate, loss);
      stuckRate = Math.max(stuckRate, cn.stuck || 0);
      lagRps += lag;
      if (loss > 0.0005) {
        flag('loss', c.id, `${c.label} fell so far behind ${t.label} that ${pct(Math.max(0.01, loss))} of messages passed the ${fmtDuration(t.retentionSec)} retention and were deleted unread.`);
        note(c.id, `Unread messages pass the ${fmtDuration(t.retentionSec)} retention and are deleted: ${pct(Math.max(0.01, loss))} of the stream is lost.`);
      } else if (Number.isFinite(toLoss) && toLoss < 7 * 86400) {
        note(c.id, `At this pace unread messages start expiring after ${fmtDuration(toLoss)} (the topic keeps them for ${fmtDuration(t.retentionSec)}).`);
      }
      if (cn.stuck > 0) {
        flag('stuck', c.id, ctx.kafka.schemaBreak
          ? `${c.label} cannot read messages in the new, incompatible schema, so it stops processing ${t.label}.`
          : `A message ${c.label} can never process blocks ${pct(cn.stuck)} of ${t.label}, and nothing behind it moves.`);
      }
      if (!noted.has(c.id)) {
        noted.add(c.id);
        consumerNotes(c, cn, group, note, ctx);
        nodes.set(c.id, {
          role: 'consumer', partitions: c.inputPartitions, consumers: cn.inst, active: cn.active ?? cn.inst,
          idle: Math.max(0, cn.inst - c.inputPartitions), lag_rps: round1(lag), behind_share: round3(behind),
          freshness_ms: Number.isFinite(fresh) ? round1(fresh) : null, stuck_share: round3(cn.stuck || 0),
          rebalancing: (cn.storm || 0) > 0.5, commit: c.commit, idempotent: !!c.idempotent, dlq: !!c.dlq,
        });
      }
    }
    nodes.set(t.id, {
      role: 'topic', partitions: t.partitions, brokers: t.brokers, brokers_alive: t.alive, replication_factor: t.rf,
      min_insync: t.minIsr, acks: t.acks, key: t.key, hot_share: round3(t.hot), write_fail: round3(t.writeFail),
      offline: round3(t.offline), groups: row.groups.length,
    });
    rows.push(row);
  }

  // A broker failure shows which acknowledged writes can vanish and which retries duplicate.
  for (const t of topics) {
    if (!(t.lost > 0)) continue;
    if (t.rf < 2) flag('loss', t.id, `${t.label} keeps one copy of each partition (replication factor 1), so the partitions on the failed broker lost their data.`);
    else if (t.acks !== 'all') flag('loss', t.id, `${t.label} uses acks=${t.acks}: the leader confirms writes before followers copy them, so recent writes on the failed leader were lost.`);
    else if (t.minIsr < 2) flag('loss', t.id, `${t.label} allows min.insync.replicas=1, so a leader can confirm writes alone; they are lost if it fails before followers catch up.`);
    if (!t.idempotentProducer && t.acks !== '0') flag('duplicates', t.id, `Producers to ${t.label} retried writes whose acknowledgement was lost in the failover. Without an idempotent producer, each retry wrote the message again.`);
  }
  // Crashes and rebalances: the commit mode decides whether in-flight messages are skipped or replayed.
  for (const m of ctx.models.values()) {
    if (!m.inputTopics || !(m.crashed || (ctx.storm.get(m.id) || 0) > 0.5)) continue;
    if (m.commit === 'before_processing') flag('loss', m.id, `${m.label} commits offsets before processing, so messages it was working on when it stopped are skipped.`);
    else if (!m.idempotent) flag('duplicates', m.id, `${m.label} commits after processing (at-least-once). After a crash or rebalance, messages since the last commit are processed again, and processing is not idempotent.`);
  }
  // Ordering is a property of the design, so it is checked in every scenario.
  for (const t of topics) {
    if (t.key === 'none') flag('order', t.id, `${t.label} has no message key, so events for the same entity land on different partitions and can be processed out of order.`);
    if (!t.idempotentProducer) flag('order', t.id, `Producers to ${t.label} are not idempotent, so a retried batch can land after the batch behind it.`);
  }
  for (const m of ctx.models.values()) {
    if (m.inputTopics || m.type === 'User') continue;
    const outs = ctx.outs.get(m.id) || [];
    const db = outs.find((e) => ctx.models.get(e.target).type === 'Database' && !ctx.isAsyncEdge(e));
    const topic = outs.find((e) => ctx.models.get(e.target).type === 'KafkaTopic');
    if (db && topic) {
      flag('consistency', m.id, `${m.label} writes to ${ctx.models.get(db.target).label} and publishes to ${ctx.models.get(topic.target).label} as two separate steps. If one fails, they disagree for good. Use the outbox pattern: save the event in the same database transaction and stream it out with CDC.`);
    }
  }

  return {
    topics: rows,
    data_loss_rate: round3(lossRate),
    stuck_rate: round3(stuckRate),
    behind_rate: round3(behindRate),
    freshness_ms: Number.isFinite(freshness) ? round1(freshness) : null,
    lag_rps: round1(lagRps),
    guarantees,
    notes,
    nodes,
  };
}

export function guaranteeViolations(sim, requires = []) {
  const g = sim?.kafka?.guarantees;
  if (!g || !requires?.length) return [];
  return requires.flatMap((r) => (g[REQUIREMENTS[r]] || []).map((x) => ({ requirement: r, id: x.id, text: x.text })));
}

export const REQUIREMENT_LABELS = {
  'no-loss': 'No message may be lost',
  'no-duplicates': 'No message may be processed twice',
  ordered: 'Events for one entity stay in order',
  consistent: 'The database and the events never disagree',
  'no-stuck': 'Bad messages never block the stream',
};

// ================================================================ scoring
// Turns simulation metrics into 0–100 scores. A scenario passes at 60.

export const PASS_SCORE = 60;

function interp(x, points) {
  if (x <= points[0][0]) return points[0][1];
  for (let i = 1; i < points.length; i++) {
    const [x1, y1] = points[i];
    const [x0, y0] = points[i - 1];
    if (x <= x1) return y0 + ((x - x0) / (x1 - x0)) * (y1 - y0);
  }
  return points[points.length - 1][1];
}

const SPEED = [[0, 100], [1, 100], [1.5, 75], [2, 55], [4, 20], [8, 0]];

// Messages that consumers never process (lost to retention, or stuck behind a bad message)
// count as failures, just like rejected requests. With a freshness target, consumers must
// also keep up: messages they fall behind on count as unprocessed, and the age of the newest
// data they have handled is held to the target like latency.
export function scoreSimulation(sim, { latencyTarget = 300, freshnessTarget = 0 } = {}) {
  const k = sim.kafka;
  const behind = k ? (freshnessTarget > 0 ? Math.max(k.stuck_rate || 0, k.behind_rate || 0) : k.stuck_rate || 0) : 0;
  const unprocessed = k ? Math.min(1, (k.data_loss_rate || 0) + behind) : 0;
  const failed = 1 - (1 - sim.error_rate) * (1 - unprocessed);
  const availability = interp(failed, [[0, 100], [0.005, 92], [0.01, 85], [0.05, 55], [0.15, 20], [0.3, 0]]);
  const ratio = latencyTarget > 0 ? sim.latency_ms.p95 / latencyTarget : 0;
  const latency = interp(ratio, SPEED);
  const freshness = freshnessTarget > 0 && k
    ? interp(k.freshness_ms === null ? Infinity : k.freshness_ms / freshnessTarget, SPEED)
    : null;
  const headroom = interp(sim.max_utilization, [[0, 100], [0.7, 100], [0.85, 85], [1, 60], [1.2, 30], [2, 0]]);
  const speed = freshness === null ? latency : Math.min(latency, freshness);
  // Speed and headroom only count for requests that were actually served.
  let total = 0.55 * availability + (0.3 * speed + 0.15 * headroom) * (1 - Math.min(1, failed));
  // A design that loses most requests cannot pass on the strength of its latency alone.
  if (failed > 0.2) total = Math.min(total, 45);
  total = Math.round(total);
  const parts = { availability: Math.round(availability), latency: Math.round(latency), headroom: Math.round(headroom) };
  if (freshness !== null) parts.freshness = Math.round(freshness);
  return { total, pass: total >= PASS_SCORE, parts };
}

export function gradeFor(score) {
  if (score >= 90) return 'A';
  if (score >= 75) return 'B';
  if (score >= 60) return 'C';
  if (score >= 40) return 'D';
  return 'F';
}

export function combineSubmission({ testScore, ruleScore, securityScore }) {
  const total = Math.round(0.65 * testScore + 0.2 * ruleScore + 0.15 * securityScore);
  return { total, grade: gradeFor(total), weights: { tests: 0.65, design: 0.2, security: 0.15 } };
}

// ================================================================ traffic simulator
// Capacity-and-flow model. Every component has a capacity (requests/s per instance) and a
// base latency. Traffic enters at User nodes and is split along edges; demand above a
// component's capacity is dropped, latency inflates as utilization approaches 1, and a few
// feedback loops (retries, thread exhaustion from slow dependencies, queue overflow, cache
// misses, rebalance storms) are resolved by iterating passes until they settle.

const ROUTERS = new Set(['LoadBalancer', 'APIGateway', 'ReverseProxy', 'RateLimiter']);
const CALLERS = new Set(['Server', 'Service', 'Worker', 'WebSocketServer', ...KAFKA_CONSUMERS]);
const SCALABLE = new Set(['LoadBalancer', 'ReverseProxy', 'RateLimiter', 'APIGateway', 'Server', 'Service', 'WebSocketServer', 'Cache', 'Worker', ...KAFKA_CONSUMERS, 'SchemaRegistry']);
const AUTOSCALABLE = new Set(['Server', 'Service', 'WebSocketServer', 'Worker', 'ConsumerGroup', 'StreamProcessor']);
const WINDOW_SEC = 600;
const STAMPEDE = 1.5;
const MAX_PASSES = 10;
// Thread pools are sized for ~4× the steady-state request time, so callers only lose
// capacity once a slow dependency stretches request time beyond that headroom.
const THREAD_HEADROOM = 4;
const AUTOSCALE_TARGET = 0.65;

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

function role(type) {
  if (type === 'User') return 'source';
  if (ROUTERS.has(type)) return 'router';
  if (type === 'CDN') return 'cdn';
  if (type === 'Cache') return 'cache';
  if (type === 'Queue') return 'queue';
  if (type === 'KafkaTopic') return 'topic';
  return 'caller';
}

export function queueFactor(rho) {
  if (!Number.isFinite(rho) || rho >= 1) return 25;
  const r = Math.min(rho, 0.98);
  return Math.min(25, 1 + (0.5 * r ** 8) / (1 - r));
}

function buildModel(node, ov) {
  const p = (k) => param(node, k);
  const t = node.type;
  const m = {
    id: node.id, type: t, label: node.label,
    cap: Number(p('capacity')) || 0,
    lat: Number(p('base_latency')) || 0,
    replicas: 1, autoscaling: false, minInst: 1, maxInst: 1, instDelta: 0,
    down: !!ov.down, latMult: ov.latencyMult ?? 1,
    crashed: !!ov.down || (ov.instancesDelta ?? 0) < 0, poison: !!ov.poison,
    // A live run passes the messages a consumer is behind; it then works flat out to catch up.
    backlog: Math.max(0, Number(ov.lag) || 0),
    timeout: 0, retries: 0, breaker: false,
    notes: [],
  };
  if (t === 'User') {
    m.cap = Infinity;
    return m;
  }
  if (SCALABLE.has(t)) m.replicas = Math.max(1, Math.round(Number(p('replicas')) || 1));
  if (AUTOSCALABLE.has(t) && p('autoscaling')) {
    m.autoscaling = true;
    m.minInst = Math.max(1, Math.round(p('autoscale_min')));
    m.maxInst = Math.max(m.minInst, Math.round(p('autoscale_max')));
    // A live run passes the instances actually running, since new ones take time to start.
    m.fixedInst = Number.isFinite(ov.instances) ? Math.max(0, Math.round(ov.instances)) : null;
  }
  if (t === 'Worker') {
    m.cap *= Math.max(1, Number(p('concurrency')) || 8) / 8;
    m.idempotent = !!p('idempotent');
  }
  if (CALLERS.has(t)) {
    m.timeout = Number(p('timeout_ms')) || 0;
    m.retries = Math.max(0, Math.round(Number(p('retry_limit')) || 0));
    m.breaker = !!p('circuit_breaker');
  }
  if (KAFKA_CONSUMERS.has(t)) consumerModel(m, p);

  switch (t) {
    case 'LoadBalancer':
      m.algorithm = p('algorithm');
      m.healthChecks = !!p('health_checks');
      m.sticky = !!p('sticky_sessions');
      break;
    case 'APIGateway':
      m.rateLimiting = !!p('rate_limiting');
      m.healthChecks = true;
      break;
    case 'ReverseProxy':
      m.healthChecks = true;
      break;
    case 'RateLimiter':
      m.limit = Number(p('limit_per_client_rps')) || 1;
      m.burst = Number(p('burst_multiplier')) || 1;
      m.scope = p('scope');
      m.store = p('store');
      m.healthChecks = true;
      break;
    case 'CDN':
      m.hit = clamp01(p('cache_hit_ratio') * (ov.hitMult ?? 1));
      break;
    case 'Cache': {
      const ttl = Number(p('ttl_seconds')) || 1;
      const ttlFactor = Math.min(1, ttl / 30);
      m.hit = clamp01(p('hit_ratio') * ttlFactor * (ov.hitMult ?? 1));
      m.coalescing = !!p('request_coalescing');
      if (ttlFactor < 1) m.notes.push(`A ${ttl} s TTL expires hot keys before reuse, so the real hit ratio is only ${pct(m.hit)}.`);
      m.disrupted = !!(ov.down || ov.cold || (ov.instancesDelta ?? 0) < 0 || (ov.hitMult !== undefined && ov.hitMult < 1));
      break;
    }
    case 'Database':
      m.readRatio = clamp01(p('read_ratio'));
      m.readReplicas = Math.max(0, Math.round(Number(p('read_replicas')) || 0));
      m.shards = Math.max(1, Math.round(Number(p('shards')) || 1));
      m.failover = !!p('failover');
      m.readMult = ov.readMult ?? 1;
      m.writeMult = ov.writeMult ?? 1;
      if (!p('indexed')) {
        m.cap /= 4;
        m.lat *= 5;
        m.notes.push('Queries scan whole tables because indexes are off.');
      }
      if (p('replication') === 'sync' && m.readReplicas > 0) m.lat *= 1.3;
      break;
    case 'SearchIndex':
      m.replicas = Math.max(1, Math.round(Number(p('shards')) || 1)) * (1 + Math.max(0, Math.round(Number(p('replicas')) || 0)));
      break;
    case 'BlobStorage':
      if (p('storage_class') === 'archive') m.lat *= 50;
      else if (p('storage_class') === 'infrequent') m.lat *= 1.5;
      break;
    case 'Queue':
      m.maxDepth = Math.max(1, Number(p('max_depth')) || 1);
      m.fifo = !!p('fifo');
      m.partitions = Math.max(1, Math.round(Number(p('partitions')) || 1));
      // A live run passes the messages waiting now; the queue only rejects work once it is full.
      m.liveDepth = Number.isFinite(ov.depth) ? Math.max(0, ov.depth) : null;
      break;
    case 'WebSocketServer':
      m.maxConn = Math.max(1, Number(p('max_connections')) || 1);
      break;
    case 'ThirdPartyAPI':
      m.timeout = Number(p('timeout_ms')) || 0;
      m.retries = Math.max(0, Math.round(Number(p('retry_limit')) || 0));
      m.breaker = !!p('circuit_breaker');
      m.errorRate = clamp01(p('error_rate'));
      m.provider = p('provider');
      break;
    default:
      break;
  }

  m.cap *= ov.capacityMult ?? 1;

  const lost = Math.max(0, -(ov.instancesDelta ?? 0));
  if (t === 'KafkaTopic') {
    topicModel(m, p, ov);
  } else if (t === 'Database') {
    if (m.down || lost > 0) {
      if (m.readReplicas > 0 && m.failover) {
        m.readReplicas -= 1;
        m.down = false;
        m.notes.push('The primary failed and a replica was promoted automatically.');
      } else if (m.readReplicas > 0) {
        m.down = false;
        m.primaryDown = true;
        m.notes.push('The primary failed and nothing promoted a replica: reads continue, every write fails.');
      } else {
        m.down = true;
        m.notes.push('The only copy of the database is down.');
      }
    }
  } else if (lost > 0) {
    const before = m.replicas;
    m.lostInst = lost;
    m.instBefore = m.autoscaling ? Math.max(m.minInst, lost + 1) : before;
    if (m.autoscaling) {
      m.instDelta = -lost;
      m.notes.push('Lost an instance; the autoscaler is still replacing it.');
    } else {
      m.replicas = Math.max(0, m.replicas - lost);
      if (m.replicas === 0) m.down = true;
      else m.notes.push(`Lost ${lost} of ${before} instances.`);
    }
    if (t === 'Cache' && !m.down) {
      m.hit *= 1 - lost / before;
      m.notes.push(`Keys owned by the lost node miss until they are refilled (hit ratio now ${pct(m.hit)}).`);
    }
  }
  if (t === 'Cache' && m.down) m.hit = 0;
  m.surge = t === 'Cache' && m.disrupted && !m.coalescing ? STAMPEDE : 1;
  if (m.down && t !== 'Database') m.notes.push('This component is down.');
  return m;
}

function resolveEntry(idx, traffic = {}) {
  const users = idx.users().map((u) => ({
    id: u.id,
    users: Math.max(0, Number(param(u, 'users')) || 0),
    rpu: Math.max(0, Number(param(u, 'requests_per_user')) || 0),
  }));
  const baseTotal = users.reduce((s, u) => s + u.users * u.rpu, 0);
  const baseUsers = users.reduce((s, u) => s + u.users, 0);
  const mult = Number(traffic.multiplier) > 0 ? Number(traffic.multiplier) : 1;
  let total = baseTotal * mult;
  let totalUsers = baseUsers * mult;
  if (Number(traffic.requests_per_second) > 0) {
    total = Number(traffic.requests_per_second) * mult;
    if (baseTotal > 0) totalUsers = baseUsers * (total / baseTotal);
  }
  if (Number(traffic.users) > 0) {
    const avgRpu = baseUsers > 0 ? baseTotal / baseUsers : 1;
    const rpu = Number(traffic.requests_per_user) > 0 ? Number(traffic.requests_per_user) : avgRpu;
    totalUsers = Number(traffic.users) * mult;
    total = totalUsers * rpu;
  }
  const abusiveShare = clamp(Number(traffic.abusive_share) || 0, 0, 0.99);
  const entry = new Map();
  for (const u of users) {
    const share = baseTotal > 0 ? (u.users * u.rpu) / baseTotal : 1 / users.length;
    const rps = total * share;
    entry.set(u.id, { rps, abusive: rps * abusiveShare });
  }
  const legit = total * (1 - abusiveShare);
  return {
    entry,
    totalRps: total,
    legitRps: legit,
    totalUsers,
    rpuLegit: totalUsers > 0 ? legit / totalUsers : 0,
    asyncMult: Number(traffic.async_multiplier) > 0 ? Number(traffic.async_multiplier) : 1,
  };
}

function makeContext(graph, options) {
  const idx = new GraphIndex(graph);
  const { order, back } = idx.topo();
  const ov = options.overrides || {};
  const models = new Map(graph.nodes.map((n) => [n.id, buildModel(n, ov[n.id] || {})]));
  const outs = new Map(graph.nodes.map((n) => [n.id, idx.out(n.id).filter((e) => !back.has(e.id))]));
  const reachCache = new Map();
  const traffic = resolveEntry(idx, options.traffic);
  linkConsumers(models, graph.edges, back, WINDOW_SEC);
  const kafka = options.kafka || {};
  const ctx = {
    idx, order, back, models, outs, ...traffic,
    retryMult: new Map(), slow: new Map(), queueOverflow: new Map(), cacheDrop: new Map(), storm: new Map(),
    kafka: { poison: !!kafka.poison_pill, schemaBreak: !!kafka.schema_break },
    schemaGuard: idx.ofType('SchemaRegistry').some((r) => param(r, 'compatibility') !== 'none' && idx.in(r.id).length > 0),
    reach(a, b) {
      if (!reachCache.has(a)) reachCache.set(a, idx.reachableFrom([a], { syncOnly: true }));
      return reachCache.get(a).has(b);
    },
    // Change data capture reads the database log, so it never slows down or fails a write.
    isAsyncEdge(e) {
      if (e.kind === 'async') return true;
      const target = models.get(e.target)?.type;
      return target === 'Queue' || target === 'KafkaTopic' || (target === 'KafkaConnect' && models.get(e.source)?.type === 'Database');
    },
  };
  return ctx;
}

function routeWeights(m, edges, ctx, { healthChecks = m.healthChecks } = {}) {
  let targets = edges;
  if (healthChecks) {
    const healthy = edges.filter((e) => !ctx.models.get(e.target).down);
    if (healthy.length) targets = healthy;
  }
  if (!targets.length) return [];
  const withRatio = targets.filter((e) => e.ratio !== null && e.ratio !== undefined);
  let w;
  if (withRatio.length) {
    const given = withRatio.reduce((s, e) => s + e.ratio, 0);
    const rest = targets.length - withRatio.length;
    if (given >= 1 || rest === 0) w = targets.map((e) => (e.ratio ?? 0) / (given || 1));
    else w = targets.map((e) => (e.ratio !== null && e.ratio !== undefined ? e.ratio : (1 - given) / rest));
  } else if (m.type === 'LoadBalancer' && m.algorithm === 'least_connections') {
    const caps = targets.map((e) => {
      const c = ctx.models.get(e.target);
      return c.down ? 0 : (c.autoscaling ? c.maxInst : c.replicas) * (Number.isFinite(c.cap) ? c.cap : 1);
    });
    const total = caps.reduce((a, b) => a + b, 0) || 1;
    w = caps.map((c) => c / total);
  } else {
    w = targets.map(() => 1 / targets.length);
  }
  if (m.type === 'LoadBalancer' && m.sticky && targets.length > 1) {
    w[0] *= 1.35;
    const s = w.reduce((a, b) => a + b, 0);
    w = w.map((x) => x / s);
  }
  return targets.map((e, i) => ({ edge: e, w: w[i] }));
}

function hitNow(cacheModel, ctx) {
  return cacheModel.hit * (1 - (ctx.cacheDrop.get(cacheModel.id) || 0));
}

// Share of traffic that still reaches `target` after passing a cache. Writes skip the cache.
function missShare(cacheModel, target, ctx, { withSurge = true, readShare = null } = {}) {
  const miss = 1 - hitNow(cacheModel, ctx);
  const surge = withSurge ? cacheModel.surge : 1;
  if (target && target.type === 'Database') {
    const rr = readShare ?? target.readRatio;
    return rr * miss * surge + (1 - rr);
  }
  return miss * surge;
}

function splitEdges(id, ctx) {
  const edges = ctx.outs.get(id) || [];
  return {
    sync: edges.filter((e) => !ctx.isAsyncEdge(e)),
    async: edges.filter((e) => ctx.isAsyncEdge(e)),
  };
}

// Share of a caller's requests that hit a Database edge, given caches the caller also uses.
function dbShare(edge, target, caches, ctx, withSurge) {
  const rr = edge.read_share ?? target.readRatio;
  if (!caches.length) return 1;
  if (caches.some((c) => ctx.reach(c.target, edge.target))) return 1 - rr;
  return Math.min(...caches.map((c) => missShare(ctx.models.get(c.target), target, ctx, { withSurge, readShare: edge.read_share })));
}

// Read/write split of traffic arriving at a database. Caches absorb reads, never writes,
// so a cached workload reaches the database much more write-heavy than it started.
// A connection can override the database's read share (e.g. a worker that only writes).
function toDatabase(target, base, ctx, { cache = null, writesOnly = false, readShare = null } = {}) {
  const rr = readShare ?? target.readRatio;
  if (writesOnly) return { reads: 0, writes: base * (1 - rr) };
  if (cache) return { reads: base * rr * (1 - hitNow(cache, ctx)) * cache.surge, writes: base * (1 - rr) };
  return { reads: base * rr, writes: base * (1 - rr) };
}

function share(edge, flow, ctx, opts = {}) {
  const target = ctx.models.get(edge.target);
  if (target.type !== 'Database') return { edge, flow };
  const rw = toDatabase(target, flow, ctx, { ...opts, readShare: edge.read_share ?? null });
  return { edge, flow: rw.reads + rw.writes, rw };
}

function distribute(m, served, ctx) {
  const out = [];
  if (served <= 0) return out;
  const { sync, async: asyncEdges } = splitEdges(m.id, ctx);
  const writeShare = m.type === 'Database' ? 1 - (m.lastReadShare ?? m.readRatio) : 1;
  const pushAsync = (base) => {
    for (const e of asyncEdges) {
      const cdc = m.type === 'Database' && ctx.models.get(e.target).type === 'KafkaConnect';
      out.push(share(e, base * (cdc ? writeShare : 1) * (e.ratio ?? 1) * ctx.asyncMult, ctx));
    }
  };
  switch (role(m.type)) {
    case 'source':
    case 'router':
      for (const { edge, w } of routeWeights(m, sync, ctx)) out.push(share(edge, served * w, ctx));
      pushAsync(served);
      break;
    case 'cdn': {
      const miss = 1 - m.hit;
      for (const { edge, w } of routeWeights(m, sync, ctx, { healthChecks: true })) out.push(share(edge, served * miss * w, ctx));
      pushAsync(served * miss);
      break;
    }
    case 'cache':
      for (const { edge, w } of routeWeights(m, sync, ctx, { healthChecks: false })) {
        const target = ctx.models.get(edge.target);
        out.push(target.type === 'Database'
          ? share(edge, served * w, ctx, { cache: m })
          : { edge, flow: served * missShare(m, target, ctx) * w });
      }
      pushAsync(served);
      break;
    case 'queue': {
      const consumers = ctx.outs.get(m.id) || [];
      for (const { edge, w } of routeWeights(m, consumers, ctx, { healthChecks: true })) out.push(share(edge, served * w, ctx));
      break;
    }
    case 'topic':
      // Every consumer group reads the whole stream.
      for (const e of ctx.outs.get(m.id) || []) out.push(share(e, served * (e.ratio ?? 1), ctx));
      break;
    default: {
      const caches = sync.filter((e) => ctx.models.get(e.target).type === 'Cache');
      for (const e of sync) {
        const target = ctx.models.get(e.target);
        const base = served * (e.ratio ?? 1) * (target.type === 'SchemaRegistry' ? SCHEMA_LOOKUP : 1);
        if (target.type !== 'Database' || !caches.length) {
          out.push(share(e, base, ctx));
        } else if (caches.some((c) => ctx.reach(c.target, e.target))) {
          out.push(share(e, base, ctx, { writesOnly: true }));
        } else {
          const best = caches.map((c) => ctx.models.get(c.target)).sort((a, b) => hitNow(b, ctx) - hitNow(a, ctx))[0];
          out.push(share(e, base, ctx, { cache: best }));
        }
      }
      pushAsync(served);
    }
  }
  return out;
}

// How many instances the autoscaler would like for this demand.
function wantedInstances(m, d, ctx) {
  let want = Math.ceil(d / Math.max(1e-9, m.cap * AUTOSCALE_TARGET));
  if (m.type === 'WebSocketServer') {
    const conns = ctx.totalRps > 0 ? (d / ctx.totalRps) * ctx.totalUsers : 0;
    want = Math.max(want, Math.ceil(conns / (m.maxConn * AUTOSCALE_TARGET)));
  }
  // Consumers beyond the partition count would have nothing to read, and the consumer with
  // the most partitions sets the pace, so keep adding until the busiest one copes.
  if (m.inputPartitions) {
    const P = m.inputPartitions;
    want = Math.min(want, P);
    while (want < P && (m.cap * P * AUTOSCALE_TARGET) / Math.ceil(P / want) < d) want++;
  }
  return clamp(want, m.minInst, m.maxInst);
}

function instancesFor(m, d, ctx) {
  if (m.down) return 0;
  let inst = m.replicas;
  if (m.autoscaling) inst = (m.fixedInst ?? wantedInstances(m, d, ctx)) + m.instDelta;
  return Math.max(0, inst);
}

function computeLoad(m, d, ctx, rw = null) {
  if (m.type === 'User') return { inst: 1, capEff: Infinity, rho: 0, drop: 0 };
  if (m.type === 'KafkaTopic') {
    const capEff = topicLimits(m).capacity;
    const rho = capEff > 0 ? d / capEff : d > 0 ? Infinity : 0;
    return { inst: m.alive, capEff, rho, drop: !Number.isFinite(rho) ? (d > 0 ? 1 : 0) : rho > 1 ? 1 - 1 / rho : 0 };
  }
  const inst = instancesFor(m, d, ctx);
  const slow = CALLERS.has(m.type) ? ctx.slow.get(m.id) ?? 1 : 1;
  if (m.inputPartitions) return { inst, slow, ...consumerLoad(m, d, inst, slow, ctx) };
  let capEff = inst * m.cap * slow;
  let rho;
  if (m.type === 'Database') {
    const R = (rw ? rw.reads : d * m.readRatio) * m.readMult;
    const W = (rw ? rw.writes : d * (1 - m.readRatio)) * m.writeMult;
    m.lastReadShare = R + W > 0 ? R / (R + W) : m.readRatio;
    if (m.down) rho = d > 0 ? Infinity : 0;
    else if (m.primaryDown) rho = R / (m.shards * m.readReplicas) / m.cap;
    else rho = (W / m.shards + R / (m.shards * (1 + m.readReplicas))) / m.cap;
    capEff = m.down ? 0 : rho > 0 ? d / rho : m.cap * m.shards * (1 + m.readReplicas);
  } else if (m.type === 'WebSocketServer') {
    const conns = ctx.totalRps > 0 ? (d / ctx.totalRps) * ctx.totalUsers : 0;
    m.connections = conns;
    rho = capEff > 0 ? Math.max(d / capEff, conns / (m.maxConn * inst)) : d > 0 ? Infinity : 0;
  } else {
    rho = capEff > 0 ? d / capEff : d > 0 ? Infinity : 0;
  }
  const drop = !Number.isFinite(rho) ? (d > 0 ? 1 : 0) : rho > 1 ? 1 - 1 / rho : 0;
  return { inst, capEff, rho, drop, slow };
}

function limiterEfficiency(m) {
  if (m.type === 'APIGateway') return 0.9;
  if (m.scope === 'global') return 0;
  return m.store === 'in_memory' && m.replicas > 1 ? 0.6 : 0.95;
}

function flowPass(ctx) {
  const demand = new Map();
  const abusive = new Map();
  const edgeFlow = new Map();
  const reads = new Map();
  const writes = new Map();
  const add = (map, k, v) => map.set(k, (map.get(k) || 0) + v);
  for (const [uid, e] of ctx.entry) {
    add(demand, uid, e.rps);
    add(abusive, uid, e.abusive);
  }
  const nodes = new Map();
  for (const id of ctx.order) {
    const m = ctx.models.get(id);
    const d = demand.get(id) || 0;
    const ab = Math.min(d, abusive.get(id) || 0);
    const rw = m.type === 'Database' ? { reads: reads.get(id) || 0, writes: writes.get(id) || 0 } : null;
    const l = computeLoad(m, d, ctx, rw);
    let served = d * (1 - l.drop);
    if (m.backlog > 0 && l.full > 0 && l.capEff > served) {
      served = Math.min(l.capEff, d + m.backlog);
      l.rho = Math.max(l.rho, served / l.full);
    }
    let abServed = ab * (1 - l.drop);
    let blocked = 0;
    if (m.type === 'RateLimiter' || (m.type === 'APIGateway' && m.rateLimiting)) {
      blocked = abServed * limiterEfficiency(m);
      served -= blocked;
      abServed -= blocked;
    }
    if (m.type === 'Queue') served *= 1 - (ctx.queueOverflow.get(id) || 0);
    if (m.type === 'KafkaTopic') served *= 1 - m.writeFail;
    nodes.set(id, { ...l, d, served, blocked });
    for (const s of distribute(m, served, ctx)) {
      const mult = ctx.retryMult.get(s.edge.id) || 1;
      const flow = s.flow * mult;
      add(demand, s.edge.target, flow);
      if (s.rw) {
        add(reads, s.edge.target, s.rw.reads * mult);
        add(writes, s.edge.target, s.rw.writes * mult);
      }
      add(abusive, s.edge.target, served > 0 ? (abServed / served) * flow : 0);
      edgeFlow.set(s.edge.id, (edgeFlow.get(s.edge.id) || 0) + flow);
    }
  }
  return { nodes, edgeFlow };
}

// How a caller experiences one dependency once timeouts, circuit breakers and retries apply.
function edgeView(m, e, ctx, L, S, nominal) {
  const c = ctx.models.get(e.target);
  const Lc = L.get(e.target) ?? 0;
  const Sc = S.get(e.target) ?? 1;
  if (nominal) return { L: Lc, S: 1, attempts: 1, tripped: false };
  const thirdParty = c.type === 'ThirdPartyAPI';
  const timeout = thirdParty && c.timeout > 0 ? c.timeout : m.timeout || 0;
  const retries = thirdParty ? c.retries : m.retries || 0;
  const breaker = !!(m.breaker || (thirdParty && c.breaker));
  const late = timeout > 0 && Lc > timeout ? clamp((Lc - timeout) / Lc, 0, 0.99) : 0;
  if (breaker && (1 - Sc > 0.25 || late > 0.25)) {
    // An open breaker fails doomed calls immediately and never retries them.
    const s = Sc * (1 - late);
    const okL = timeout > 0 ? Math.min(Lc, timeout) : Lc;
    return { L: s * okL + (1 - s) * 1, S: s, attempts: 1, tripped: true };
  }
  let l = Lc;
  let s = Sc;
  if (late > 0) {
    s = Sc * (1 - late);
    l = timeout;
  }
  let attempts = 1;
  if (retries > 0) {
    const f = 1 - s;
    attempts = 0;
    for (let k = 0; k <= retries; k++) attempts += f ** k;
    s = 1 - f ** (retries + 1);
    l *= attempts;
  }
  return { L: l, S: s, attempts, tripped: false };
}

function evaluate(ctx, st, { nominal = false } = {}) {
  const L = new Map();
  const S = new Map();
  const seen = new Map();
  const downSeen = new Map();
  for (const id of [...ctx.order].reverse()) {
    const m = ctx.models.get(id);
    const n = st.nodes.get(id);
    const r = role(m.type);
    const rho = nominal ? 0 : n.rho;
    const drop = nominal ? 0 : n.drop;
    const own = m.type === 'User' || (!nominal && m.down) ? 0 : m.lat * queueFactor(rho) * (nominal ? 1 : m.latMult);
    const { sync, async: asyncEdges } = splitEdges(id, ctx);
    let down = 0;
    let succ = 1;

    if (r === 'source' || r === 'router' || r === 'cdn' || r === 'cache') {
      const hc = r === 'cache' ? false : r === 'cdn' ? true : m.healthChecks;
      const routes = routeWeights(m, sync, ctx, { healthChecks: hc });
      let dl = 0;
      let ds = 0;
      for (const { edge, w } of routes) {
        const view = edgeView(m, edge, ctx, L, S, nominal);
        seen.set(edge.id, view);
        dl += w * view.L;
        ds += w * view.S;
      }
      if (r === 'cdn') {
        const miss = 1 - m.hit;
        down = miss * dl;
        succ = routes.length ? 1 - miss + miss * ds : 1 - miss;
      } else if (r === 'cache') {
        const hit = nominal ? m.hit : hitNow(m, ctx);
        down = routes.length ? (1 - hit) * dl : 0;
        succ = routes.length ? hit + (1 - hit) * ds : 1;
      } else {
        down = dl;
        succ = routes.length ? ds : m.type === 'User' && asyncEdges.length ? 1 : 0;
      }
    } else if (r === 'caller') {
      const caches = sync.filter((e) => ctx.models.get(e.target).type === 'Cache');
      for (const e of sync) {
        const target = ctx.models.get(e.target);
        const view = edgeView(m, e, ctx, L, S, nominal);
        seen.set(e.id, view);
        const registry = target.type === 'SchemaRegistry';
        let p = (e.ratio ?? 1) * (registry ? SCHEMA_LOOKUP : 1);
        // A cache-aside lookup that fails just becomes a miss; a read-through cache forwards
        // misses itself, so its success already includes the store behind it. Optional
        // dependencies (and schema lookups, which clients cache) cost latency but the
        // request succeeds without them.
        if (e.optional || registry || (target.type === 'Cache' && !splitEdges(e.target, ctx).sync.length)) {
          down += p * view.L;
          continue;
        }
        if (target.type === 'Database') p *= dbShare(e, target, caches, ctx, false);
        down += p * view.L;
        succ *= p >= 1 ? view.S ** p : 1 - p + p * view.S;
      }
      for (const e of asyncEdges) {
        const q = ctx.models.get(e.target);
        if (m.type === 'Database' && q.type === 'KafkaConnect') continue;
        const qn = st.nodes.get(e.target);
        const p = Math.min(1, e.ratio ?? 1);
        const rejected = q.type === 'KafkaTopic' ? q.writeFail : 0;
        const ok = nominal ? 1 : (q.down ? 0 : 1 - qn.drop) * (1 - (ctx.queueOverflow.get(e.target) || 0)) * (1 - rejected);
        down += p * (q.lat || 0);
        succ *= 1 - p + p * ok;
      }
    }

    let self = 1 - drop;
    if (!nominal) {
      if (m.down) self = 0;
      if (m.type === 'KafkaTopic') self *= 1 - m.writeFail;
      if (m.type === 'ThirdPartyAPI') self *= 1 - m.errorRate;
      if (m.type === 'Database' && m.primaryDown) self *= m.lastReadShare ?? m.readRatio;
      if (m.type === 'RateLimiter') {
        const allowed = m.limit * m.burst;
        self *= ctx.rpuLegit > allowed ? allowed / ctx.rpuLegit : 1;
      }
    }
    if (m.type === 'Cache' && !sync.length) self = 1;
    downSeen.set(id, down);
    L.set(id, own + down);
    S.set(id, clamp01(self * succ));
  }
  return { L, S, seen, downSeen };
}

function queueStats(ctx, st) {
  const stats = new Map();
  for (const id of ctx.order) {
    const m = ctx.models.get(id);
    if (m.type !== 'Queue') continue;
    const n = st.nodes.get(id);
    const inflow = m.down ? 0 : n.d * (1 - n.drop);
    const consumers = ctx.outs.get(id) || [];
    let consumed = 0;
    let instances = 0;
    let spare = 0;
    for (const e of consumers) {
      const cn = st.nodes.get(e.target);
      consumed += (st.edgeFlow.get(e.id) || 0) * (1 - cn.drop);
      instances += cn.inst;
      if (Number.isFinite(cn.capEff)) spare += Math.max(0, cn.capEff - cn.d);
    }
    if (m.fifo && instances > m.partitions) consumed *= m.partitions / instances;
    const backlog = Math.max(0, inflow - consumed);
    let timeToFull;
    let overflow;
    if (m.liveDepth !== null && m.liveDepth !== undefined) {
      timeToFull = backlog > 0 ? Math.max(0, m.maxDepth - m.liveDepth) / backlog : Infinity;
      overflow = backlog > 0 && inflow > 0 && m.liveDepth >= m.maxDepth ? backlog / inflow : 0;
    } else {
      timeToFull = backlog > 0 ? m.maxDepth / backlog : Infinity;
      overflow = backlog > 0 && inflow > 0 ? (backlog / inflow) * Math.max(0, 1 - timeToFull / WINDOW_SEC) : 0;
    }
    stats.set(id, {
      inflow, consumed, backlog, timeToFull, overflow, spare,
      depthAfterWindow: Math.min(m.maxDepth, backlog * WINDOW_SEC),
      consumers: consumers.length,
    });
  }
  return stats;
}

function feedback(ctx, st, ev, refDown) {
  const retryMult = new Map();
  for (const [edgeId, view] of ev.seen) if (view.attempts > 1.0001) retryMult.set(edgeId, view.attempts);
  const slow = new Map();
  for (const id of ctx.order) {
    const m = ctx.models.get(id);
    if (!CALLERS.has(m.type)) continue;
    const tRef = m.lat + (refDown.get(id) || 0);
    const tAct = m.lat + (ev.downSeen.get(id) || 0);
    const factor = (THREAD_HEADROOM * tRef) / Math.max(tAct, 1e-9);
    if (factor < 1) slow.set(id, clamp(factor, 0.05, 1));
  }
  const qs = queueStats(ctx, st);
  const queueOverflow = new Map([...qs].map(([id, q]) => [id, q.overflow]));
  const cacheDrop = new Map();
  for (const id of ctx.order) if (ctx.models.get(id).type === 'Cache') cacheDrop.set(id, st.nodes.get(id).drop);
  return { retryMult, slow, queueOverflow, cacheDrop, storm: stormTargets(ctx, ev.downSeen) };
}

function blend(prev, next, keepMissing) {
  const out = new Map();
  const keys = new Set([...prev.keys(), ...next.keys()]);
  let delta = 0;
  for (const k of keys) {
    const a = prev.get(k) ?? keepMissing;
    const b = next.get(k) ?? keepMissing;
    const v = a + 0.6 * (b - a);
    delta = Math.max(delta, Math.abs(v - a));
    out.set(k, v);
  }
  return { map: out, delta };
}

function statusFor(m, n) {
  if (m.type === 'User') return 'source';
  if (m.down || (m.type === 'KafkaTopic' && m.writeFail >= 0.999)) return 'down';
  if (n.d <= 0) return 'idle';
  if (n.stuck > 0.001 || (m.type === 'KafkaTopic' && m.writeFail > 0.001)) return 'overloaded';
  if (!Number.isFinite(n.rho) || n.rho > 1) return 'overloaded';
  if (n.rho >= 0.85) return 'hot';
  if (n.rho >= 0.7) return 'warm';
  return 'ok';
}

function criticalPath(ctx, st, ev) {
  let start = null;
  let best = -1;
  for (const [uid, e] of ctx.entry) if (e.rps > best) { best = e.rps; start = uid; }
  const hops = [];
  const visited = new Set();
  let id = start;
  while (id && !visited.has(id) && hops.length < 20) {
    visited.add(id);
    const m = ctx.models.get(id);
    const n = st.nodes.get(id);
    if (m.type !== 'User') hops.push({ id, label: m.label, type: m.type, latency_ms: round1(m.down ? 0 : m.lat * queueFactor(n.rho) * m.latMult) });
    const { sync } = splitEdges(id, ctx);
    let next = null;
    let weight = -1;
    for (const e of sync) {
      const view = ev.seen.get(e.id);
      const flow = st.edgeFlow.get(e.id) || 0;
      const score = (view ? view.L : 0) * (flow > 0 ? 1 : 0.001);
      if (score > weight) { weight = score; next = e.target; }
    }
    id = next;
  }
  return hops;
}


export function simulate(graph, options = {}) {
  const ctx = makeContext(graph, options);
  const refState = flowPass(ctx);
  const refDown = evaluate(ctx, refState, { nominal: true }).downSeen;

  let st = refState;
  let ev = evaluate(ctx, st);
  let passes = 1;
  for (let pass = 1; pass < MAX_PASSES; pass++) {
    const fb = feedback(ctx, st, ev, refDown);
    const r = blend(ctx.retryMult, fb.retryMult, 1);
    const s = blend(ctx.slow, fb.slow, 1);
    const q = blend(ctx.queueOverflow, fb.queueOverflow, 0);
    const c = blend(ctx.cacheDrop, fb.cacheDrop, 0);
    const k = blend(ctx.storm, fb.storm, 0);
    ctx.retryMult = r.map;
    ctx.slow = s.map;
    ctx.queueOverflow = q.map;
    ctx.cacheDrop = c.map;
    ctx.storm = k.map;
    st = flowPass(ctx);
    ev = evaluate(ctx, st);
    passes = pass + 1;
    if (Math.max(r.delta, s.delta, q.delta, c.delta, k.delta) < 0.002) break;
  }

  let legit = 0;
  let okWeighted = 0;
  let latWeighted = 0;
  for (const [uid, e] of ctx.entry) {
    const share = e.rps - e.abusive;
    legit += share;
    okWeighted += share * (ev.S.get(uid) ?? 1);
    latWeighted += share * (ev.L.get(uid) ?? 0);
  }
  const success = legit > 0 ? okWeighted / legit : 1;
  const mean = legit > 0 ? latWeighted / legit : 0;

  const onPath = new Set();
  const stack = ctx.idx.users().map((u) => u.id);
  while (stack.length) {
    const id = stack.pop();
    if (onPath.has(id)) continue;
    onPath.add(id);
    for (const e of ctx.outs.get(id) || []) if (!ctx.isAsyncEdge(e)) stack.push(e.target);
  }
  let maxRho = 0;
  for (const id of onPath) {
    const m = ctx.models.get(id);
    if (!m || m.type === 'User') continue;
    const rho = st.nodes.get(id).rho;
    maxRho = Math.max(maxRho, Number.isFinite(rho) ? rho : 9.99);
  }
  const hot = clamp((Math.min(maxRho, 1.2) - 0.6) / 0.6, 0, 1);
  const qs = queueStats(ctx, st);
  const horizonSec = Math.max(WINDOW_SEC, Number(options.horizonSec) || WINDOW_SEC);
  const kafka = kafkaReport(ctx, st, ev, { windowSec: WINDOW_SEC, horizonSec });

  const nodeMetrics = [];
  let blocked = 0;
  for (const node of graph.nodes) {
    const m = ctx.models.get(node.id);
    const n = st.nodes.get(node.id);
    if (!m || !n) continue;
    blocked += n.blocked || 0;
    const notes = [...m.notes];
    const incomingRetry = Math.max(1, ...ctx.idx.in(node.id).map((e) => ctx.retryMult.get(e.id) || 1));
    if (incomingRetry > 1.15) notes.push(`Retries from callers multiply the load on this component by ${incomingRetry.toFixed(1)}×.`);
    if (n.slow !== undefined && n.slow < 0.9) notes.push(`Slow dependencies tie up its threads; effective capacity is ${pct(n.slow)} of normal.`);
    for (const e of ctx.outs.get(node.id) || []) {
      const view = ev.seen.get(e.id);
      if (view?.tripped) notes.push(`Circuit breaker to ${ctx.models.get(e.target).label} is open: calls fail fast instead of piling up.`);
    }
    if (m.autoscaling) notes.push(`Autoscaled to ${n.inst} instances (range ${m.minInst}–${m.maxInst}).`);
    if (n.blocked > 0.5) notes.push(`Rejected ${Math.round(n.blocked)} rps of abusive traffic.`);
    const q = qs.get(node.id);
    if (q && q.backlog > 0.5) {
      notes.push(Number.isFinite(q.timeToFull) && q.timeToFull < WINDOW_SEC
        ? `Backlog grows by ${Math.round(q.backlog)} msg/s; the queue fills in ${Math.round(q.timeToFull)} s and starts rejecting work.`
        : `Backlog grows by ${Math.round(q.backlog)} msg/s.`);
    }
    notes.push(...(kafka?.notes.get(node.id) || []));
    nodeMetrics.push({
      id: node.id, type: node.type, label: node.label,
      demand_rps: round1(n.d),
      served_rps: round1(n.served),
      capacity_rps: Number.isFinite(n.capEff) ? round1(n.capEff) : null,
      instances: n.inst,
      utilization: Number.isFinite(n.rho) ? round3(n.rho) : 9.99,
      drop_rate: round3(n.drop),
      read_share: m.type === 'Database' ? round3(m.lastReadShare ?? m.readRatio) : null,
      latency_ms: round1(m.type === 'User' || m.down ? 0 : m.lat * queueFactor(n.rho) * m.latMult),
      status: statusFor(m, n),
      on_request_path: onPath.has(node.id),
      autoscaling: m.autoscaling ? { min: m.minInst, max: m.maxInst, instances: n.inst, wanted: m.down ? 0 : wantedInstances(m, n.d, ctx) } : null,
      queue: q ? {
        inflow: round1(q.inflow), consumed: round1(q.consumed), backlog_rps: round1(q.backlog), spare_rps: round1(q.spare),
        time_to_full_sec: Number.isFinite(q.timeToFull) ? Math.round(q.timeToFull) : null,
        depth_after_window: Math.round(q.depthAfterWindow), max_depth: m.maxDepth, consumers: q.consumers,
        depth: m.liveDepth,
      } : null,
      kafka: kafka?.nodes.get(node.id) || null,
      notes,
    });
  }

  const order = (a, b) => b.utilization - a.utilization;
  const bottlenecks = nodeMetrics.filter((n) => n.status === 'overloaded' || n.status === 'hot' || n.status === 'down').sort(order).map((n) => n.id);
  const backlog = [...qs.values()].reduce((s, q) => s + q.backlog, 0) + (kafka?.lag_rps || 0);

  return {
    incoming_rps: round1(ctx.totalRps),
    legit_rps: round1(ctx.legitRps),
    blocked_rps: round1(blocked),
    throughput_rps: round1(ctx.legitRps * success),
    success_rate: round3(success),
    error_rate: round3(1 - success),
    latency_ms: {
      mean: round1(mean),
      p95: round1(mean * (1.6 + 1.4 * hot)),
      p99: round1(mean * (2.2 + 3.8 * hot)),
    },
    max_utilization: round3(maxRho),
    bottleneck_ids: bottlenecks,
    down_ids: nodeMetrics.filter((n) => n.status === 'down').map((n) => n.id),
    async: { backlog_rps: round1(backlog), overflowing: [...qs].filter(([, q]) => q.overflow > 0.001).map(([id]) => id) },
    node_metrics: nodeMetrics,
    edge_flows: graph.edges.map((e) => ({ id: e.id, source: e.source, target: e.target, rps: round1(st.edgeFlow.get(e.id) || 0) })),
    latency_hops: criticalPath(ctx, st, ev),
    kafka: kafka ? {
      topics: kafka.topics,
      data_loss_rate: kafka.data_loss_rate,
      stuck_rate: kafka.stuck_rate,
      behind_rate: kafka.behind_rate,
      freshness_ms: kafka.freshness_ms,
      lag_rps: kafka.lag_rps,
      guarantees: kafka.guarantees,
    } : null,
    cycles_ignored: ctx.back.size,
    passes,
  };
}

// ================================================================ auto layout
export const NODE_W = 156;
export const NODE_H = 58;
const COL_GAP = 210;
const ROW_GAP = 96;
const ORIGIN = { x: 40, y: 40 };

// Left-to-right layout: column = longest path from a source. Rows are tried two ways (compact
// columns, and straight lanes that keep long arrows clear of other boxes) and the arrangement
// whose arrows pass through fewer boxes wins.
export function autoLayout(graph) {
  const idx = new GraphIndex(graph);
  const { order, back } = idx.topo();
  const rank = new Map();
  for (const id of order) {
    const parents = idx.in(id).filter((e) => !back.has(e.id));
    rank.set(id, parents.length ? Math.max(...parents.map((e) => (rank.get(e.source) ?? 0) + 1)) : 0);
  }
  const compact = compactRows(idx, order, back, rank);
  const lanes = laneRows(idx, order, back, rank);
  const pos = untangle(graph, arrowsThroughBoxes(graph, lanes) < arrowsThroughBoxes(graph, compact) ? lanes : compact);
  return {
    ...graph,
    nodes: graph.nodes.map((n) => ({ ...n, ...(pos.get(n.id) || { x: n.x, y: n.y }) })),
  };
}

// Rows ordered by the average row of each node's parents, each column centred.
function compactRows(idx, order, back, rank) {
  const columns = new Map();
  for (const id of order) {
    const r = rank.get(id);
    if (!columns.has(r)) columns.set(r, []);
    columns.get(r).push(id);
  }
  const row = new Map();
  const ranks = [...columns.keys()].sort((a, b) => a - b);
  for (const r of ranks) {
    const ids = columns.get(r);
    const score = (id) => {
      const parents = idx.in(id).filter((e) => !back.has(e.id) && row.has(e.source));
      return parents.length ? parents.reduce((s, e) => s + row.get(e.source), 0) / parents.length : ids.indexOf(id);
    };
    ids.sort((a, b) => score(a) - score(b));
    ids.forEach((id, i) => row.set(id, i));
  }
  const tallest = Math.max(1, ...[...columns.values()].map((c) => c.length));
  const pos = new Map();
  for (const r of ranks) {
    const ids = columns.get(r);
    const offset = ((tallest - ids.length) * ROW_GAP) / 2;
    ids.forEach((id, i) => pos.set(id, { x: ORIGIN.x + r * COL_GAP, y: ORIGIN.y + offset + i * ROW_GAP }));
  }
  return pos;
}

// Each parent hands its row ("lane") to its furthest child and keeps that lane empty in the
// columns in between, so the longest arrow runs straight. Other children take the nearest free
// lane, alternating below and above.
function laneRows(idx, order, back, rank) {
  const mainParent = new Map();
  for (const id of order) {
    const parents = idx.in(id).filter((e) => !back.has(e.id));
    if (parents.length) mainParent.set(id, parents.reduce((best, e) => (rank.get(e.source) < rank.get(best.source) ? e : best)).source);
  }
  const heir = new Map();
  for (const [child, parent] of mainParent) {
    const current = heir.get(parent);
    if (current === undefined || rank.get(child) > rank.get(current)) heir.set(parent, child);
  }

  const cells = new Map();
  const ownerOf = (col, lane) => cells.get(col)?.get(lane);
  const claim = (col, lane, id) => {
    if (!cells.has(col)) cells.set(col, new Map());
    cells.get(col).set(lane, id);
  };
  const lane = new Map();
  const byColumn = [...order].sort((a, b) => rank.get(a) - rank.get(b));
  for (const id of byColumn) {
    const col = rank.get(id);
    const parent = mainParent.get(id);
    const start = parent === undefined ? 0 : lane.get(parent);
    let mine = ownerOf(col, start) === id ? start : null;
    for (let d = 0; mine === null; d++) {
      for (const candidate of d === 0 ? [start] : [start + d, start - d]) {
        if (mine === null && ownerOf(col, candidate) === undefined) mine = candidate;
      }
    }
    lane.set(id, mine);
    claim(col, mine, id);

    const next = heir.get(id);
    if (next === undefined) continue;
    const span = [];
    for (let c = col + 1; c <= rank.get(next); c++) span.push(c);
    if (span.every((c) => ownerOf(c, mine) === undefined)) span.forEach((c) => claim(c, mine, next));
  }

  const top = Math.min(0, ...lane.values());
  const pos = new Map();
  for (const [id, l] of lane) pos.set(id, { x: ORIGIN.x + rank.get(id) * COL_GAP, y: ORIGIN.y + (l - top) * ROW_GAP });
  return pos;
}

// Moves single boxes up or down a few rows while that clears arrows out of other boxes,
// preferring shorter vertical arrows when the count is tied.
function untangle(graph, start) {
  const pos = new Map(start);
  const cost = () => arrowsThroughBoxes(graph, pos) * 1e6 + graph.edges.reduce((s, e) => {
    const a = pos.get(e.source);
    const b = pos.get(e.target);
    return a && b ? s + Math.abs(a.y - b.y) : s;
  }, 0);
  if (arrowsThroughBoxes(graph, pos) === 0) return pos;
  let best = cost();
  for (let pass = 0; pass < 2; pass++) {
    for (const [id, home] of [...pos]) {
      let here = home;
      for (let d = -3; d <= 3; d++) {
        const y = home.y + d * ROW_GAP;
        if (!d || [...pos].some(([other, p]) => other !== id && p.x === home.x && p.y === y)) continue;
        pos.set(id, { x: home.x, y });
        const c = cost();
        if (c < best) {
          best = c;
          here = pos.get(id);
        }
        pos.set(id, here);
      }
    }
  }
  const top = Math.min(...[...pos.values()].map((p) => p.y));
  for (const [id, p] of pos) pos.set(id, { x: p.x, y: p.y - top + ORIGIN.y });
  return pos;
}

// Counts (arrow, box) pairs where an arrow is drawn through, or grazes, a box it does not connect.
// The curve must match the arrow shape in public/js/workspace/editor.js (edgePath).
const CLEARANCE = 8;
export function arrowsThroughBoxes(graph, pos = new Map(graph.nodes.map((n) => [n.id, n]))) {
  let count = 0;
  for (const e of graph.edges) {
    const a = pos.get(e.source);
    const b = pos.get(e.target);
    if (!a || !b || e.source === e.target) continue;
    const x1 = a.x + NODE_W;
    const y1 = a.y + NODE_H / 2;
    const x2 = b.x;
    const y2 = b.y + NODE_H / 2;
    const dx = x2 >= x1 ? Math.max(40, (x2 - x1) * 0.5) : 120;
    for (const [id, n] of pos) {
      if (id === e.source || id === e.target) continue;
      for (let t = 0.04; t < 1; t += 0.04) {
        const u = 1 - t;
        const x = u * u * u * x1 + 3 * u * u * t * (x1 + dx) + 3 * u * t * t * (x2 - dx) + t * t * t * x2;
        const y = (u * u * u + 3 * u * u * t) * y1 + (3 * u * t * t + t * t * t) * y2;
        if (x > n.x - CLEARANCE && x < n.x + NODE_W + CLEARANCE && y > n.y - CLEARANCE && y < n.y + NODE_H + CLEARANCE) {
          count++;
          break;
        }
      }
    }
  }
  return count;
}

// ================================================================ kafka design rules
const STORES = new Set(['Database', 'Cache', 'SearchIndex', 'BlobStorage', 'ThirdPartyAPI']);
const READERS = new Set(['ConsumerGroup', 'StreamProcessor', 'MirrorMaker']);

// Design checks for the Kafka parts of a graph. Returns findings, scored security checks and strengths.
export function kafkaReview(graph, idx, { p, instances, problem = null }) {
  const findings = [];
  const strengths = [];
  const add = (f) => findings.push({ node_ids: [], category: 'kafka', ...f });
  const ofType = (t) => graph.nodes.filter((n) => n.type === t);
  const label = (id) => idx.node(id)?.label || id;
  const topics = ofType('KafkaTopic');
  const consumers = graph.nodes.filter((n) => idx.in(n.id).some((e) => idx.type(e.source) === 'KafkaTopic'));
  const inputPartitions = (id) => idx.in(id).filter((e) => idx.type(e.source) === 'KafkaTopic')
    .reduce((s, e) => s + (Number(p(e.source, 'partitions')) || 1), 0);
  const kafkaParts = graph.nodes.filter((n) => n.type === 'KafkaTopic' || KAFKA_CONSUMERS.has(n.type) || n.type === 'SchemaRegistry');
  if (!kafkaParts.length) return { findings, checks: [], strengths };

  // ---- Wiring ------------------------------------------------------------------
  for (const t of topics) {
    if (!idx.out(t.id).length) {
      add({ code: 'TOPIC_NO_CONSUMER', severity: 'warning', title: `Nothing reads ${t.label}`,
        reason: 'Messages are kept for the retention period and then deleted without anyone using them.',
        impact: 'The work behind these events never happens.', recommendation: 'Connect a Consumer Group, Stream Processor or Kafka Connect sink to the topic.', node_ids: [t.id] });
    }
    if (!idx.in(t.id).length) {
      add({ code: 'TOPIC_NO_PRODUCER', severity: 'warning', title: `Nothing writes to ${t.label}`,
        reason: 'No service, connector or stream processor produces messages to this topic.', impact: 'Its consumers have nothing to do.',
        recommendation: 'Connect the component that produces these events to the topic.', node_ids: [t.id] });
    }
    const pushes = idx.out(t.id).filter((e) => STORES.has(idx.type(e.target)));
    if (pushes.length) {
      add({ code: 'TOPIC_PUSHES_TO_STORE', severity: 'warning', title: `${t.label} points straight at a store`,
        reason: 'Kafka never pushes data anywhere: consumers pull it. Something has to read the topic and write to the store.',
        impact: 'Nothing would actually move the data.', recommendation: 'Put a Kafka Connect sink or a Consumer Group between the topic and the store.', node_ids: [t.id, ...pushes.map((e) => e.target)] });
    }
  }
  for (const n of graph.nodes) {
    const reads = idx.in(n.id).some((e) => idx.type(e.source) === 'KafkaTopic');
    const sink = n.type === 'KafkaConnect' && p(n.id, 'mode') === 'sink';
    if ((READERS.has(n.type) || sink) && !reads) {
      add({ code: 'CONSUMER_NO_TOPIC', severity: 'warning', title: `${n.label} reads no topic`,
        reason: 'Consumers pull messages from topics. Nothing connects a topic to this one, so it has nothing to read.',
        impact: 'It does nothing in the simulation.', recommendation: 'Draw a connection from the topic it should read to it.', node_ids: [n.id] });
    }
    const called = idx.in(n.id).filter((e) => READERS.has(n.type) && idx.type(e.source) !== 'KafkaTopic');
    if (called.length) {
      add({ code: 'CONSUMER_CALLED_DIRECTLY', severity: 'warning', title: `${n.label} is called directly`,
        reason: 'A consumer group pulls work from a topic; it has no API for others to call.',
        impact: 'The design reads as if callers wait for the consumer.', recommendation: 'Have the caller publish to a topic and connect the topic to the consumer.', node_ids: [n.id] });
    }
    if (n.type === 'Database' && idx.out(n.id).some((e) => idx.type(e.target) === 'KafkaTopic')) {
      add({ code: 'DB_TO_TOPIC_DIRECT', severity: 'warning', title: `${n.label} publishes to Kafka by itself`,
        reason: 'A database cannot write to Kafka on its own. Change data capture reads its log and publishes the changes.',
        impact: 'Nothing would actually stream the changes.', recommendation: 'Put Kafka Connect (source mode, CDC connector such as Debezium) between the database and the topic.', node_ids: [n.id] });
    }
  }

  // ---- Durability ----------------------------------------------------------------
  for (const t of topics) {
    const brokers = Number(p(t.id, 'brokers')) || 1;
    const rf = Number(p(t.id, 'replication_factor')) || 1;
    const minIsr = Number(p(t.id, 'min_insync_replicas')) || 1;
    const acks = String(p(t.id, 'acks'));
    if (rf > brokers) {
      add({ code: 'TOPIC_RF_EXCEEDS_BROKERS', severity: 'critical', title: `${t.label} wants more copies than there are brokers`,
        reason: `A replication factor of ${rf} puts each copy on a different broker, but the cluster has ${brokers}.`,
        impact: 'Kafka refuses to create the topic.', recommendation: `Run at least ${rf} brokers or lower the replication factor.`, node_ids: [t.id] });
      continue;
    }
    if (rf < 2) {
      add({ code: 'TOPIC_NO_REPLICATION', severity: 'warning', title: `${t.label} keeps a single copy`,
        reason: 'With replication factor 1, each partition lives on one broker only.',
        impact: 'A broker failure takes its partitions offline and can lose their data for good.', recommendation: 'Run 3 brokers with replication factor 3 and min.insync.replicas 2.', node_ids: [t.id] });
    }
    if (acks !== 'all') {
      add({ code: 'TOPIC_UNSAFE_ACKS', severity: 'warning', title: `Producers to ${t.label} do not wait for copies`,
        reason: `With acks=${acks}, a write counts as done before the followers have it${acks === '0' ? ', or even before the leader has it' : ''}.`,
        impact: 'A leader crash silently loses recent messages.', recommendation: 'Use acks=all with min.insync.replicas=2.', node_ids: [t.id] });
    } else if (minIsr > rf) {
      add({ code: 'TOPIC_MIN_ISR_TOO_HIGH', severity: 'critical', title: `${t.label} rejects every write`,
        reason: `min.insync.replicas is ${minIsr} but only ${rf} copies exist, so no acks=all write can ever succeed.`,
        impact: 'Producers get errors for every message.', recommendation: 'Set min.insync.replicas to replication factor − 1 (2 of 3 copies).', node_ids: [t.id] });
    } else if (rf >= 2 && minIsr >= rf) {
      add({ code: 'TOPIC_MIN_ISR_EQUALS_RF', severity: 'warning', title: `Losing one broker stops writes to ${t.label}`,
        reason: `acks=all needs ${minIsr} in-sync copies and there are only ${rf}, so any broker restart leaves partitions short.`,
        impact: 'Every rolling restart or broker failure becomes a write outage.', recommendation: 'Set min.insync.replicas to replication factor − 1 (2 of 3 copies).', node_ids: [t.id] });
    } else if (rf >= 2 && minIsr < 2) {
      add({ code: 'TOPIC_MIN_ISR_ONE', severity: 'info', title: `${t.label} lets the leader confirm writes alone`,
        reason: 'With min.insync.replicas=1, acks=all only waits for the copies that are in sync, which can be just the leader.',
        impact: 'Under load, acks=all quietly becomes acks=1 and a leader crash loses data.', recommendation: 'Set min.insync.replicas to 2.', node_ids: [t.id] });
    }
    if (!p(t.id, 'idempotent_producer')) {
      add({ code: 'PRODUCER_NOT_IDEMPOTENT', severity: 'info', title: `Producers to ${t.label} are not idempotent`,
        reason: 'When an acknowledgement is lost, the producer retries and the broker cannot tell it is the same message.',
        impact: 'Duplicate messages, and retried batches can land out of order.', recommendation: 'Enable the idempotent producer (the default since Kafka 3.0).', node_ids: [t.id] });
    }
    if (rf >= 2 && !p(t.id, 'rack_aware')) {
      add({ code: 'TOPIC_NOT_RACK_AWARE', severity: 'info', title: `${t.label} may keep all copies in one zone`,
        reason: 'Without rack awareness, every copy of a partition can sit in the same availability zone.', impact: 'A zone outage takes the partition down despite replication.',
        recommendation: 'Spread copies across zones (broker.rack).', node_ids: [t.id] });
    }
    if (p(t.id, 'controller') === 'zookeeper') {
      add({ code: 'ZOOKEEPER_MODE', severity: 'info', title: `${t.label}'s cluster still runs on ZooKeeper`,
        reason: 'ZooKeeper mode is deprecated and removed in Kafka 4.0. KRaft keeps the metadata inside Kafka itself.', impact: 'An extra system to run, and a forced migration later.',
        recommendation: 'Use KRaft mode.', node_ids: [t.id] });
    }
    if (p(t.id, 'cleanup_policy') === 'delete' && (Number(p(t.id, 'retention_hours')) || 0) < 24) {
      add({ code: 'SHORT_RETENTION', severity: 'info', title: `${t.label} keeps messages for under a day`,
        reason: `Messages are deleted after ${p(t.id, 'retention_hours')} h, read or not.`, impact: 'A consumer outage longer than that loses data, and nobody can replay history.',
        recommendation: 'Keep at least 3–7 days, or use tiered storage for longer.', node_ids: [t.id] });
    }
  }

  // ---- Partitions, keys and batching ------------------------------------------------
  for (const t of topics) {
    const partitions = Number(p(t.id, 'partitions')) || 1;
    const key = p(t.id, 'partition_key');
    const hot = Number(p(t.id, 'hot_key_share')) || 0;
    if (key === 'skewed' && hot > 1.5 / partitions) {
      add({ code: 'HOT_KEY', severity: 'warning', title: `${t.label} has a hot key`,
        reason: `One key carries ${Math.round(hot * 100)}% of messages, and a key always maps to the same partition.`,
        impact: 'One partition, and the one consumer reading it, carry that load alone. Adding partitions or consumers does not help.',
        recommendation: 'Key by something with many evenly used values (user or order id), or split the hot key by adding a suffix and recombining downstream.', node_ids: [t.id] });
    }
    if (key === 'none' && problem?.kafka?.ordered) {
      add({ code: 'NO_KEY_ORDERING', severity: 'warning', title: `Events in ${t.label} can arrive out of order`,
        reason: 'Kafka only keeps order within a partition. Without a key, events for the same entity are spread over partitions.',
        impact: 'Consumers may see "order shipped" before "order paid".', recommendation: 'Key messages by the entity whose order matters (for example the order id).', node_ids: [t.id] });
    }
    if ((Number(p(t.id, 'linger_ms')) || 0) === 0) {
      add({ code: 'LINGER_ZERO', severity: 'info', title: `Producers to ${t.label} send tiny batches`,
        reason: 'With linger.ms=0, each message tends to go out in its own request.', impact: 'Brokers do far more work per message, cutting throughput by about 40% here.',
        recommendation: 'Set linger.ms to 5–20 ms so batches fill up.', node_ids: [t.id] });
    }
    if (p(t.id, 'compression') === 'none') {
      add({ code: 'NO_COMPRESSION', severity: 'info', title: `${t.label} is not compressed`,
        reason: 'Messages travel and sit on disk at full size.', impact: 'More disk and network per message, so fewer messages per broker.',
        recommendation: 'Use lz4 or zstd compression on the producer.', node_ids: [t.id] });
    }
  }
  for (const c of consumers) {
    const partitions = inputPartitions(c.id);
    const autoscaled = !!p(c.id, 'autoscaling');
    const count = autoscaled ? Number(p(c.id, 'autoscale_min')) || 1 : instances(c.id);
    if (count > partitions) {
      add({ code: 'TOO_FEW_PARTITIONS', severity: 'warning', title: `${c.label} has more consumers than partitions`,
        reason: `${count} consumers read ${partitions} partitions. Within a group, each partition goes to one consumer.`,
        impact: `${count - partitions} consumers sit idle.`, recommendation: 'Add partitions (plan for your peak consumer count) or run fewer consumers.', node_ids: [c.id] });
    } else if (autoscaled && (Number(p(c.id, 'autoscale_max')) || 1) > partitions) {
      add({ code: 'AUTOSCALE_PAST_PARTITIONS', severity: 'info', title: `${c.label} can never use its autoscaling maximum`,
        reason: `It may scale to ${p(c.id, 'autoscale_max')} consumers, but only ${partitions} partitions exist.`, impact: 'Scaling stops at the partition count.',
        recommendation: 'Add partitions if you need more parallelism.', node_ids: [c.id] });
    }
  }

  // ---- Consumers --------------------------------------------------------------------
  for (const c of consumers) {
    const t = c.type;
    const commit = t === 'ConsumerGroup' ? p(c.id, 'commit')
      : t === 'StreamProcessor' ? (p(c.id, 'processing_guarantee') === 'exactly_once' ? 'transactional' : 'after_processing')
        : t === 'KafkaConnect' && p(c.id, 'exactly_once') ? 'transactional' : 'after_processing';
    const idempotent = commit === 'transactional' || !!p(c.id, 'idempotent');
    if (commit === 'before_processing') {
      add({ code: 'AT_MOST_ONCE_CONSUMER', severity: 'warning', title: `${c.label} can skip messages`,
        reason: 'It commits offsets before processing (at-most-once). If it crashes mid-batch, the committed but unprocessed messages are never seen again.',
        impact: 'Silent data loss on every crash or rebalance.', recommendation: 'Commit after processing, and make processing idempotent.', node_ids: [c.id] });
    } else if (!idempotent && (t === 'ConsumerGroup' || t === 'Worker' || t === 'Service')) {
      add({ code: 'KAFKA_CONSUMER_NOT_IDEMPOTENT', severity: 'warning', title: `${c.label} can process a message twice`,
        reason: 'It commits after processing (at-least-once): after a crash or rebalance, messages since the last commit come back.',
        impact: 'Double charges, duplicate emails or double-counted events.', recommendation: 'Make processing idempotent (upsert by event id, or keep a table of handled ids), or use transactions when the output is Kafka.', node_ids: [c.id] });
    }
    if ((t === 'ConsumerGroup' || t === 'StreamProcessor' || (t === 'KafkaConnect' && p(c.id, 'mode') === 'sink')) && !p(c.id, 'dead_letter_topic')) {
      add({ code: 'KAFKA_NO_DLT', severity: 'info', title: `One bad message can block ${c.label}`,
        reason: 'Without a dead letter topic, a message that always fails is retried forever, and its partition stops moving.',
        impact: 'A single malformed event stalls part of the pipeline.', recommendation: 'Enable a dead letter topic and alert on it.', node_ids: [c.id] });
    }
    if (t === 'ConsumerGroup') {
      const batchMs = (Number(p(c.id, 'max_poll_records')) || 500) * (Number(p(c.id, 'base_latency')) || 0);
      const interval = Number(p(c.id, 'max_poll_interval_ms')) || 300000;
      if (batchMs > interval * 0.5) {
        add({ code: 'REBALANCE_STORM_RISK', severity: 'warning', title: `${c.label} is close to a rebalance storm`,
          reason: `One poll of ${p(c.id, 'max_poll_records')} messages takes about ${Math.round(batchMs / 1000)} s, more than half of max.poll.interval.ms (${Math.round(interval / 1000)} s). A slowdown pushes it over.`,
          impact: 'Kafka keeps evicting consumers and rebalancing, replaying the same batches and making no progress.',
          recommendation: 'Lower max.poll.records, or raise max.poll.interval.ms.', node_ids: [c.id] });
      }
      if (p(c.id, 'assignment') === 'eager' && instances(c.id) >= 3) {
        add({ code: 'EAGER_REBALANCING', severity: 'info', title: `Every rebalance stops all of ${c.label}`,
          reason: 'With eager rebalancing, all consumers give up their partitions whenever one joins or leaves.', impact: 'Deploys and autoscaling pause the whole group.',
          recommendation: 'Use cooperative rebalancing and static membership.', node_ids: [c.id] });
      }
    }
    if (t === 'StreamProcessor' && p(c.id, 'state') === 'stateful' && !(Number(p(c.id, 'standby_replicas')) > 0)) {
      add({ code: 'STATEFUL_NO_STANDBY', severity: 'info', title: `${c.label} rebuilds its state after a failure`,
        reason: 'Its state stores have no standby copies, so a failed instance\'s state is replayed from the changelog topic.', impact: 'Minutes of paused partitions after every crash.',
        recommendation: 'Run one standby replica.', node_ids: [c.id] });
    }
  }

  // ---- Schemas and consistency --------------------------------------------------------
  const registries = ofType('SchemaRegistry');
  const groupCount = topics.reduce((s, t) => s + idx.out(t.id).length, 0);
  if (topics.length && !registries.length && (groupCount >= 2 || problem?.kafka?.schemas)) {
    add({ code: 'NO_SCHEMA_REGISTRY', severity: problem?.kafka?.schemas ? 'warning' : 'info', title: 'Nothing checks message formats',
      reason: 'Producers and consumers agree on the message format only by convention. One careless change can break every consumer.',
      impact: 'A producer deploy stops consumers that cannot read the new format.', recommendation: 'Add a Schema Registry with backward compatibility, and connect producers and consumers to it.' });
  }
  for (const r of registries) {
    if (p(r.id, 'compatibility') === 'none') {
      add({ code: 'SCHEMA_COMPAT_NONE', severity: 'warning', title: `${r.label} accepts any schema change`,
        reason: 'With compatibility "none", the registry stores schemas but lets breaking changes through.', impact: 'It cannot stop a change that breaks consumers.',
        recommendation: 'Use backward (or full) compatibility.', node_ids: [r.id] });
    }
  }
  for (const n of graph.nodes) {
    if (consumers.includes(n) || n.type === 'User') continue;
    const db = idx.out(n.id).find((e) => idx.type(e.target) === 'Database' && e.kind !== 'async');
    const topic = idx.out(n.id).find((e) => idx.type(e.target) === 'KafkaTopic');
    if (db && topic) {
      add({ code: 'DUAL_WRITE', severity: 'warning', title: `${n.label} writes to the database and Kafka separately`,
        reason: `It saves to ${label(db.target)} and publishes to ${label(topic.target)} as two steps. A crash between them, or one failing, leaves them disagreeing for good.`,
        impact: 'Lost events or events for changes that never happened.',
        recommendation: 'Use the transactional outbox: write the event to an outbox table in the same transaction, and stream it with Kafka Connect (CDC).', node_ids: [n.id] });
    }
  }

  // ---- Security (scored separately) ------------------------------------------------------
  const checks = topics.length ? [
    { code: 'SEC_KAFKA_TLS', title: 'Kafka traffic is not encrypted', label: 'Kafka TLS', applicable: true,
      passed: topics.every((t) => p(t.id, 'tls')), reason: 'Producers, consumers and brokers talk in plaintext.',
      impact: 'Anyone on the network can read or alter events.', recommendation: 'Enable TLS on the Kafka cluster.',
      node_ids: topics.filter((t) => !p(t.id, 'tls')).map((t) => t.id) },
    { code: 'SEC_KAFKA_AUTH', title: 'Kafka clients are not authenticated', label: 'Kafka client authentication', applicable: true,
      passed: topics.every((t) => p(t.id, 'client_auth') !== 'none'), reason: 'Any client that can reach the brokers may connect.',
      impact: 'A rogue client can read every event or flood topics.', recommendation: 'Require SASL/SCRAM, mTLS or OAuth for every client.',
      node_ids: topics.filter((t) => p(t.id, 'client_auth') === 'none').map((t) => t.id) },
    { code: 'SEC_KAFKA_ACLS', title: 'Any client can use any topic', label: 'Kafka ACLs', applicable: true,
      passed: topics.every((t) => p(t.id, 'acls')), reason: 'Without ACLs, an authenticated client may read or write every topic.',
      impact: 'One compromised service exposes all events.', recommendation: 'Grant each service only the topics it needs.',
      node_ids: topics.filter((t) => !p(t.id, 'acls')).map((t) => t.id) },
  ] : [];

  // ---- Strengths -----------------------------------------------------------------------
  const durable = (t) => (Number(p(t.id, 'replication_factor')) || 1) >= 3 && p(t.id, 'acks') === 'all'
    && (Number(p(t.id, 'min_insync_replicas')) || 1) >= 2 && (Number(p(t.id, 'brokers')) || 1) >= 3;
  if (topics.length && topics.every(durable)) strengths.push('Kafka keeps 3 copies of every message and confirms a write only once 2 have it.');
  if (graph.edges.some((e) => idx.type(e.source) === 'Database' && idx.type(e.target) === 'KafkaConnect')) strengths.push('Database changes reach Kafka through CDC, so events and data cannot drift apart.');
  if (registries.some((r) => p(r.id, 'compatibility') !== 'none')) strengths.push('A Schema Registry blocks breaking changes to message formats.');
  const groups = consumers.filter((c) => c.type === 'ConsumerGroup');
  if (groups.length && groups.every((c) => p(c.id, 'dead_letter_topic'))) strengths.push('Bad messages go to a dead letter topic instead of blocking partitions.');
  if (groups.length && groups.every((c) => p(c.id, 'commit') === 'transactional' || (p(c.id, 'commit') === 'after_processing' && p(c.id, 'idempotent')))) {
    strengths.push('Consumers commit after processing and handle replays safely.');
  }
  return { findings, checks, strengths };
}

// ================================================================ kafka solutions
// Candidate fixes for the Kafka parts of a failing test. Each one is checked by re-running the
// test, exactly like the other fixes in solutions.js, which passes in its helpers.

const HEADROOM = 0.7;

// Partition counts people actually pick: small numbers as they are, then multiples of 6.
const niceCount = (n) => (n <= 6 ? Math.max(1, Math.ceil(n)) : Math.ceil(n / 6) * 6);

function uid(graph, base) {
  const used = new Set(graph.nodes.map((n) => n.id));
  let id = base;
  for (let i = 2; used.has(id); i++) id = `${base}-${i}`;
  return id;
}

const edge = (source, target, kind = 'sync') => ({ id: `${source}->${target}`, source, target, kind, ratio: null, optional: false, read_share: null });

function healthyTopic(node) {
  const p = (k) => param(node, k);
  const m = topicModel({ cap: Number(p('capacity')) || 1, lat: 0, notes: [], down: false }, p, {});
  return { m, lim: topicLimits(m) };
}

function durabilityFix(node, cause, h) {
  const p = (k) => param(node, k);
  const rf = Math.max(3, Number(p('replication_factor')) || 1);
  return h.fixWith(h.graph, node, [['acks', 'all'], ['replication_factor', rf], ['brokers', Math.max(rf, Number(p('brokers')) || 1)], ['min_insync_replicas', 2]],
    cause, 'Confirm a write only once 2 of 3 copies have it: acks=all, replication factor 3, min.insync.replicas 2.');
}

function retentionFix(topic, horizonHours, cause, h) {
  const keep = Math.max(168, Math.ceil(horizonHours * 1.5));
  if ((Number(param(topic, 'retention_hours')) || 0) >= keep) return null;
  return h.fixWith(h.graph, topic, [['retention_hours', keep], ['tiered_storage', keep > 336]],
    cause, `Keep messages for ${keep} hours (${Math.round(keep / 24)} days), longer than the worst consumer outage you plan for.`);
}

// The service writes to the database and publishes to Kafka separately: publish from the
// database log instead (transactional outbox streamed by a CDC connector).
function outboxFix(graph, caller, idx) {
  const db = idx.out(caller.id).find((e) => idx.type(e.target) === 'Database' && e.kind !== 'async');
  const publishes = idx.out(caller.id).filter((e) => idx.type(e.target) === 'KafkaTopic');
  if (!db || !publishes.length) return null;
  const g = cloneGraph(graph);
  const dbNode = g.nodes.find((n) => n.id === db.target);
  const topicIds = new Set(publishes.map((e) => e.target));
  const id = uid(g, `${dbNode.id}-cdc`);
  const connect = { id, type: 'KafkaConnect', label: `${dbNode.label} CDC`, x: dbNode.x + 230, y: dbNode.y + 100, config: defaultsFor('KafkaConnect') };
  Object.assign(connect.config.advanced, { mode: 'source', connector: 'cdc', exactly_once: true });
  g.edges = g.edges.filter((e) => !(e.source === caller.id && topicIds.has(e.target)));
  g.nodes.push(connect);
  g.edges.push(edge(dbNode.id, id, 'async'), ...[...topicIds].map((t) => edge(id, t, 'async')));
  const topics = [...topicIds].map((t) => g.nodes.find((n) => n.id === t).label).join(' and ');
  const cause = `${caller.label}: saves to ${dbNode.label} and publishes to ${topics} as two separate steps, so a failure between them leaves them disagreeing.`;
  const action = `Use the transactional outbox: ${caller.label} saves the event together with the data, and a CDC connector streams committed changes from the database log to Kafka.`;
  return {
    cause, action, text: `${cause} ${action}`, graph: g,
    changes: [{ add: 'KafkaConnect', id, label: connect.label }],
    structural: { subject: caller.label, lines: [`New Kafka Connect: ${connect.label} (source, CDC)`, `${caller.label} no longer publishes to ${topics} directly`, `${dbNode.label} → ${connect.label} → ${topics}`] },
  };
}

// Producers and consumers register and check every schema, so an incompatible change is
// rejected before any consumer sees it.
function registryFix(graph) {
  if (graph.nodes.some((n) => n.type === 'SchemaRegistry' && param(n, 'compatibility') !== 'none')) return null;
  const g = cloneGraph(graph);
  const topics = g.nodes.filter((n) => n.type === 'KafkaTopic');
  const producers = [...new Set(g.edges.filter((e) => topics.some((t) => t.id === e.target)).map((e) => e.source))];
  const consumers = [...new Set(g.edges.filter((e) => topics.some((t) => t.id === e.source)).map((e) => e.target))];
  if (!producers.length) return null;
  const existing = g.nodes.find((n) => n.type === 'SchemaRegistry');
  let reg = existing;
  const lines = [];
  if (existing) {
    existing.config.advanced = { ...(existing.config.advanced || {}), compatibility: 'backward' };
    lines.push(`${existing.label}: compatibility none → backward`);
  } else {
    const anchor = topics[0];
    reg = { id: uid(g, 'schema-registry'), type: 'SchemaRegistry', label: 'Schema Registry', x: anchor.x, y: anchor.y + 120, config: defaultsFor('SchemaRegistry') };
    g.nodes.push(reg);
    lines.push('New Schema Registry (backward compatibility)');
  }
  for (const id of [...producers, ...consumers]) {
    if (id === reg.id || g.edges.some((e) => e.source === id && e.target === reg.id)) continue;
    g.edges.push(edge(id, reg.id));
  }
  lines.push('Producers and consumers check schemas with it');
  const cause = 'A producer changed its message format in a way consumers cannot read, and nothing stopped it.';
  const action = 'Add a Schema Registry with backward compatibility: an incompatible schema is rejected when the producer tries to use it.';
  return {
    cause, action, text: `${cause} ${action}`, graph: g,
    changes: existing ? [{ id: reg.id, label: reg.label, key: 'compatibility', name: 'Compatibility rule', from: 'none', to: 'backward' }] : [{ add: 'SchemaRegistry', id: reg.id, label: reg.label }],
    structural: { subject: reg.label, lines },
  };
}

function violationFixes(v, node, testCase, h, idx) {
  if (!node) return [];
  const p = (k) => param(node, k);
  const out = [];
  const topic = node.type === 'KafkaTopic';
  if (v.requirement === 'no-loss') {
    if (topic) out.push(durabilityFix(node, v.text, h));
    else if (p('commit') === 'before_processing') {
      out.push(h.fixWith(h.graph, node, [['commit', 'after_processing'], ['idempotent', true]], v.text,
        'Commit offsets only after processing, and make processing idempotent so the replays this causes are harmless.'));
    } else {
      for (const e of idx.in(node.id)) {
        if (idx.type(e.source) === 'KafkaTopic') out.push(retentionFix(idx.node(e.source), testCase.kafka?.horizon_hours || 24, v.text, h));
      }
    }
  } else if (v.requirement === 'no-duplicates') {
    if (topic) out.push(h.fixWith(h.graph, node, [['idempotent_producer', true]], v.text, 'Turn on the idempotent producer: the broker spots a retried message and keeps one copy.'));
    else if (node.type === 'StreamProcessor') out.push(h.fixWith(h.graph, node, [['processing_guarantee', 'exactly_once']], v.text, 'Use exactly-once processing: reads, state changes and writes commit together in one transaction.'));
    else if (node.type === 'KafkaConnect') out.push(h.fixWith(h.graph, node, [['exactly_once', true]], v.text, 'Turn on exactly-once delivery for the connector.'));
    else if (node.type === 'ConsumerGroup' || node.type === 'Worker') {
      out.push(h.fixWith(h.graph, node, [['idempotent', true]], v.text, 'Make processing idempotent: remember handled message ids, or write with an upsert keyed by the event id, so a replay changes nothing.'));
    } else h.advice.add(`Make ${node.label} idempotent: dedupe on the message id before acting on it.`);
  } else if (v.requirement === 'ordered' && topic) {
    if (p('partition_key') === 'none') out.push(h.fixWith(h.graph, node, [['partition_key', 'id']], v.text, 'Key every message by the entity id, so all events for one entity share a partition and stay in order.'));
    if (!p('idempotent_producer')) out.push(h.fixWith(h.graph, node, [['idempotent_producer', true]], v.text, 'Turn on the idempotent producer, which also keeps retried batches in order.'));
  } else if (v.requirement === 'consistent') {
    out.push(outboxFix(h.graph, node, idx));
  }
  return out;
}

function topicFixes(node, m, h) {
  const p = (k) => param(node, k);
  const label = node.label;
  const out = [];
  const rf = Number(p('replication_factor')) || 1;
  const brokers = Number(p('brokers')) || 1;
  const minIsr = Number(p('min_insync_replicas')) || 1;
  if (rf > brokers) {
    out.push(h.fixWith(h.graph, node, [['brokers', rf]], `${label}: wants ${rf} copies but the cluster has ${brokers} brokers, so the topic cannot exist.`, `Run ${rf} brokers.`));
    return out;
  }
  if (m.kafka?.write_fail > 0 || m.status === 'down') {
    if (p('acks') === 'all' && minIsr >= rf && rf >= 2) {
      out.push(h.fixWith(h.graph, node, [['min_insync_replicas', rf - 1]], `${label}: needs all ${rf} copies in sync to accept a write, so one lost broker stops writes.`, `Set min.insync.replicas to ${rf - 1}: writes continue with one broker down, still on ${rf - 1} copies.`));
    } else if (rf < 3) {
      out.push(h.fixWith(h.graph, node, [['replication_factor', 3], ['brokers', Math.max(3, brokers)], ['min_insync_replicas', 2]],
        `${label}: ${rf === 1 ? 'keeps a single copy' : 'keeps only 2 copies'}, so losing a broker leaves partitions without enough copies.`, 'Use 3 brokers, 3 copies and min.insync.replicas 2: any one broker can fail.'));
    } else {
      out.push(h.fixWith(h.graph, node, [['brokers', brokers + 2]], `${label}: too many brokers failed for the copies left.`, `Run ${brokers + 2} brokers, so fewer partitions lose a majority of their copies.`));
    }
  }
  if (m.status === 'down' || !(m.utilization > 0.7)) return out;
  const { m: model, lim } = healthyTopic(node);
  const demand = m.demand_rps;
  if (model.key === 'skewed' && model.hot > 1.5 / model.partitions && model.perPartition / model.hot < demand / HEADROOM) {
    out.push(h.fixWith(h.graph, node, [['partition_key', 'id']], `${label}: ${pct(model.hot)} of messages share one key, so one partition takes them all.`,
      'Key by something with many evenly used values (such as the order or user id), so messages spread over every partition.'));
  }
  if ((Number(p('linger_ms')) || 0) < 5 || p('compression') === 'none') {
    out.push(h.fixWith(h.graph, node, [['linger_ms', Math.max(10, Number(p('linger_ms')) || 0)], ['compression', p('compression') === 'none' ? 'lz4' : p('compression')]],
      `${label}: producers send small, uncompressed batches.`, 'Batch for 10 ms (linger.ms) and compress with lz4: the same brokers carry far more messages.'));
  }
  if (lim.partitionLimit <= lim.brokerLimit) {
    const need = niceCount(Math.max(model.partitions + 1, demand / (model.perPartition * HEADROOM)));
    out.push(h.fixWith(h.graph, node, [['partitions', need]], `${label}: ${model.partitions} partitions take about ${fmtCount(lim.partitionLimit)} messages a second; this test sends ${fmtCount(demand)}.`, `Use ${need} partitions.`));
  } else {
    const need = Math.max(brokers + 1, Math.ceil(demand / (model.brokerCap * HEADROOM)));
    out.push(h.fixWith(h.graph, node, [['brokers', need]], `${label}: ${brokers} brokers write about ${fmtCount(lim.brokerLimit)} messages a second with ${rf} copies each; this test sends ${fmtCount(demand)}.`, `Run ${need} brokers.`));
  }
  return out;
}

function consumerFixes(node, m, topics, h, failure) {
  const p = (k) => param(node, k);
  const label = node.label;
  const out = [];
  const k = m.kafka;
  // In a failure test one instance is gone, so what runs is one less than what is configured.
  const lost = failure?.node_id === node.id ? 1 : 0;
  if (lost && node.type === 'StreamProcessor' && p('state') === 'stateful' && !(Number(p('standby_replicas')) > 0)) {
    out.push(h.fixWith(h.graph, node, [['standby_replicas', 1]], `${label}: the instance that died held state that must be rebuilt from its changelog topic, so its partitions wait for minutes.`,
      'Keep a standby replica of every state store on another instance, so a failover takes seconds.'));
  }
  if (k.rebalancing && node.type === 'ConsumerGroup') {
    const next = Math.max(10, Math.floor((Number(p('max_poll_records')) || 500) / 5));
    out.push(h.fixWith(h.graph, node, [['max_poll_records', next]], `${label}: one poll of ${p('max_poll_records')} messages takes longer than max.poll.interval.ms, so Kafka keeps evicting consumers and rebalancing.`,
      `Fetch ${next} messages per poll, so each batch finishes well within the interval.`));
  }
  if (!(k.lag_rps > 0.5) && !(m.utilization > 0.85)) return out;
  const P = topics.reduce((s, t) => s + (Number(param(t, 'partitions')) || 1), 0);
  if (!P) return out;
  const running = Math.max(1, m.instances || 1);
  const active = Math.min(running, P);
  const units = P / Math.ceil(P / active);
  const needUnits = Math.ceil((units * Math.min(m.utilization, 9.99)) / HEADROOM);
  const autoscaled = !!p('autoscaling');
  const behind = `${label}: ${fmtCount(m.demand_rps)} messages a second arrive, but it processes about ${fmtCount(m.capacity_rps ?? 0)}.`;
  const hot = topics.find((t) => param(t, 'partition_key') === 'skewed' && (Number(param(t, 'hot_key_share')) || 0) > 1.5 / (Number(param(t, 'partitions')) || 1));
  if (hot) {
    out.push(h.fixWith(h.graph, hot, [['partition_key', 'id']], `${label}: the hot partition of ${hot.label} goes to one consumer, which no number of consumers can relieve.`,
      `Key ${hot.label} by a well-spread id, so the load spreads over every consumer.`));
  }
  let count = active;
  while (count < P && P / Math.ceil(P / count) < needUnits) count++;
  const spare = lost ? ', so losing one still leaves enough' : '';
  if (P / Math.ceil(P / count) >= needUnits && count + lost <= P) {
    const target = count + lost;
    if (autoscaled) {
      const max = Number(p('autoscale_max')) || 1;
      const min = Number(p('autoscale_min')) || 1;
      if (target > min) {
        out.push(h.fixWith(h.graph, node, [['autoscale_min', target], ['autoscale_max', Math.max(max, target)]], behind, `Keep at least ${target} consumers running${spare}.`));
      }
    } else {
      const configured = Number(p('replicas')) || 1;
      const next = Math.max(target, count > running ? configured + 1 : configured);
      const why = P % next === 0 ? '' : ` With ${P} partitions, ${next} consumers spread them most evenly.`;
      if (next > configured) out.push(h.fixWith(h.graph, node, [['replicas', next]], behind, `Run ${next} consumers${spare}.${why}`));
    }
    return out;
  }
  // More parallelism than the partitions allow: add partitions and consumers together. To
  // survive losing a consumer, give each one two partitions, so a crash spreads its share thinly.
  const parts = niceCount(lost ? 2 * needUnits : needUnits);
  const consumers = lost ? needUnits + lost : parts;
  const edits = topics.map((t) => [t, [['partitions', Math.max(Number(param(t, 'partitions')) || 1, Math.ceil((parts * (Number(param(t, 'partitions')) || 1)) / P))]]]);
  edits.push([node, autoscaled
    ? [['autoscale_min', Math.max(consumers, Number(p('autoscale_min')) || 1)], ['autoscale_max', Math.max(consumers, Number(p('autoscale_max')) || 1)]]
    : [['replicas', consumers]]]);
  const names = topics.map((t) => t.label).join(' and ');
  const cause = lost
    ? `${behind} With one partition per consumer, the crashed consumer's partition doubles someone else's load.`
    : `${behind} It already has a consumer for every partition of ${names}, and extra consumers would sit idle.`;
  out.push(h.fixMany(h.graph, edits, cause, `Give ${names} ${parts} partitions and run ${consumers} consumers${spare}.`));
  return out;
}

export function kafkaFixes(graph, r, testCase, helpers) {
  const h = { ...helpers, graph };
  const idx = new GraphIndex(graph);
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const out = [];
  const push = (f) => { if (f && f.graph) out.push(f); };
  for (const v of r.violations || []) violationFixes(v, byId.get(v.id), testCase, h, idx).forEach(push);
  const k = r.sim.kafka;
  if (!k) return out;
  const metrics = r.sim.node_metrics;
  if (k.stuck_rate > 0) {
    if (testCase.kafka?.schema_break) {
      push(registryFix(graph));
    } else {
      for (const m of metrics.filter((x) => x.kafka?.stuck_share > 0)) {
        const node = byId.get(m.id);
        if (['ConsumerGroup', 'KafkaConnect', 'StreamProcessor'].includes(node.type)) {
          push(h.fixWith(graph, node, [['dead_letter_topic', true]], `${node.label}: a message that always fails blocks its partition, and everything behind it waits.`,
            'Send messages that keep failing to a dead letter topic, so the rest keep flowing (and alert on it).'));
        } else h.advice.add(`${node.label} needs a way to skip a message it cannot process: park it in a dead letter topic.`);
      }
    }
  }
  if (k.data_loss_rate > 0) {
    for (const t of k.topics) {
      if (!t.groups.some((g) => g.loss_share > 0)) continue;
      push(retentionFix(byId.get(t.id), testCase.kafka?.horizon_hours || 24, `${t.label}: unread messages are deleted after ${Math.round(t.retention_sec / 3600)} hours, before slow consumers get to them.`, h));
    }
  }
  for (const m of metrics.filter((x) => x.type === 'KafkaTopic' && ['overloaded', 'hot', 'down'].includes(x.status))) topicFixes(byId.get(m.id), m, h).forEach(push);
  // Tests that let consumers fall behind for a while (spikes) gain nothing from faster consumers.
  if (testCase.ignore_freshness) return out;
  for (const m of metrics.filter((x) => x.kafka?.role === 'consumer' && !(x.kafka.stuck_share > 0) && x.status !== 'down')) {
    const topics = idx.in(m.id).filter((e) => idx.type(e.source) === 'KafkaTopic').map((e) => byId.get(e.source));
    consumerFixes(byId.get(m.id), m, topics, h, r.failure).forEach(push);
  }
  return out;
}

// Short reasons a Kafka test failed, for the "why it failed" list.
export function kafkaReasons(r, freshnessTarget) {
  const out = [];
  const k = r.sim?.kafka;
  for (const v of r.violations || []) out.push(v.text);
  if (!k) return out;
  if (k.stuck_rate > 0) out.push(`${pct(k.stuck_rate)} of messages are stuck behind messages consumers cannot handle.`);
  if (k.data_loss_rate > 0) out.push(`${pct(Math.max(0.01, k.data_loss_rate))} of messages expired before consumers read them.`);
  if (freshnessTarget && (k.freshness_ms === null || k.freshness_ms > freshnessTarget)) {
    out.push(k.freshness_ms === null ? 'Consumers never catch up with the newest messages.' : `Consumers are ${Math.round(k.freshness_ms / 1000)} s behind, over the ${Math.round(freshnessTarget / 1000)} s freshness target.`);
  }
  return out;
}

export const isKafkaPart = (type) => type === 'KafkaTopic' || KAFKA_CONSUMERS.has(type) || type === 'SchemaRegistry';

// ================================================================ test scenarios
export const DEFAULT_FAILURE_ORDER = ['Cache', 'SearchIndex', 'Queue', 'Worker', 'Service', 'Server', 'Database', 'LoadBalancer'];
const GROUP_PARENTS = new Set(['LoadBalancer', 'APIGateway', 'ReverseProxy', 'Queue']);
// A test that breaks a delivery requirement cannot pass, however fast the design is.
const VIOLATION_CAP = 40;

export function isRedundant(graph, node, idx = new GraphIndex(graph)) {
  const p = (k) => param(node, k);
  if (node.type === 'Database') return (Number(p('read_replicas')) || 0) > 0 && !!p('failover');
  if (node.type === 'SearchIndex') return (Number(p('replicas')) || 0) > 0;
  if (node.type === 'KafkaTopic') return (Number(p('brokers')) || 1) >= 2 && (Number(p('replication_factor')) || 1) >= 2;
  const inst = p('autoscaling') ? Math.max(2, Number(p('autoscale_min')) || 1) : Number(p('replicas')) || 1;
  if (inst >= 2) return true;
  return idx.in(node.id).some((e) => GROUP_PARENTS.has(idx.type(e.source))
    && idx.out(e.source).some((x) => x.target !== node.id && idx.type(x.target) === node.type));
}

// Prefer a component that has redundancy (so the test shows failover working), then fall
// back to a single point of failure. Busier components are chosen first.
export function pickFailureTarget(graph, prefer = DEFAULT_FAILURE_ORDER, baseline = null) {
  const idx = new GraphIndex(graph);
  const load = (id) => (baseline ? baseline.node_metrics.find((n) => n.id === id)?.demand_rps || 0 : 1);
  for (const redundantOnly of [true, false]) {
    for (const type of prefer) {
      const candidates = idx.ofType(type)
        .filter((n) => load(n.id) > 0 && (!redundantOnly || isRedundant(graph, n, idx)))
        .sort((a, b) => load(b.id) - load(a.id));
      if (candidates.length) return { node: candidates[0], redundant: isRedundant(graph, candidates[0], idx) };
    }
  }
  return { node: null, redundant: false };
}

export function failureOverrides(node) {
  const p = (k) => param(node, k);
  if (node.type === 'Database') return { [node.id]: { down: true } };
  if (node.type === 'SearchIndex') return { [node.id]: (Number(p('replicas')) || 0) > 0 ? { instancesDelta: -1 } : { down: true } };
  if (node.type === 'KafkaTopic') return { [node.id]: (Number(p('brokers')) || 1) >= 2 ? { instancesDelta: -1 } : { down: true } };
  const inst = Number(p('replicas')) || 1;
  if (inst >= 2 || p('autoscaling')) return { [node.id]: { instancesDelta: -1 } };
  return { [node.id]: { down: true } };
}

export function problemTraffic(problem, multiplier = 1) {
  const t = problem?.traffic || {};
  return {
    users: t.users,
    requests_per_user: t.requests_per_user,
    abusive_share: t.abusive_share || 0,
    multiplier,
  };
}

export function summarizeSim(sim) {
  return {
    error_rate: sim.error_rate,
    throughput_rps: sim.throughput_rps,
    incoming_rps: sim.incoming_rps,
    blocked_rps: sim.blocked_rps,
    latency_ms: sim.latency_ms,
    max_utilization: sim.max_utilization,
    bottleneck_ids: sim.bottleneck_ids,
    backlog_rps: sim.async.backlog_rps,
    kafka: sim.kafka ? {
      lag_rps: sim.kafka.lag_rps,
      freshness_ms: sim.kafka.freshness_ms,
      data_loss_rate: sim.kafka.data_loss_rate,
      stuck_rate: sim.kafka.stuck_rate,
    } : null,
  };
}

// Plain-language summary of what a Kafka scenario did, for the test note.
function kafkaNote(tc, sim) {
  const k = sim.kafka;
  if (!k) return '';
  const parts = [];
  if (k.stuck_rate > 0) parts.push(`${Math.round(k.stuck_rate * 100)}% of messages are stuck and never processed`);
  if (k.data_loss_rate > 0) parts.push(`${Math.round(Math.max(0.01, k.data_loss_rate) * 100)}% of messages expired unread`);
  else if (k.lag_rps > 0.5) parts.push(`consumers fall behind by ${fmtCount(k.lag_rps)} msg/s`);
  if (!tc.ignore_freshness && k.freshness_ms !== null && k.freshness_ms > 2000) parts.push(`consumed data is ${fmtDuration(k.freshness_ms / 1000)} old`);
  if (k.freshness_ms === null && !k.stuck_rate) parts.push('consumers process nothing');
  return parts.length ? ` Kafka: ${parts.join('; ')}.` : '';
}

export function runTestCase(graph, problem, tc, { baseline = null } = {}) {
  const multiplier = tc.scenario === 'spike' ? tc.multiplier || 3 : tc.multiplier || 1;
  const traffic = { ...problemTraffic(problem, multiplier), async_multiplier: tc.async_multiplier || 1 };
  let overrides = {};
  let failure = null;
  let note = tc.scenario === 'spike' ? `Traffic at ${multiplier}× the normal peak.` : 'Normal peak traffic.';
  if (tc.typeOverrides) {
    // `where` narrows an override to parts with matching settings, e.g. only the SMS provider.
    for (const n of graph.nodes) {
      const spec = tc.typeOverrides[n.type];
      if (!spec) continue;
      const { where, ...ov } = spec;
      if (where && !Object.entries(where).every(([k, v]) => param(n, k) === v)) continue;
      overrides[n.id] = ov;
    }
    note = tc.description || note;
  } else if (tc.scenario === 'failure') {
    const { node, redundant } = pickFailureTarget(graph, tc.prefer || DEFAULT_FAILURE_ORDER, baseline);
    if (node) {
      overrides = failureOverrides(node);
      failure = { node_id: node.id, label: node.label, type: node.type, redundant };
      note = redundant
        ? `${node.label} lost a copy. Redundancy kept the path alive; what matters now is whether the survivors have enough headroom.`
        : `${node.label} failed and nothing could take over, which makes it a single point of failure.`;
    } else {
      note = `The design has no ${(tc.prefer || []).join(' or ') || 'eligible component'} to fail, so this ran at normal load.`;
    }
  }
  const horizonSec = tc.kafka?.horizon_hours ? tc.kafka.horizon_hours * 3600 : undefined;
  const sim = simulate(graph, { traffic, overrides, kafka: tc.kafka, horizonSec });
  const score = scoreSimulation(sim, {
    latencyTarget: problem?.scale?.latency_target_ms || 300,
    freshnessTarget: tc.ignore_freshness ? 0 : problem?.scale?.freshness_target_ms || 0,
  });
  const violations = guaranteeViolations(sim, tc.requires);
  if (violations.length) {
    score.total = Math.min(score.total, VIOLATION_CAP);
    score.pass = false;
  }
  note += kafkaNote(tc, sim);
  return {
    id: tc.id, name: tc.name, tier: tc.tier, scenario: tc.scenario, weight: tc.weight ?? 1, multiplier,
    description: tc.description || '',
    score: score.total, pass: score.pass, parts: score.parts,
    failure, note,
    requires: (tc.requires || []).map((r) => ({ id: r, label: REQUIREMENT_LABELS[r] || r, met: !violations.some((v) => v.requirement === r) })),
    violations,
    metrics: summarizeSim(sim),
    bottlenecks: sim.bottleneck_ids.map((id) => sim.node_metrics.find((n) => n.id === id)?.label || id),
    sim,
  };
}

export function runSuite(graph, problem, { tiers = ['component'] } = {}) {
  const baseline = simulate(graph, { traffic: problemTraffic(problem, 1) });
  const cases = (problem.tests || []).filter((t) => tiers.includes(t.tier));
  const results = cases.map((tc) => runTestCase(graph, problem, tc, { baseline }));
  const weightSum = results.reduce((s, r) => s + r.weight, 0) || 1;
  const score = Math.round(results.reduce((s, r) => s + r.score * r.weight, 0) / weightSum);
  return {
    baseline,
    results,
    summary: { passed: results.filter((r) => r.pass).length, total: results.length, score },
  };
}

// ================================================================ design rules
const COMPUTE = new Set(['Server', 'Service', 'WebSocketServer']);
const FRONT_DOOR = new Set(['CDN', 'LoadBalancer', 'ReverseProxy', 'RateLimiter', 'APIGateway']);
const STORE_TYPES = new Set(['Database', 'Cache', 'SearchIndex', 'BlobStorage']);
const STORE_ALLOWED_TARGETS = {
  Cache: new Set(['Database', 'SearchIndex', 'BlobStorage']),
  Database: new Set(['SearchIndex', 'Queue', 'KafkaConnect']),
  BlobStorage: new Set(['Queue']),
  SearchIndex: new Set(),
};
const PENALTY = { critical: 25, warning: 10, info: 3 };
const SEVERITY_RANK = { critical: 0, warning: 1, info: 2 };
const NON_CRITICAL_PROVIDERS = new Set(['email', 'sms', 'push']);

export function analyzeDesign(graph, { problem = null, sim = null } = {}) {
  const idx = new GraphIndex(graph);
  const findings = [];
  const strengths = [];
  const label = (id) => idx.node(id)?.label || id;
  const typeLabel = (t) => COMPONENTS[t]?.label || t;
  const add = (f) => findings.push({ node_ids: [], ...f });
  const p = (id, key) => param(idx.node(id), key);
  const isAsync = (e) => e.kind === 'async' || ['Queue', 'KafkaTopic'].includes(idx.type(e.target))
    || (idx.type(e.source) === 'Database' && idx.type(e.target) === 'KafkaConnect');
  const syncOut = (id) => idx.out(id).filter((e) => !isAsync(e));
  const instances = (id) => (p(id, 'autoscaling') ? Math.max(Number(p(id, 'autoscale_max')) || 1, 1) : Number(p(id, 'replicas')) || 1);

  const users = idx.users();
  const userIds = users.map((u) => u.id);
  const reach = idx.reachableFrom(userIds);
  const syncReach = new Set();
  {
    const stack = [...userIds];
    while (stack.length) {
      const id = stack.pop();
      if (syncReach.has(id)) continue;
      syncReach.add(id);
      for (const e of syncOut(id)) stack.push(e.target);
    }
  }
  const nodesOfType = (t) => graph.nodes.filter((n) => n.type === t);

  // ---- Structure -------------------------------------------------------------
  if (!users.length) {
    add({ code: 'NO_USERS', severity: 'critical', category: 'structure', title: 'No traffic source',
      reason: 'Without a Users node the simulator has nowhere to send traffic from.',
      impact: 'Nothing can be simulated or scored.',
      recommendation: 'Drag a Users node onto the canvas and connect it to your entry point.' });
  }
  for (const u of users) {
    if (!idx.out(u.id).length) {
      add({ code: 'USERS_NOT_CONNECTED', severity: 'critical', category: 'structure', title: 'Users are not connected',
        reason: `${u.label} has no outgoing connection, so its requests go nowhere.`,
        impact: 'No traffic reaches your system.', recommendation: 'Connect the Users node to a CDN, load balancer, gateway or server.', node_ids: [u.id] });
    }
  }
  const intoUsers = graph.edges.filter((e) => idx.type(e.target) === 'User');
  if (intoUsers.length) {
    add({ code: 'EDGE_INTO_USERS', severity: 'critical', category: 'structure', title: 'Connection points back at users',
      reason: 'Arrows show the direction a request travels. Responses flow back automatically, so nothing should point at a Users node.',
      impact: 'The traffic model cannot interpret this connection.', recommendation: 'Delete connections that end at a Users node.',
      node_ids: intoUsers.map((e) => e.source) });
  }
  const selfLoops = graph.edges.filter((e) => e.source === e.target);
  if (selfLoops.length) {
    add({ code: 'SELF_LOOP', severity: 'critical', category: 'structure', title: 'Component connected to itself',
      reason: 'A component calling itself creates infinite traffic in this model.', impact: 'The connection is ignored.',
      recommendation: 'Remove the self-connection.', node_ids: selfLoops.map((e) => e.source) });
  }
  const computeOnPath = [...syncReach].filter((id) => COMPUTE.has(idx.type(id)));
  const storesOnPath = [...syncReach].filter((id) => STORE_TYPES.has(idx.type(id)) && idx.type(id) !== 'BlobStorage');
  if (users.length && !computeOnPath.length && (problem || storesOnPath.length)) {
    add({ code: 'NO_COMPUTE', severity: 'critical', category: 'structure', title: 'No application tier on the request path',
      reason: 'Requests never reach a server or service that can run business logic, validate input or enforce permissions.',
      impact: 'Clients would have to talk to storage directly.', recommendation: 'Put an App Server or Service between your entry point and your data stores.' });
  }

  // ---- Topology ----------------------------------------------------------------
  const unreachable = graph.nodes.filter((n) => n.type !== 'User' && !reach.has(n.id));
  if (users.length && unreachable.length) {
    add({ code: 'UNREACHABLE_COMPONENT', severity: 'warning', category: 'topology', title: `${unreachable.length === 1 ? 'A component is' : `${unreachable.length} components are`} never reached`,
      reason: `${unreachable.map((n) => n.label).join(', ')} ${unreachable.length === 1 ? 'has' : 'have'} no path from your users, so ${unreachable.length === 1 ? 'it does' : 'they do'} nothing in the simulation.`,
      impact: 'Unused components add cost and confuse reviewers.', recommendation: 'Connect them into a request or async path, or delete them.',
      node_ids: unreachable.map((n) => n.id) });
  }
  if (idx.topo().back.size) {
    add({ code: 'CYCLE', severity: 'warning', category: 'topology', title: 'Circular dependency',
      reason: 'Some components call each other in a loop. The simulator ignores the edge that closes the loop.',
      impact: 'Loops make failures bounce around and are hard to reason about.', recommendation: 'Break the loop with an async event (queue) or remove one direction.' });
  }
  for (const u of users) {
    for (const e of idx.out(u.id)) {
      const t = idx.type(e.target);
      if (['Database', 'Cache', 'SearchIndex', 'Queue', 'KafkaTopic'].includes(t)) {
        add({ code: 'CLIENT_TO_DATASTORE', severity: 'critical', category: 'security', title: `Clients talk to the ${typeLabel(t).toLowerCase()} directly`,
          reason: 'Exposing a data store to clients means anyone can read or change data without your business rules or access checks.',
          impact: 'Data leaks, abuse and no place to evolve your schema safely.', recommendation: `Route clients through an application tier that owns access to ${label(e.target)}.`, node_ids: [e.target] });
      } else if (t === 'Service' || t === 'Worker') {
        add({ code: 'CLIENT_TO_INTERNAL', severity: 'warning', category: 'topology', title: 'Clients call an internal component directly',
          reason: `${label(e.target)} is reachable straight from clients, skipping the gateway or load balancer.`,
          impact: 'Every internal service has to handle auth, rate limits and TLS on its own.', recommendation: 'Put an API gateway or load balancer in front of internal services.', node_ids: [e.target] });
      }
    }
  }
  for (const n of graph.nodes) {
    if (!STORE_TYPES.has(n.type)) continue;
    const allowed = STORE_ALLOWED_TARGETS[n.type];
    const bad = idx.out(n.id).filter((e) => !allowed.has(idx.type(e.target)));
    if (bad.length) {
      add({ code: 'DATASTORE_CALLS_OUT', severity: 'warning', category: 'topology', title: `${n.label} initiates calls`,
        reason: 'Data stores answer queries; they do not call application components. The arrow probably points the wrong way.',
        impact: 'The traffic model treats this as an extra request path.', recommendation: 'Point arrows from the caller to the data store.', node_ids: [n.id] });
    }
  }

  // Compute redundancy and load balancing
  const routerParents = (id) => idx.in(id).filter((e) => ['LoadBalancer', 'APIGateway', 'ReverseProxy'].includes(idx.type(e.source)));
  for (const id of computeOnPath) {
    const siblings = routerParents(id).flatMap((e) => idx.out(e.source).map((x) => x.target)).filter((t) => t !== id && idx.type(t) === idx.type(id));
    if (instances(id) < 2 && !siblings.length) {
      add({ code: 'SINGLE_INSTANCE', severity: 'warning', category: 'reliability', title: `${label(id)} runs as a single instance`,
        reason: 'One instance is both a capacity ceiling and a single point of failure: a deploy or crash takes the path down.',
        impact: 'Downtime during restarts and no headroom for spikes.', recommendation: 'Run two or more instances behind a load balancer, or enable autoscaling.', node_ids: [id] });
    }
  }
  for (const u of users) {
    const direct = idx.out(u.id).filter((e) => COMPUTE.has(idx.type(e.target)));
    const multi = direct.length >= 2 || direct.some((e) => instances(e.target) >= 2);
    if (multi) {
      add({ code: 'NO_LOAD_BALANCER', severity: 'warning', category: 'scalability', title: 'Several servers but no load balancer',
        reason: 'Clients are connected straight to your servers, so nothing spreads load evenly or removes a dead instance from rotation.',
        impact: 'Uneven load and errors whenever an instance dies.', recommendation: 'Add a load balancer between users and the server pool.', node_ids: direct.map((e) => e.target) });
    }
  }

  // Data stores
  for (const db of nodesOfType('Database')) {
    if (!reach.has(db.id)) continue;
    const replicas = Number(p(db.id, 'read_replicas')) || 0;
    if (replicas === 0) {
      add({ code: 'SINGLE_DATABASE', severity: 'warning', category: 'reliability', title: `${db.label} has no replica`,
        reason: 'With a single copy, a crash or a bad disk takes every read and write down until someone restores it.',
        impact: 'Full outage and possible data loss on failure.', recommendation: 'Add at least one read replica and turn on automatic failover.', node_ids: [db.id] });
    } else if (!p(db.id, 'failover')) {
      add({ code: 'NO_FAILOVER', severity: 'info', category: 'reliability', title: `${db.label} has replicas but no failover`,
        reason: 'If the primary dies, reads keep working on replicas but every write fails until someone promotes one by hand.',
        impact: 'Write outage measured in minutes to hours.', recommendation: 'Enable automatic failover.', node_ids: [db.id] });
    }
    if (!p(db.id, 'indexed')) {
      add({ code: 'DB_UNINDEXED', severity: 'warning', category: 'performance', title: `${db.label} scans tables`,
        reason: 'Queries without indexes read every row, so capacity collapses as data grows.', impact: 'About 4× less throughput and 5× slower queries.',
        recommendation: 'Index the columns your queries filter and sort on.', node_ids: [db.id] });
    }
  }

  const frontsStore = (cacheId) => {
    if (idx.out(cacheId).some((e) => ['Database', 'SearchIndex', 'BlobStorage'].includes(idx.type(e.target)))) return true;
    return idx.in(cacheId).some((e) => idx.out(e.source).some((x) => x.target !== cacheId && ['Database', 'SearchIndex', 'BlobStorage'].includes(idx.type(x.target))));
  };
  for (const c of nodesOfType('Cache')) {
    if (!reach.has(c.id)) continue;
    const fronts = frontsStore(c.id);
    if (!fronts) {
      add({ code: 'CACHE_WITHOUT_STORE', severity: 'info', category: 'data', title: `${c.label} is not in front of a store`,
        reason: 'A cache only helps when it saves trips to something slower. Nothing behind this cache is shielded.',
        impact: 'Extra hop with no benefit.', recommendation: 'Have the same caller use the cache before the database (cache-aside), or connect the cache to the store it fronts.', node_ids: [c.id] });
    }
    if (fronts && !p(c.id, 'request_coalescing') && (Number(p(c.id, 'replicas')) || 1) < 2) {
      add({ code: 'CACHE_STAMPEDE_RISK', severity: 'warning', category: 'reliability', title: `${c.label} restart would stampede the database`,
        reason: 'If this single cache node restarts, every request misses at the same moment and the same hot keys are fetched from the database many times over.',
        impact: 'A cache blip turns into a database outage.', recommendation: 'Turn on request coalescing, or run two or more cache nodes.', node_ids: [c.id] });
    }
    if ((Number(p(c.id, 'ttl_seconds')) || 0) < 30) {
      add({ code: 'CACHE_LOW_TTL', severity: 'info', category: 'performance', title: `${c.label} expires entries very quickly`,
        reason: 'Entries that live only a few seconds are gone before most repeat reads arrive.', impact: 'The real hit ratio is far below the configured one.',
        recommendation: 'Raise the TTL and invalidate on writes instead of relying on short expiry.', node_ids: [c.id] });
    }
  }

  // Async
  for (const q of nodesOfType('Queue')) {
    const consumers = idx.out(q.id);
    if (!consumers.length) {
      add({ code: 'QUEUE_NO_CONSUMER', severity: 'critical', category: 'async', title: `Nothing consumes ${q.label}`,
        reason: 'Messages are produced but no worker or service reads them, so the backlog grows until the queue is full.',
        impact: 'Work is never done, then producers start failing.', recommendation: 'Connect the queue to a Worker (or Service) that processes its messages.', node_ids: [q.id] });
    }
    const syncIn = idx.in(q.id).filter((e) => e.kind === 'sync');
    if (syncIn.length) {
      add({ code: 'QUEUE_SYNC_EDGE', severity: 'info', category: 'async', title: `Connection into ${q.label} is marked synchronous`,
        reason: 'Putting a message on a queue is fire-and-forget; the producer does not wait for the work. The simulator treats it as async.',
        impact: 'None in the simulation, but the diagram reads ambiguously.', recommendation: 'Mark connections into queues as async.', node_ids: syncIn.map((e) => e.source) });
    }
    const consumerIds = consumers.map((e) => e.target);
    if (p(q.id, 'delivery') === 'at_least_once') {
      const unsafe = consumerIds.filter((id) => idx.type(id) === 'Worker' && !p(id, 'idempotent'));
      if (unsafe.length) {
        add({ code: 'NON_IDEMPOTENT_CONSUMER', severity: 'warning', category: 'async', title: 'Jobs can run twice',
          reason: `${q.label} delivers at least once: after a crash or timeout the same message comes back. ${unsafe.map(label).join(', ')} ${unsafe.length === 1 ? 'is' : 'are'} not idempotent.`,
          impact: 'Duplicate emails, double charges or double-counted events.', recommendation: 'Make processing idempotent (dedupe on a message or idempotency key).', node_ids: unsafe });
      }
    }
    if (!p(q.id, 'dead_letter_queue') && consumerIds.some((id) => (Number(p(id, 'retry_limit')) || 0) > 0)) {
      add({ code: 'NO_DLQ', severity: 'info', category: 'async', title: `${q.label} has no dead-letter queue`,
        reason: 'A message that always fails is retried forever and blocks or slows everything behind it.', impact: 'Poison messages stall the pipeline.',
        recommendation: 'Enable a dead-letter queue and alert on it.', node_ids: [q.id] });
    }
    const consumerInstances = consumerIds.reduce((s, id) => s + instances(id), 0);
    if (p(q.id, 'fifo') && consumerInstances > (Number(p(q.id, 'partitions')) || 1)) {
      add({ code: 'FIFO_LIMITS_PARALLELISM', severity: 'info', category: 'async', title: 'Ordering caps consumer parallelism',
        reason: `With strict ordering only one consumer per partition can work at a time; ${consumerInstances} consumer instances share ${p(q.id, 'partitions')} partitions.`,
        impact: 'Extra workers sit idle.', recommendation: 'Add partitions (keyed by the entity that needs ordering) or drop strict ordering.', node_ids: [q.id] });
    }
  }
  for (const w of nodesOfType('Worker')) {
    const syncFeeds = idx.in(w.id).filter((e) => !['Queue', 'KafkaTopic'].includes(idx.type(e.source)) && !isAsync(e));
    if (syncFeeds.length) {
      add({ code: 'WORKER_FED_SYNCHRONOUSLY', severity: 'warning', category: 'async', title: `${w.label} is called synchronously`,
        reason: 'The caller waits while background work runs, so slow jobs show up as slow requests and a busy worker pool rejects user traffic.',
        impact: 'Higher latency and errors during bursts.', recommendation: 'Put a queue between the caller and the worker.', node_ids: [w.id] });
    }
  }

  // External calls and resilience
  for (const n of graph.nodes) {
    if (!['Server', 'Service', 'Worker'].includes(n.type)) continue;
    const deps = syncOut(n.id);
    for (const e of deps) {
      if (idx.type(e.target) !== 'ThirdPartyAPI') continue;
      const tp = e.target;
      if (!p(n.id, 'circuit_breaker') && !p(tp, 'circuit_breaker')) {
        add({ code: 'UNPROTECTED_THIRD_PARTY', severity: 'warning', category: 'reliability', title: `Calls to ${label(tp)} have no circuit breaker`,
          reason: 'When the provider slows down or fails, every caller keeps waiting and retrying instead of failing fast.',
          impact: 'A provider incident becomes your incident.', recommendation: `Enable a circuit breaker on ${label(tp)} (or on ${n.label}) and keep a short timeout.`, node_ids: [n.id, tp] });
      }
    }
    if ((n.type === 'Server' || n.type === 'Service') && deps.length) {
      if ((Number(p(n.id, 'retry_limit')) || 0) >= 3 && !p(n.id, 'circuit_breaker')) {
        add({ code: 'RETRY_STORM_RISK', severity: 'warning', category: 'reliability', title: `${n.label} retries aggressively`,
          reason: `${p(n.id, 'retry_limit')} retries without a circuit breaker can multiply load several times over on a dependency that is already struggling.`,
          impact: 'A small slowdown snowballs into an outage.', recommendation: 'Cap retries at 1–2 with backoff and add a circuit breaker.', node_ids: [n.id] });
      }
      if ((Number(p(n.id, 'timeout_ms')) || 0) === 0) {
        add({ code: 'NO_TIMEOUT', severity: 'info', category: 'reliability', title: `${n.label} waits forever on dependencies`,
          reason: 'Without a timeout, one slow dependency holds threads until the whole server stops accepting work.',
          impact: 'Slowdowns cascade upstream.', recommendation: 'Set a timeout a little above the dependency\'s normal p99.', node_ids: [n.id] });
      }
    }
    if (deps.length > 5) {
      add({ code: 'HIGH_FANOUT', severity: 'info', category: 'performance', title: `${n.label} calls ${deps.length} dependencies per request`,
        reason: 'Every synchronous dependency adds latency and another way for the request to fail.', impact: 'Tail latency grows with each call.',
        recommendation: 'Parallelise independent calls, cache results, or move non-essential work to a queue.', node_ids: [n.id] });
    }
  }
  const onPathThirdParty = [...syncReach].filter((id) => idx.type(id) === 'ThirdPartyAPI' && NON_CRITICAL_PROVIDERS.has(p(id, 'provider')));
  for (const id of onPathThirdParty) {
    add({ code: 'NOTIFICATION_ON_REQUEST_PATH', severity: 'warning', category: 'async', title: `Users wait for ${label(id)}`,
      reason: `Sending ${p(id, 'provider')} messages is not needed to answer the request, yet the request waits for the provider.`,
      impact: 'Provider slowness becomes user-facing latency and errors.', recommendation: 'Publish an event to a queue and let a worker send it.', node_ids: [id] });
  }

  // Longest synchronous chain
  {
    const memo = new Map();
    const depth = (id, stack = new Set()) => {
      if (memo.has(id)) return memo.get(id);
      if (stack.has(id)) return 0;
      stack.add(id);
      const d = 1 + Math.max(0, ...syncOut(id).map((e) => depth(e.target, stack)));
      stack.delete(id);
      memo.set(id, d);
      return d;
    };
    const longest = Math.max(0, ...userIds.map((id) => depth(id) - 1));
    if (longest > 7) {
      add({ code: 'DEEP_SYNC_CHAIN', severity: 'info', category: 'performance', title: `Requests pass through ${longest} synchronous hops`,
        reason: 'Each hop adds latency and a failure point.', impact: 'High tail latency.', recommendation: 'Collapse hops, cache earlier, or make parts async.' });
    }
  }

  // Edge and traffic layer
  for (const c of nodesOfType('CDN')) {
    if (p(c.id, 'cache_api_responses')) {
      add({ code: 'CDN_CACHES_PERSONALIZED', severity: 'warning', category: 'data', title: `${c.label} caches API responses`,
        reason: 'Personalised responses cached at the edge can be served to the wrong user or stay stale for the whole TTL.',
        impact: 'Privacy incidents and stale data.', recommendation: 'Cache static and public content only; mark user-specific responses private.', node_ids: [c.id] });
    }
  }
  for (const lb of nodesOfType('LoadBalancer')) {
    if (!p(lb.id, 'health_checks')) {
      add({ code: 'LB_NO_HEALTH_CHECKS', severity: 'warning', category: 'reliability', title: `${lb.label} has health checks off`,
        reason: 'Without health checks the load balancer keeps sending a share of traffic to dead backends.', impact: 'Errors for every request routed to a failed instance.',
        recommendation: 'Enable health checks.', node_ids: [lb.id] });
    }
    if (p(lb.id, 'sticky_sessions')) {
      add({ code: 'LB_STICKY_SESSIONS', severity: 'info', category: 'scalability', title: `${lb.label} pins users to servers`,
        reason: 'Sticky sessions keep heavy users on one backend and lose their session when it dies.', impact: 'Uneven load.',
        recommendation: 'Keep servers stateless and store sessions in a shared cache.', node_ids: [lb.id] });
    }
  }
  const wsNodes = nodesOfType('WebSocketServer');
  const wsInstances = wsNodes.reduce((s, n) => s + instances(n.id), 0);
  if (wsInstances >= 2 && !wsNodes.some((n) => idx.out(n.id).some((e) => ['Queue', 'Cache'].includes(idx.type(e.target))))) {
    add({ code: 'WEBSOCKET_NO_FANOUT', severity: 'warning', category: 'topology', title: 'Connection servers cannot reach each other',
      reason: 'With several WebSocket servers, the sender and the recipient are often connected to different instances. Nothing here relays messages between them.',
      impact: 'Messages only reach users on the same instance.', recommendation: 'Connect WebSocket servers to a pub/sub channel (queue or cache) that fans messages out.', node_ids: wsNodes.map((n) => n.id) });
  }

  // Problem requirements
  for (const req of problem?.required || []) {
    if (!nodesOfType(req.type).some((n) => reach.has(n.id))) {
      add({ code: 'MISSING_REQUIRED_COMPONENT', severity: 'warning', category: 'requirements', title: `No ${typeLabel(req.type)} in the design`,
        reason: req.why, impact: 'A stated requirement is not covered.', recommendation: `Add a ${typeLabel(req.type)} where it fits the request flow.` });
    }
  }

  const kafka = kafkaReview(graph, idx, { p, instances, problem });
  findings.push(...kafka.findings);

  // Simulation-derived findings
  if (sim?.kafka) {
    const k = sim.kafka;
    const freshTarget = problem?.scale?.freshness_target_ms;
    if (k.data_loss_rate > 0) {
      add({ code: 'KAFKA_DATA_LOSS', severity: 'critical', category: 'kafka', title: `${(Math.max(0.001, k.data_loss_rate) * 100).toFixed(1)}% of messages expire unread`,
        reason: 'A consumer group falls so far behind that messages pass the topic\'s retention time and are deleted before it reads them.',
        impact: 'Those events are gone for good.', recommendation: 'Give the slow consumers more capacity (and partitions), and keep retention longer than your worst outage.' });
    }
    if (k.stuck_rate > 0) {
      add({ code: 'KAFKA_STUCK', severity: 'critical', category: 'kafka', title: `${Math.round(k.stuck_rate * 100)}% of messages are stuck`,
        reason: 'Consumers cannot get past messages they fail to read or process, so those partitions stop moving.',
        impact: 'Work behind the bad messages never happens until someone intervenes.', recommendation: 'Add a dead letter topic for bad messages and a Schema Registry to block breaking format changes.' });
    }
    if (freshTarget && (k.freshness_ms === null || k.freshness_ms > freshTarget)) {
      add({ code: 'FRESHNESS_OVER_TARGET', severity: 'warning', category: 'kafka',
        title: k.freshness_ms === null ? 'Consumers never catch up' : `Consumed data is ${fmtDuration(k.freshness_ms / 1000)} old, over the ${fmtDuration(freshTarget / 1000)} target`,
        reason: 'Consumers process messages more slowly than they arrive, so the newest data they have handled keeps getting older.',
        impact: 'Dashboards, search results or notifications show stale information.', recommendation: 'Add consumers (up to the partition count), add partitions, or speed up the work each message needs.' });
    }
  }
  if (sim) {
    const target = problem?.scale?.latency_target_ms;
    for (const n of sim.node_metrics) {
      if (n.status === 'overloaded' && n.type === 'KafkaTopic') {
        add({ code: 'TOPIC_OVERLOADED', severity: 'critical', category: 'kafka', title: `${n.label} cannot take all writes`,
          reason: n.kafka?.write_fail > 0
            ? `${Math.round(n.kafka.write_fail * 100)}% of partitions cannot accept acks=all writes after the broker failure.`
            : `Producers send ${shortCount(n.demand_rps)} msg/s but the topic takes about ${shortCount(n.capacity_rps ?? 0)} msg/s.`,
          impact: 'Producers get errors, and requests that publish events fail.', recommendation: overloadAdvice(n.type), node_ids: [n.id] });
      } else if (n.status === 'overloaded') {
        add({ code: n.on_request_path ? 'COMPONENT_OVERLOADED' : 'WORKERS_FALLING_BEHIND', severity: n.on_request_path ? 'critical' : 'warning', category: 'performance',
          title: `${n.label} is overloaded`,
          reason: `It receives ${shortCount(n.demand_rps)} rps but can serve about ${shortCount(n.capacity_rps ?? 0)} rps.`,
          impact: n.on_request_path ? 'Requests above capacity fail and latency explodes.' : 'Background work piles up.',
          recommendation: overloadAdvice(n.type), node_ids: [n.id] });
      } else if (n.status === 'hot') {
        add({ code: 'NEAR_SATURATION', severity: 'warning', category: 'performance', title: `${n.label} is running hot`,
          reason: `Utilization is ${Math.round(n.utilization * 100)}%; latency climbs steeply past ~85% and a small spike tips it over.`,
          impact: 'No headroom for spikes.', recommendation: overloadAdvice(n.type), node_ids: [n.id] });
      }
      if (n.autoscaling && n.autoscaling.instances >= n.autoscaling.max && n.utilization > 0.85) {
        add({ code: 'AUTOSCALE_AT_CEILING', severity: 'info', category: 'scalability', title: `${n.label} hit its autoscaling ceiling`,
          reason: `It scaled to its maximum of ${n.autoscaling.max} instances and is still busy.`, impact: 'Further growth turns into errors.',
          recommendation: 'Raise the autoscaling maximum or remove the bottleneck behind it.', node_ids: [n.id] });
      }
    }
    for (const id of sim.async.overflowing) {
      add({ code: 'QUEUE_OVERFLOW', severity: 'critical', category: 'async', title: `${label(id)} fills up`,
        reason: 'Producers add messages faster than consumers drain them, and the queue reaches its maximum depth within the simulated window.',
        impact: 'Producers start getting errors and work is lost.', recommendation: 'Add consumers (or autoscale them) and size the queue for your longest expected backlog.', node_ids: [id] });
    }
    if (sim.error_rate > 0.05) {
      add({ code: 'HIGH_ERROR_RATE', severity: 'critical', category: 'performance', title: `${(sim.error_rate * 100).toFixed(1)}% of requests fail`,
        reason: 'Users see errors under this load.', impact: 'Broken user experience.', recommendation: 'Fix the overloaded or failed components highlighted on the canvas.' });
    } else if (sim.error_rate > 0.01) {
      add({ code: 'ELEVATED_ERROR_RATE', severity: 'warning', category: 'performance', title: `${(sim.error_rate * 100).toFixed(1)}% of requests fail`,
        reason: 'More than 1% of requests fail at this load.', impact: 'Noticeable errors.', recommendation: 'Add headroom to the busiest component.' });
    }
    if (target && sim.latency_ms.p95 > target) {
      add({ code: 'LATENCY_OVER_TARGET', severity: 'warning', category: 'performance', title: `p95 latency ${Math.round(sim.latency_ms.p95)} ms exceeds the ${target} ms target`,
        reason: 'The slowest 5% of requests take longer than the requirement allows.', impact: 'Requirement missed.',
        recommendation: 'Cache earlier in the path, cut synchronous hops, or relieve the hottest component.' });
    }
  }

  // ---- Security checks (scored separately) -------------------------------------
  const checks = [...securityChecks(graph, idx, { userIds, syncReach, p }), ...kafka.checks];
  for (const c of checks.filter((x) => x.applicable && !x.passed)) {
    add({ code: c.code, severity: 'warning', category: 'security', title: c.title, reason: c.reason, impact: c.impact, recommendation: c.recommendation, node_ids: c.node_ids || [] });
  }
  const applicable = checks.filter((c) => c.applicable);
  const securityScore = applicable.length ? Math.round((applicable.filter((c) => c.passed).length / applicable.length) * 100) : 100;

  // ---- Strengths ---------------------------------------------------------------
  const lbBalanced = nodesOfType('LoadBalancer').some((lb) => syncReach.has(lb.id) && (idx.out(lb.id).length >= 2 || idx.out(lb.id).some((e) => instances(e.target) >= 2)));
  if (lbBalanced) strengths.push('Compute sits behind a load balancer with more than one instance.');
  if (nodesOfType('Cache').some((c) => reach.has(c.id) && frontsStore(c.id))) strengths.push('A cache shields the database from repeated reads.');
  if (nodesOfType('Queue').some((q) => idx.out(q.id).length)) strengths.push('Slow or bursty work runs asynchronously through a queue.');
  if (nodesOfType('Database').some((d) => (Number(p(d.id, 'read_replicas')) || 0) > 0 && p(d.id, 'failover'))) strengths.push('The database has replicas and automatic failover.');
  if (nodesOfType('CDN').some((c) => syncReach.has(c.id))) strengths.push('A CDN serves cacheable content close to users.');
  if (checks.find((c) => c.code === 'SEC_RATE_LIMITING')?.passed) strengths.push('Rate limiting protects the system from abusive clients.');
  if (graph.nodes.some((n) => COMPUTE.has(n.type) && p(n.id, 'autoscaling'))) strengths.push('Compute autoscales with load.');
  strengths.push(...kafka.strengths);

  // ---- Score ------------------------------------------------------------------
  const scored = findings.filter((f) => f.category !== 'security');
  const counts = { critical: 0, warning: 0, info: 0 };
  for (const f of scored) counts[f.severity]++;
  const valid = !findings.some((f) => f.category === 'structure' && f.severity === 'critical');
  let total = 100 - (counts.critical * PENALTY.critical + counts.warning * PENALTY.warning + counts.info * PENALTY.info);
  total = Math.max(0, Math.min(100, total));
  if (!valid) total = Math.min(total, 30);

  findings.sort((a, b) => SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity]);
  const guidance = findings.slice(0, 6).map((f) => ({
    priority: f.severity === 'critical' ? 'High' : f.severity === 'warning' ? 'Medium' : 'Low',
    action: f.recommendation, code: f.code,
  }));

  return {
    valid,
    findings,
    strengths,
    security: { score: securityScore, passed: applicable.filter((c) => c.passed).length, total: applicable.length, checks },
    score: { total, grade: gradeFor(total), counts, penalties: PENALTY },
    guidance,
  };
}

function securityChecks(graph, idx, { userIds, syncReach, p }) {
  const ofType = (t) => graph.nodes.filter((n) => n.type === t);
  const entries = [...new Set(userIds.flatMap((u) => idx.out(u).map((e) => e.target)))];
  const tlsOf = (id, seen = new Set()) => {
    if (seen.has(id)) return true;
    seen.add(id);
    const t = idx.type(id);
    if (['CDN', 'LoadBalancer', 'ReverseProxy', 'APIGateway', 'Server', 'WebSocketServer'].includes(t)) return !!p(id, 'https');
    if (t === 'RateLimiter') {
      const next = idx.out(id).map((e) => e.target);
      return next.length > 0 && next.every((n) => tlsOf(n, seen));
    }
    return false;
  };

  const gatewaysWithAuth = ofType('APIGateway').filter((g) => syncReach.has(g.id) && p(g.id, 'auth') !== 'none');
  const frontCompute = [...syncReach].filter((id) => COMPUTE.has(idx.type(id)) && idx.in(id).some((e) => FRONT_DOOR.has(idx.type(e.source)) || idx.type(e.source) === 'User'));
  const authPassed = gatewaysWithAuth.length > 0 || (frontCompute.length > 0 && frontCompute.every((id) => p(id, 'auth') !== 'none'));
  const limiters = ofType('RateLimiter').filter((r) => syncReach.has(r.id));
  const gatewayLimits = ofType('APIGateway').filter((g) => syncReach.has(g.id) && p(g.id, 'rate_limiting'));
  const stores = [...ofType('Database'), ...ofType('BlobStorage')];
  const dbs = ofType('Database');

  return [
    {
      code: 'SEC_AUTHENTICATION', title: 'Requests are not authenticated', label: 'Authentication at the front door',
      applicable: frontCompute.length > 0 || gatewaysWithAuth.length > 0, passed: authPassed,
      reason: 'Nothing on the request path checks who the caller is before doing work for them.',
      impact: 'Anyone who finds the URL can use the API.', recommendation: 'Enable JWT or OAuth on the API gateway, or on every server that receives client traffic.',
      node_ids: frontCompute,
    },
    {
      code: 'SEC_TLS', title: 'Traffic from clients is not encrypted', label: 'TLS on every entry point',
      applicable: entries.length > 0, passed: entries.length > 0 && entries.every((id) => tlsOf(id)),
      reason: 'At least one entry point accepts plain HTTP, so credentials and data cross the internet unencrypted.',
      impact: 'Session hijacking and data exposure.', recommendation: 'Terminate TLS at the CDN, load balancer or gateway that clients connect to.',
      node_ids: entries.filter((id) => !tlsOf(id)),
    },
    {
      code: 'SEC_RATE_LIMITING', title: 'No rate limiting', label: 'Rate limiting',
      applicable: entries.length > 0, passed: limiters.length > 0 || gatewayLimits.length > 0,
      reason: 'One client (or a botnet) can send unlimited requests and starve everyone else.',
      impact: 'Abuse and accidental floods take the service down.', recommendation: 'Add a Rate Limiter at the edge or enable rate limiting on the API gateway.',
    },
    {
      code: 'SEC_ENCRYPTION_AT_REST', title: 'Stored data is not encrypted', label: 'Encryption at rest',
      applicable: stores.length > 0, passed: stores.every((s) => p(s.id, 'encryption_at_rest')),
      reason: 'Database or file storage keeps data in plaintext on disk and in backups.',
      impact: 'A leaked snapshot exposes everything.', recommendation: 'Enable encryption at rest on databases and blob storage.',
      node_ids: stores.filter((s) => !p(s.id, 'encryption_at_rest')).map((s) => s.id),
    },
    {
      code: 'SEC_BACKUPS', title: 'No point-in-time backups', label: 'Backups',
      applicable: dbs.length > 0, passed: dbs.every((d) => p(d.id, 'backups')),
      reason: 'Replicas copy mistakes instantly. Without backups, an accidental delete or corruption is permanent.',
      impact: 'Unrecoverable data loss.', recommendation: 'Enable point-in-time backups on every database.',
      node_ids: dbs.filter((d) => !p(d.id, 'backups')).map((d) => d.id),
    },
  ];
}

function shortCount(n) {
  if (n >= 10000) return `${(n / 1000).toFixed(0)}k`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return `${Math.round(n)}`;
}

function overloadAdvice(type) {
  switch (type) {
    case 'Database': return 'Put a cache in front for reads, add read replicas, or shard to spread writes.';
    case 'Cache': return 'Add cache nodes or enable cluster mode.';
    case 'Queue': return 'Add partitions or another queue for this stream.';
    case 'Worker': return 'Add worker instances, raise concurrency, or enable autoscaling.';
    case 'ThirdPartyAPI': return 'Cache provider responses, batch calls, or negotiate a higher rate limit.';
    case 'SearchIndex': return 'Add shards or replica copies.';
    case 'KafkaTopic': return 'Add partitions and brokers, batch with linger.ms and compress, or fix a skewed message key.';
    case 'ConsumerGroup': case 'StreamProcessor': case 'KafkaConnect': case 'MirrorMaker':
      return 'Add consumers up to the partition count; beyond that, add partitions too.';
    case 'SchemaRegistry': return 'Add registry instances; clients should cache schemas.';
    case 'LoadBalancer': case 'APIGateway': case 'ReverseProxy': case 'RateLimiter': return 'Add instances of this component.';
    default: return 'Add instances behind a load balancer or enable autoscaling.';
  }
}

// ================================================================ live run
// A live run steps the steady-state simulator through time and adds what only time can show:
// autoscalers that need a while to start instances, queues that fill and drain, consumer lag
// that builds and drains (and expires past retention), caches that refill after a restart,
// spikes that come and go, and parts that break and get fixed.

export const BOOT_SECONDS = 30;
export const SCALE_DOWN_AFTER = 60;
export const WARM_SECONDS = 60;
export const RAMP_SECONDS = 120;
export const WAVE_SECONDS = 180;
export const SPIKE_SHAPE = { rise: 3, hold: 30, fall: 5 };
const MAX_HISTORY = 3600;
const MAX_EVENTS = 200;

const POISONABLE = new Set([...KAFKA_CONSUMERS, 'Worker', 'Service']);

const percent = (v, digits = 0) => `${(v * 100).toFixed(digits)}%`;
const ms = (v) => (v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${Math.round(v)} ms`);
export const clock = (t) => `${Math.floor(t / 60)}:${String(Math.floor(t % 60)).padStart(2, '0')}`;
export const times = (m) => `${m >= 10 ? Math.round(m) : m.toFixed(1)}×`;
export function fmt(n) {
  const v = Math.round(Number(n) || 0);
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e4) return `${Math.round(v / 1e3)}k`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return String(v);
}

export function isAutoscaled(node) {
  return !!node && AUTOSCALABLE.has(node.type) && !!param(node, 'autoscaling');
}

const slowBy = (node) => (node.type === 'ThirdPartyAPI' ? 10 : 5);

// The ways a part can be broken during a run. `graph` lets consumers of topics offer a
// poison message.
export function failureOptions(node, graph = null) {
  if (!node || node.type === 'User') return [];
  const topic = node.type === 'KafkaTopic';
  const downLabel = node.type === 'Database' ? 'Primary fails' : node.type === 'ThirdPartyAPI' ? 'Provider goes down'
    : topic ? 'Whole cluster goes down' : KAFKA_CONSUMERS.has(node.type) ? 'All consumers stop' : 'Take it down';
  const out = [{ kind: 'down', label: downLabel }];
  let copies;
  if (node.type === 'SearchIndex') copies = (Number(param(node, 'replicas')) || 0) > 0;
  else if (topic) copies = (Number(param(node, 'brokers')) || 1) >= 2;
  else copies = SCALABLE.has(node.type) && (isAutoscaled(node) || (Number(param(node, 'replicas')) || 1) >= 2);
  if (copies) out.push({ kind: 'lose-one', label: node.type === 'Cache' ? 'Lose one node' : topic ? 'Lose one broker' : KAFKA_CONSUMERS.has(node.type) ? 'One consumer crashes' : 'Lose one copy' });
  out.push({ kind: 'slow', label: `Make it ${slowBy(node)}× slower` });
  if (node.type === 'Cache') out.push({ kind: 'empty', label: 'Empty the cache' });
  if (node.type === 'CDN') out.push({ kind: 'empty', label: 'Purge the CDN' });
  const readsTopic = graph ? graph.edges.some((e) => e.target === node.id && graph.nodes.find((n) => n.id === e.source)?.type === 'KafkaTopic') : KAFKA_CONSUMERS.has(node.type);
  if (POISONABLE.has(node.type) && readsTopic) out.push({ kind: 'poison', label: 'Send a poison message' });
  if (topic) out.push({ kind: 'schema', label: 'Deploy a breaking schema' });
  return out;
}

function envelope(tau) {
  const { rise, hold, fall } = SPIKE_SHAPE;
  if (tau < 0) return 0;
  if (tau < rise) return tau / rise;
  if (tau < rise + hold) return 1;
  if (tau < rise + hold + fall) return 1 - (tau - rise - hold) / fall;
  return 0;
}

export class LiveRun {
  constructor({ traffic = {}, overrides = {}, latencyTarget = 300, spikes = {} } = {}) {
    this.traffic = traffic;
    this.baseOverrides = overrides;
    this.latencyTarget = latencyTarget;
    this.spikes = { regular: spikes.regular || 3, mega: spikes.mega || 8 };
    this.level = 1;
    this.abusive = Number(traffic.abusive_share) || 0;
    this.eventSeq = 0;
    this.reset();
  }

  // Clears everything that happened; keeps the traffic level and bot share.
  reset() {
    this.t = 0;
    this.pattern = 'steady';
    this.patternStart = 0;
    this.bursts = [];
    this.failures = new Map();
    this.scale = new Map();
    this.warm = new Map();
    this.depth = new Map();
    this.lag = new Map();
    this.age = new Map();
    this.history = [];
    this.events = [];
    this.last = null;
    this.watch = {
      status: new Map(), errBand: 0, overTarget: false, blocking: false, breakers: new Set(),
      kafka: { stuck: new Set(), storm: new Set(), rejecting: new Set(), expiring: new Set() },
    };
    this.stats = {
      requests: 0, served: 0, peakRps: 0, peakMult: 0, worstP95: 0, worstError: 0, overTarget: 0, withErrors: 0, heldAt: 0, breakAt: null, busiest: new Map(), failures: 0,
      kafka: false, maxLag: 0, worstAge: 0, expired: 0,
    };
  }

  log(tone, text) {
    this.events.push({ id: ++this.eventSeq, t: this.t, tone, text });
    if (this.events.length > MAX_EVENTS) this.events.shift();
  }

  // ------------------------------------------------------------------ traffic
  multiplier(t = this.t) {
    let m = this.level;
    if (this.pattern === 'ramp') m *= 0.5 + 9.5 * clamp((t - this.patternStart) / RAMP_SECONDS, 0, 1);
    else if (this.pattern === 'wave') m *= 0.35 + (0.65 * (1 - Math.cos((2 * Math.PI * (t - this.patternStart)) / WAVE_SECONDS))) / 2;
    let burst = 1;
    for (const b of this.bursts) burst = Math.max(burst, 1 + (b.mult - 1) * envelope(t - b.start));
    return m * burst;
  }

  normalRps(graph) {
    const t = this.traffic || {};
    if (Number(t.requests_per_second) > 0) return Number(t.requests_per_second);
    if (Number(t.users) > 0) return Number(t.users) * (Number(t.requests_per_user) || 1);
    return graph.nodes.filter((n) => n.type === 'User')
      .reduce((s, n) => s + (Number(param(n, 'users')) || 0) * (Number(param(n, 'requests_per_user')) || 0), 0);
  }

  setLevel(level, graph = null) {
    this.level = clamp(Number(level) || 1, 0.1, 10);
    this.log('info', `Traffic set to ${times(this.level)} normal${graph ? ` (${fmt(this.normalRps(graph) * this.level)} requests a second)` : ''}.`);
  }

  setPattern(pattern) {
    this.pattern = ['steady', 'ramp', 'wave'].includes(pattern) ? pattern : 'steady';
    this.patternStart = this.t;
    const text = {
      steady: 'Traffic holds steady.',
      ramp: `Traffic ramps from half to ten times normal over ${RAMP_SECONDS / 60} minutes.`,
      wave: `Traffic follows a day: quiet at night, busiest at midday, every ${WAVE_SECONDS / 60} minutes.`,
    }[this.pattern];
    this.log('info', text);
  }

  spike(kind = 'regular') {
    const mult = kind === 'mega' ? this.spikes.mega : this.spikes.regular;
    this.bursts.push({ start: this.t, mult, kind });
    this.log('warn', `${kind === 'mega' ? 'Mega spike' : 'Spike'}: traffic jumps to ${times(mult)} for ${SPIKE_SHAPE.hold} seconds.`);
  }

  calm() {
    this.level = 1;
    this.pattern = 'steady';
    this.bursts = [];
    this.log('info', 'Traffic is back to normal.');
  }

  setAbusive(share) {
    this.abusive = clamp(Number(share) || 0, 0, 0.9);
    this.log('info', this.abusive > 0 ? `${percent(this.abusive)} of traffic now comes from bots and abusive clients.` : 'No more bot traffic.');
  }

  // ------------------------------------------------------------------ failures
  fail(node, kind, graph = null) {
    if (!node || node.type === 'User' || !failureOptions(node, graph).some((o) => o.kind === kind)) return false;
    const label = node.label;
    const topic = node.type === 'KafkaTopic';
    const consumer = KAFKA_CONSUMERS.has(node.type);
    if (kind === 'empty') {
      this.warm.set(node.id, { since: this.t, from: 0 });
      this.stats.failures++;
      this.log('bad', node.type === 'CDN'
        ? `${label}: purged. Requests go to your servers until the edges refill.`
        : `${label}: emptied. It refills over about a minute, so more reads reach the store behind it.`);
      return true;
    }
    if (kind === 'lose-one' && isAutoscaled(node)) {
      const s = this.scale.get(node.id);
      if (!s || s.running < 1) return false;
      s.running -= 1;
      this.stats.failures++;
      this.log('bad', `${label}: one instance crashed. The autoscaler will start a replacement.`);
      return true;
    }
    if (this.failures.has(node.id)) return false;
    this.failures.set(node.id, { kind, since: this.t, label, mult: kind === 'slow' ? slowBy(node) : 1 });
    this.stats.failures++;
    let text;
    if (kind === 'down') {
      if (node.type === 'Database') text = `${label}: the primary database failed.`;
      else if (node.type === 'ThirdPartyAPI') text = `${label}: stopped answering.`;
      else if (topic) text = `${label}: the whole Kafka cluster went down. Every write to it fails.`;
      else if (consumer) text = `${label}: every consumer stopped. Messages wait in the topic.`;
      else text = `You took down ${label}.`;
    } else if (kind === 'lose-one') {
      text = topic ? `${label}: lost one broker. Its partitions elect new leaders on the others.`
        : consumer ? `${label}: one consumer crashed. The group hands its partitions to the others.` : `${label}: lost one copy.`;
    } else if (kind === 'poison') {
      text = `${label}: a poison message arrived.${param(node, 'dead_letter_topic') ? ' It goes to the dead letter topic.' : ' Without a dead letter topic, it blocks its partition.'}`;
    } else if (kind === 'schema') {
      text = `${label}: a producer started sending messages in an incompatible schema.`;
    } else {
      text = topic ? `${label}: brokers are ${slowBy(node)}× slower (slow disks).` : `${label}: ${slowBy(node)}× slower than normal.`;
    }
    this.log('bad', text);
    return true;
  }

  fix(id, graph = null) {
    const f = this.failures.get(id);
    if (!f) return false;
    this.failures.delete(id);
    const node = graph?.nodes.find((n) => n.id === id);
    const refill = !!node && (node.type === 'Cache' || node.type === 'CDN') && f.kind !== 'slow';
    if (refill) this.warm.set(id, { since: this.t, from: f.kind === 'down' ? 0 : 0.5 });
    if (f.kind === 'poison') this.log('good', `${f.label}: someone skipped the bad message, so its partition moves again.`);
    else if (f.kind === 'schema') this.log('good', `${f.label}: the producer rolled back to the old schema.`);
    else this.log('good', `${f.label}: fixed.${refill ? ' It starts cold and refills over about a minute.' : ''}`);
    return true;
  }

  fixAll(graph = null) {
    for (const id of [...this.failures.keys()]) this.fix(id, graph);
  }

  randomFailure(graph, rand = Math.random) {
    const busy = new Set((this.last?.node_metrics || []).filter((m) => m.demand_rps > 0).map((m) => m.id));
    const candidates = graph.nodes.filter((n) => n.type !== 'User' && !this.failures.has(n.id) && (!busy.size || busy.has(n.id)));
    if (!candidates.length) return null;
    const node = candidates[Math.floor(rand() * candidates.length)];
    const options = failureOptions(node, graph);
    const pick = options[Math.floor(rand() * options.length)];
    return this.fail(node, pick.kind, graph) ? { node, kind: pick.kind } : null;
  }

  // ------------------------------------------------------------------ stepping
  overridesNow() {
    const ov = {};
    for (const [id, o] of Object.entries(this.baseOverrides || {})) ov[id] = { ...o };
    const set = (id, patch) => { ov[id] = { ...(ov[id] || {}), ...patch }; };
    for (const [id, f] of this.failures) {
      if (f.kind === 'down') set(id, { down: true });
      if (f.kind === 'slow') set(id, { latencyMult: (ov[id]?.latencyMult ?? 1) * f.mult });
      if (f.kind === 'lose-one') set(id, { instancesDelta: (ov[id]?.instancesDelta ?? 0) - 1 });
      if (f.kind === 'poison') set(id, { poison: true });
    }
    for (const [id, w] of this.warm) set(id, { hitMult: Math.min(ov[id]?.hitMult ?? 1, clamp(w.from + (this.t - w.since) / WARM_SECONDS, 0, 1)) });
    for (const [id, s] of this.scale) set(id, { instances: s.running });
    for (const [id, d] of this.depth) set(id, { depth: d });
    for (const [id, lag] of this.lag) if (lag > 0) set(id, { lag });
    return ov;
  }

  kafkaNow() {
    return { schema_break: [...this.failures.values()].some((f) => f.kind === 'schema') };
  }

  // The design as it stands, at the current traffic, without moving time forward.
  peek(graph) {
    return simulate(graph, { traffic: { ...this.traffic, multiplier: this.multiplier(), abusive_share: this.abusive }, overrides: this.overridesNow(), kafka: this.kafkaNow() });
  }

  step(graph, dt = 1) {
    const nodes = new Map(graph.nodes.map((n) => [n.id, n]));
    for (const map of [this.failures, this.scale, this.warm, this.depth, this.lag, this.age]) {
      for (const id of [...map.keys()]) if (!nodes.has(id)) map.delete(id);
    }
    for (const id of [...this.scale.keys()]) if (!isAutoscaled(nodes.get(id))) this.scale.delete(id);
    for (const n of graph.nodes) if (n.type === 'Queue' && !this.depth.has(n.id)) this.depth.set(n.id, 0);
    const first = this.history.length === 0;

    this.t += dt;
    for (const [id, s] of this.scale) {
      const ready = s.starting.filter((x) => x.readyAt <= this.t + 1e-9);
      if (!ready.length) continue;
      const count = ready.reduce((sum, x) => sum + x.count, 0);
      s.starting = s.starting.filter((x) => x.readyAt > this.t + 1e-9);
      s.running += count;
      this.log('good', `${nodes.get(id).label}: ${count} new ${count === 1 ? 'instance is' : 'instances are'} ready. ${s.running} running.`);
    }
    const spikeLength = SPIKE_SHAPE.rise + SPIKE_SHAPE.hold + SPIKE_SHAPE.fall;
    if (this.bursts.some((b) => this.t - b.start >= spikeLength)) {
      this.bursts = this.bursts.filter((b) => this.t - b.start < spikeLength);
      if (!this.bursts.length) this.log('info', 'The spike is over.');
    }

    const mult = this.multiplier();
    const traffic = { ...this.traffic, multiplier: mult, abusive_share: this.abusive };

    // Autoscaled parts new to this run start where the autoscaler would put them, or at
    // their configured size if they appear mid-run.
    const fresh = graph.nodes.filter((n) => isAutoscaled(n) && !this.scale.has(n.id));
    if (fresh.length) {
      const probe = first ? simulate(graph, { traffic, overrides: this.overridesNow(), kafka: this.kafkaNow() }) : null;
      const steady = new Map((probe?.node_metrics || []).filter((m) => m.autoscaling).map((m) => [m.id, m.autoscaling.instances]));
      for (const n of fresh) {
        const min = Math.max(1, Math.round(Number(param(n, 'autoscale_min')) || 1));
        const max = Math.max(min, Math.round(Number(param(n, 'autoscale_max')) || 1));
        const running = steady.get(n.id) ?? clamp(Math.round(Number(param(n, 'replicas')) || 1), min, max);
        this.scale.set(n.id, { running, starting: [], lowSince: null, atMax: false });
      }
    }

    const sim = simulate(graph, { traffic, overrides: this.overridesNow(), kafka: this.kafkaNow() });
    const metrics = new Map(sim.node_metrics.map((m) => [m.id, m]));
    if (first) this.log('info', `Started at ${times(mult)} normal traffic: ${fmt(sim.incoming_rps)} requests a second.`);
    this.autoscale(metrics);
    this.fillQueues(metrics, dt);
    this.fillLags(sim, dt);
    this.refillCaches(nodes);
    this.record(sim, metrics, mult, dt);
    this.notice(sim, graph);
    this.last = sim;
    return sim;
  }

  autoscale(metrics) {
    for (const [id, s] of this.scale) {
      const m = metrics.get(id);
      const a = m?.autoscaling;
      if (!a || m.status === 'down') continue;
      if (s.running > a.max) {
        s.running = a.max;
        s.starting = [];
        this.log('info', `${m.label}: cut to the new maximum of ${a.max}.`);
      }
      const pending = s.starting.reduce((sum, x) => sum + x.count, 0);
      const want = Math.max(a.wanted, a.min);
      if (want > s.running + pending) {
        const add = want - s.running - pending;
        s.starting.push({ count: add, readyAt: this.t + BOOT_SECONDS });
        this.log('info', `${m.label}: the autoscaler is starting ${add} more (ready in ${BOOT_SECONDS} s).`);
      }
      if (want < s.running) {
        if (s.lowSince === null) s.lowSince = this.t;
        if (this.t - s.lowSince >= SCALE_DOWN_AFTER) {
          const removed = s.running - want;
          s.running = want;
          s.starting = [];
          s.lowSince = null;
          this.log('info', `${m.label}: traffic dropped, so the autoscaler stopped ${removed}. ${want} running.`);
        }
      } else {
        s.lowSince = null;
      }
      if (want <= s.running) s.starting = [];
      const atMax = a.wanted >= a.max && m.utilization > 0.85;
      if (atMax && !s.atMax) this.log('warn', `${m.label}: reached the autoscaling limit of ${a.max} and still busy.`);
      s.atMax = atMax;
    }
  }

  fillQueues(metrics, dt) {
    for (const [id, depth] of this.depth) {
      const m = metrics.get(id);
      const q = m?.queue;
      if (!q) continue;
      const growing = q.backlog_rps > Math.max(1, q.inflow * 0.005);
      const next = clamp(depth + (growing ? q.backlog_rps : -q.spare_rps) * dt, 0, q.max_depth);
      if (next >= q.max_depth && depth < q.max_depth) this.log('bad', `${m.label}: full. New messages are rejected until the workers catch up.`);
      else if (depth <= 0 && next > 0) this.log('warn', `${m.label}: backing up. ${fmt(q.backlog_rps)} more messages arrive each second than the workers can handle.`);
      else if (depth > 0 && next <= 0) this.log('good', `${m.label}: caught up.`);
      this.depth.set(id, next);
    }
  }

  // Consumer lag builds while a group falls behind and drains with its spare capacity.
  // Messages older than the topic's retention are deleted, read or not.
  fillLags(sim, dt) {
    const w = this.watch.kafka;
    if (!sim.kafka) {
      this.lag.clear();
      this.age.clear();
      return;
    }
    this.stats.kafka = true;
    const retention = new Map();
    for (const t of sim.kafka.topics) for (const g of t.groups) retention.set(g.id, Math.min(retention.get(g.id) ?? Infinity, t.retention_sec));
    const flip = (set, id, on, bad, good) => {
      if (on && !set.has(id)) this.log('bad', bad);
      else if (!on && set.has(id) && good) this.log('good', good);
      if (on) set.add(id);
      else set.delete(id);
    };
    for (const m of sim.node_metrics) {
      const k = m.kafka;
      if (k?.role === 'topic') {
        flip(w.rejecting, m.id, k.write_fail > 0 && m.status !== 'down' && k.write_fail < 1,
          `${m.label}: ${percent(k.write_fail)} of writes are rejected. Too few in-sync copies are left for acks=all.`, `${m.label}: accepting every write again.`);
        continue;
      }
      if (k?.role !== 'consumer') continue;
      const lag = this.lag.get(m.id) || 0;
      const growing = k.lag_rps > Math.max(0.5, m.demand_rps * 0.005);
      const spare = Math.max(0, (m.capacity_rps ?? 0) - m.demand_rps);
      let next = Math.max(0, lag + (growing ? k.lag_rps : -spare) * dt);
      const rate = m.demand_rps;
      const keep = retention.get(m.id) ?? Infinity;
      const expiring = rate > 0 && next / rate > keep;
      if (expiring) {
        const expired = next - keep * rate;
        next -= expired;
        this.stats.expired += expired;
      }
      flip(w.expiring, m.id, expiring, `${m.label}: unread messages passed the ${fmtDuration(keep)} retention and are being deleted. They are lost.`, null);
      if (lag <= 0 && next > 0 && growing) this.log('warn', `${m.label}: falling behind. Lag grows by ${fmt(k.lag_rps)} messages a second.`);
      else if (lag > 0 && next <= 0) this.log('good', `${m.label}: caught up.`);
      this.lag.set(m.id, next);
      if (rate > 0) this.age.set(m.id, next / rate);
      else if (next <= 0) this.age.set(m.id, 0);
      flip(w.stuck, m.id, k.stuck_share > 0, `${m.label}: stuck. ${percent(k.stuck_share)} of its messages cannot be processed.`, `${m.label}: moving again.`);
      flip(w.storm, m.id, k.rebalancing, `${m.label}: rebalancing again and again. A poll takes longer than max.poll.interval.ms, so Kafka keeps evicting consumers.`, `${m.label}: the rebalance storm is over.`);
    }
  }

  refillCaches(nodes) {
    for (const [id, w] of this.warm) {
      if (w.from + (this.t - w.since) / WARM_SECONDS < 1) continue;
      this.warm.delete(id);
      this.log('good', `${nodes.get(id).label}: warm again.`);
    }
  }

  record(sim, metrics, mult, dt) {
    const lag = [...this.lag.values()].reduce((sum, v) => sum + v, 0);
    const age = Math.max(0, ...this.age.values());
    this.history.push({
      t: this.t, mult, rps: sim.incoming_rps, legit: sim.legit_rps, served: sim.throughput_rps,
      blocked: sim.blocked_rps, error: sim.error_rate, p95: sim.latency_ms.p95, util: sim.max_utilization,
      ...(sim.kafka ? { lag, age } : {}),
    });
    if (this.history.length > MAX_HISTORY) this.history.shift();
    const s = this.stats;
    s.maxLag = Math.max(s.maxLag, lag);
    s.worstAge = Math.max(s.worstAge, age);
    s.requests += sim.legit_rps * dt;
    s.served += sim.throughput_rps * dt;
    s.peakRps = Math.max(s.peakRps, sim.incoming_rps);
    s.peakMult = Math.max(s.peakMult, mult);
    s.worstP95 = Math.max(s.worstP95, sim.latency_ms.p95);
    s.worstError = Math.max(s.worstError, sim.error_rate);
    if (sim.latency_ms.p95 > this.latencyTarget) s.overTarget += dt;
    if (sim.error_rate > 0.01) {
      s.withErrors += dt;
      if (!s.breakAt) s.breakAt = { t: this.t, mult };
    } else {
      s.heldAt = Math.max(s.heldAt, mult);
    }
    for (const id of sim.bottleneck_ids) {
      const m = metrics.get(id);
      if (m && (m.status === 'overloaded' || m.status === 'down')) s.busiest.set(m.label, (s.busiest.get(m.label) || 0) + dt);
    }
  }

  // Turns changes between steps into plain-language events.
  notice(sim, graph) {
    const w = this.watch;
    const band = sim.error_rate > 0.05 ? 2 : sim.error_rate > 0.01 ? 1 : 0;
    if (band !== w.errBand) {
      if (band === 2) this.log('bad', `${percent(sim.error_rate)} of requests are failing.`);
      else if (band === 1) this.log('warn', `Some users see errors: ${percent(sim.error_rate, 1)} of requests fail.`);
      else this.log('good', 'Errors are back under 1%.');
      w.errBand = band;
    }
    // Failed requests come back fast, so speed only means something while most requests work.
    const over = sim.latency_ms.p95 > this.latencyTarget;
    if (band < 2 && over !== w.overTarget) {
      this.log(over ? 'warn' : 'good', over
        ? `Responses are slow: the slowest 5% take ${ms(sim.latency_ms.p95)}, over the ${this.latencyTarget} ms target.`
        : `Responses are fast again (slowest 5% take ${ms(sim.latency_ms.p95)}).`);
      w.overTarget = over;
    }
    const types = new Map(graph.nodes.map((n) => [n.id, n.type]));
    for (const m of sim.node_metrics) {
      if (m.status === 'source') continue;
      const prev = w.status.get(m.id) || 'ok';
      w.status.set(m.id, m.status);
      if (m.status === prev || m.status === 'down' || prev === 'down') continue;
      const inputs = graph.edges.filter((e) => e.target === m.id);
      const fedByQueue = inputs.length > 0 && inputs.every((e) => types.get(e.source) === 'Queue');
      // Lag events already tell this story for consumers of topics.
      const fedByTopic = inputs.some((e) => types.get(e.source) === 'KafkaTopic');
      if (fedByTopic && m.status !== 'hot') continue;
      if (m.status === 'overloaded') {
        let text = `${m.label}: overloaded. ${fmt(m.demand_rps)} requests a second arrive, but only about ${fmt(m.capacity_rps)} can be handled.`;
        if (fedByQueue) text = `${m.label}: cannot keep up, so work waits in the queue.`;
        else if (m.type === 'KafkaTopic') text = `${m.label}: producers send ${fmt(m.demand_rps)} messages a second, but the topic takes only about ${fmt(m.capacity_rps)}. The rest of the writes fail.`;
        this.log('bad', text);
      } else if (m.status === 'hot' && ['ok', 'warm', 'idle'].includes(prev)) {
        this.log('warn', `${m.label}: running hot (${percent(m.utilization)} busy).`);
      } else if (prev === 'overloaded' && (m.status === 'ok' || m.status === 'warm')) {
        this.log('good', `${m.label}: keeping up again.`);
      }
    }
    const open = new Set();
    for (const m of sim.node_metrics) {
      for (const note of m.notes) {
        const found = note.match(/^Circuit breaker to (.+) is open/);
        if (found) open.add(`${m.label}\u0000${found[1]}`);
      }
    }
    for (const key of open) {
      if (w.breakers.has(key)) continue;
      const [from, to] = key.split('\u0000');
      this.log('warn', `${from} stopped calling ${to} for now (circuit breaker open). Those calls fail fast.`);
    }
    for (const key of w.breakers) {
      if (open.has(key)) continue;
      const [from, to] = key.split('\u0000');
      this.log('good', `${from} → ${to}: circuit breaker closed, calls go through again.`);
    }
    w.breakers = open;
    const blocking = sim.blocked_rps > 1;
    if (blocking && !w.blocking) this.log('good', `Rate limiting is turning away ${fmt(sim.blocked_rps)} bot requests a second.`);
    w.blocking = blocking;
  }

  // ------------------------------------------------------------------ report
  summary() {
    const s = this.stats;
    const successRate = s.requests > 0 ? s.served / s.requests : 1;
    const busiest = [...s.busiest].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([label, seconds]) => ({ label, seconds }));
    let verdict = successRate >= 0.995 && s.overTarget <= this.t * 0.05 ? 'held' : successRate >= 0.9 ? 'struggled' : 'failed';
    // Messages deleted unread are lost data, however well requests went.
    if (s.expired >= 1 && verdict === 'held') verdict = 'struggled';
    return {
      duration: this.t, requests: s.requests, served: s.served, failed: Math.max(0, s.requests - s.served), successRate,
      peakRps: s.peakRps, peakMult: s.peakMult, worstP95: s.worstP95, worstError: s.worstError,
      secondsOverTarget: s.overTarget, secondsWithErrors: s.withErrors, heldAt: s.heldAt, breakAt: s.breakAt,
      busiest, failures: s.failures, verdict,
      kafka: s.kafka ? { maxLag: s.maxLag, worstAge: s.worstAge, expired: s.expired, lagNow: [...this.lag.values()].reduce((sum, v) => sum + v, 0) } : null,
    };
  }
}

// ================================================================ stress tests
export const RAMP = [0.5, 1, 1.5, 2, 3, 4, 5, 6, 8, 10];

const forType = (graph, types) => graph.nodes.filter((n) => types.includes(n.type));
const busiest = (nodes, baseline) => [...nodes].sort((a, b) => demandOf(baseline, b.id) - demandOf(baseline, a.id))[0];
const demandOf = (sim, id) => sim.node_metrics.find((n) => n.id === id)?.demand_rps || 0;
const each = (nodes, ov) => Object.fromEntries(nodes.map((n) => [n.id, ov]));
const topicReaders = (graph) => {
  const idx = new GraphIndex(graph);
  return graph.nodes.filter((n) => idx.in(n.id).some((e) => idx.type(e.source) === 'KafkaTopic'));
};

export const STRESS_AREAS = [
  {
    id: 'Database', label: 'Database', types: ['Database'],
    tests: [
      { id: 'db-write-surge', name: 'Write surge', description: 'A launch or bulk import triples writes. Replicas do not help here: every write lands on a primary.',
        advice: 'Shard to spread writes across primaries, or buffer writes through a queue.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Database']), { writeMult: 3 }) }) },
      { id: 'db-read-surge', name: 'Read surge', description: 'A viral page triples the reads that reach the database.',
        advice: 'Cache hot reads and add read replicas.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Database']), { readMult: 3 }) }) },
      { id: 'db-lose-primary', name: 'Lose the primary', description: 'The busiest database primary dies. Does a replica take over, and can the survivors carry the load?',
        advice: 'Add read replicas with automatic failover.',
        build: ({ graph, baseline }) => ({ overrides: failureOverrides(busiest(forType(graph, ['Database']), baseline)) }) },
      { id: 'db-slow-queries', name: 'Slow queries', description: 'Lock contention or a bad deploy makes queries 4× slower and halves throughput.',
        advice: 'Set timeouts on callers, cache reads, and keep headroom on the database.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Database']), { latencyMult: 4, capacityMult: 0.5 }) }) },
    ],
  },
  {
    id: 'Cache', label: 'Cache', types: ['Cache'],
    tests: [
      { id: 'cache-cold-start', name: 'Cold start', description: 'The cache restarts empty and every lookup misses until it refills.',
        advice: 'Enable request coalescing so one request refills each hot key, and give the database headroom for misses.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Cache']), { hitMult: 0, cold: true }) }) },
      { id: 'cache-node-loss', name: 'Lose a cache node', description: 'The busiest cache node dies and its keys miss until refilled.',
        advice: 'Run several cache nodes and enable request coalescing.',
        build: ({ graph, baseline }) => ({ overrides: failureOverrides(busiest(forType(graph, ['Cache']), baseline)) }) },
      { id: 'cache-hit-drop', name: 'Hit ratio halves', description: 'New content or a key format change means half the usual hits now miss.',
        advice: 'Size the database for a realistic miss rate, not the best case.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Cache']), { hitMult: 0.5 }) }) },
    ],
  },
  {
    id: 'Queue', label: 'Queue', types: ['Queue'],
    tests: [
      { id: 'queue-consumer-outage', name: 'Consumers stop', description: 'Every consumer is down for ten minutes. Does the queue hold the backlog without rejecting producers?',
        advice: 'Size queue depth for your longest consumer outage and alert on backlog age.', async: true,
        build: ({ graph }) => {
          const idx = new GraphIndex(graph);
          const consumers = forType(graph, ['Queue']).flatMap((q) => idx.out(q.id).map((e) => idx.node(e.target)));
          return { overrides: each(consumers, { down: true }) };
        } },
      { id: 'queue-burst', name: 'Burst of events', description: 'Producers emit five times the usual messages.',
        advice: 'Autoscale consumers on queue depth.', async: true,
        build: () => ({ traffic: { async_multiplier: 5 } }) },
    ],
  },
  {
    id: 'LoadBalancer', label: 'Load balancer', types: ['LoadBalancer'],
    tests: [
      { id: 'lb-instance-loss', name: 'Lose a load balancer', description: 'One load balancer instance fails.',
        advice: 'Run at least two load balancer instances.',
        build: ({ graph, baseline }) => ({ overrides: failureOverrides(busiest(forType(graph, ['LoadBalancer']), baseline)) }) },
      { id: 'lb-backend-loss', name: 'Lose a backend', description: 'One server behind the load balancer dies. Health checks should send its share elsewhere.',
        advice: 'Keep health checks on and run N+1 backends.',
        build: ({ graph, baseline }) => {
          const idx = new GraphIndex(graph);
          const lb = busiest(forType(graph, ['LoadBalancer']), baseline);
          const backends = idx.out(lb.id).map((e) => idx.node(e.target));
          const victim = busiest(backends, baseline);
          return victim ? { overrides: failureOverrides(victim) } : {};
        } },
    ],
  },
  {
    id: 'Compute', label: 'Server / Service', types: ['Server', 'Service', 'WebSocketServer'],
    tests: [
      { id: 'compute-instance-loss', name: 'Lose an instance', description: 'The busiest app server or service loses an instance.',
        advice: 'Run at least two instances behind a load balancer, or autoscale.',
        build: ({ graph, baseline }) => {
          const pick = pickFailureTarget(graph, ['Server', 'Service', 'WebSocketServer'], baseline);
          return pick.node ? { overrides: failureOverrides(pick.node) } : {};
        } },
      { id: 'compute-spike', name: 'Traffic spike', description: 'Three times the normal peak arrives at once.',
        advice: 'Enable autoscaling with a high enough ceiling, and protect the data tier with caches.',
        build: () => ({ traffic: { multiplier: 3 } }) },
      { id: 'compute-slow-dependency', name: 'Slow dependency', description: 'Everything your servers call becomes 5× slower. Timeouts decide whether servers keep their threads.',
        advice: 'Set dependency timeouts near normal p99 and add circuit breakers.',
        build: ({ graph }) => {
          const idx = new GraphIndex(graph);
          const deps = new Set();
          for (const c of forType(graph, ['Server', 'Service'])) {
            for (const e of idx.out(c.id)) if (!['Cache', 'Queue'].includes(idx.type(e.target)) && e.kind !== 'async') deps.add(e.target);
          }
          return { overrides: Object.fromEntries([...deps].map((id) => [id, { latencyMult: 5 }])) };
        } },
    ],
  },
  {
    id: 'Worker', label: 'Worker', types: ['Worker'],
    tests: [
      { id: 'worker-crash', name: 'Workers crash', description: 'Every worker crashes. Jobs wait in the queue and are redelivered when workers return.',
        advice: 'Keep jobs idempotent and size the queue for the outage.', async: true,
        build: ({ graph }) => ({ overrides: each(forType(graph, ['Worker']), { down: true }) }) },
      { id: 'worker-backlog', name: 'Backlog surge', description: 'Three times the usual background work arrives.',
        advice: 'Autoscale workers on queue depth.', async: true,
        build: () => ({ traffic: { async_multiplier: 3 } }) },
    ],
  },
  {
    id: 'Kafka', label: 'Kafka', types: ['KafkaTopic'],
    tests: [
      { id: 'kafka-broker-loss', name: 'Lose a broker', description: 'One broker of the cluster fails. With enough copies, partitions elect new leaders and producers barely notice.',
        advice: 'Run 3 brokers with replication factor 3, acks=all and min.insync.replicas 2.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['KafkaTopic']), { instancesDelta: -1 }) }) },
      { id: 'kafka-consumer-outage', name: 'Consumers stop for a day', description: 'Every consumer is down for 24 hours. Kafka keeps the messages, but only for the retention time.',
        advice: 'Keep retention longer than your worst consumer outage (7 days is common).', async: true,
        build: ({ graph }) => ({ overrides: each(topicReaders(graph), { down: true }), kafka: { horizon_hours: 24 } }) },
      { id: 'kafka-event-burst', name: 'Burst of events', description: 'Producers publish five times the usual messages.',
        advice: 'Give topics enough partitions, and autoscale consumers on lag up to the partition count.', async: true,
        build: () => ({ traffic: { async_multiplier: 5 } }) },
      { id: 'kafka-poison-pill', name: 'Poison message', description: 'A malformed message that always fails processing lands in a topic.',
        advice: 'Enable a dead letter topic on every consumer group.', async: true,
        build: () => ({ kafka: { poison_pill: true } }) },
      { id: 'kafka-schema-change', name: 'Breaking schema change', description: 'A producer deploy changes the message format in a way existing consumers cannot read.',
        advice: 'Add a Schema Registry with backward compatibility, and connect producers to it.', async: true,
        build: () => ({ kafka: { schema_break: true } }) },
    ],
  },
  {
    id: 'ThirdPartyAPI', label: 'Third-party API', types: ['ThirdPartyAPI'],
    tests: [
      { id: 'tp-slowdown', name: 'Provider slows down', description: 'The provider answers 10× slower than usual.',
        advice: 'Use short timeouts and a circuit breaker, or call the provider from a worker.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['ThirdPartyAPI']), { latencyMult: 10 }) }) },
      { id: 'tp-outage', name: 'Provider outage', description: 'The provider is completely down.',
        advice: 'Fail fast with a circuit breaker and queue work that can wait.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['ThirdPartyAPI']), { down: true }) }) },
    ],
  },
  {
    id: 'CDN', label: 'CDN', types: ['CDN'],
    tests: [
      { id: 'cdn-purge', name: 'Edge purge', description: 'A full purge sends every request to the origin until the edge refills.',
        advice: 'Give the origin enough headroom (or an origin shield) to survive a purge.',
        build: ({ graph }) => ({ overrides: each(forType(graph, ['CDN']), { hitMult: 0 }) }) },
    ],
  },
];

export function stressCatalog() {
  return STRESS_AREAS.map((a) => ({
    id: a.id, label: a.label, types: a.types,
    tests: a.tests.map((t) => ({ id: t.id, name: t.name, description: t.description })),
  }));
}

function findTest(id) {
  for (const area of STRESS_AREAS) {
    const test = area.tests.find((t) => t.id === id);
    if (test) return { area, test };
  }
  return null;
}

function judge(sim, target, { async = false } = {}) {
  const overflow = sim.async.overflowing.length > 0;
  if (sim.kafka && (sim.kafka.data_loss_rate > 0 || sim.kafka.stuck_rate > 0)) return 'fail';
  if (sim.error_rate < 0.01 && sim.latency_ms.p95 <= target * 1.5 && !overflow) return 'pass';
  if (sim.error_rate < 0.05 && !overflow) return async && sim.async.backlog_rps > 0 && sim.error_rate < 0.01 ? 'pass' : 'warn';
  return 'fail';
}

function describe(sim) {
  const hottest = [...sim.node_metrics].filter((n) => n.status !== 'source').sort((a, b) => b.utilization - a.utilization)[0];
  const parts = [`${(sim.error_rate * 100).toFixed(1)}% errors`, `p95 ${Math.round(sim.latency_ms.p95)} ms`];
  if (hottest) parts.push(hottest.status === 'down' ? `${hottest.label} down` : `${hottest.label} at ${Math.round(Math.min(hottest.utilization, 9.99) * 100)}%`);
  if (sim.async.backlog_rps > 0) parts.push(`backlog +${Math.round(sim.async.backlog_rps)}/s`);
  if (sim.kafka?.stuck_rate > 0) parts.push(`${Math.round(sim.kafka.stuck_rate * 100)}% of messages stuck`);
  if (sim.kafka?.data_loss_rate > 0) parts.push(`${Math.round(Math.max(0.01, sim.kafka.data_loss_rate) * 100)}% expired unread`);
  return parts.join(' · ');
}

export function runStressTest(graph, { traffic = {}, latencyTarget = 300, focus = [] } = {}) {
  const baseline = simulate(graph, { traffic });
  const ramp = RAMP.map((x) => {
    const sim = simulate(graph, { traffic: { ...traffic, multiplier: (traffic.multiplier || 1) * x } });
    return {
      x,
      rps: sim.incoming_rps,
      error: sim.error_rate,
      latency_ms: sim.latency_ms.p95,
      load: Object.fromEntries(sim.node_metrics.filter((n) => n.status !== 'source').map((n) => [n.id, Math.min(n.utilization, 3)])),
      bottleneck: sim.bottleneck_ids[0] || null,
    };
  });
  const breakPoint = ramp.find((pt) => pt.error > 0.01 || pt.latency_ms > latencyTarget);
  const label = (id) => graph.nodes.find((n) => n.id === id)?.label || id;
  let verdict;
  if (!breakPoint) {
    verdict = { status: 'pass', headline: `Holds all the way to ${RAMP[RAMP.length - 1]}× (${Math.round(ramp[ramp.length - 1].rps).toLocaleString()} rps).` };
  } else {
    const why = breakPoint.error > 0.01 ? `${(breakPoint.error * 100).toFixed(1)}% errors` : `p95 ${Math.round(breakPoint.latency_ms)} ms`;
    const culprit = breakPoint.bottleneck ? `, ${label(breakPoint.bottleneck)} saturates first` : '';
    const status = breakPoint.x > 3 ? 'pass' : breakPoint.x >= 1.5 ? 'warn' : 'fail';
    verdict = { status, headline: `Breaks at ${breakPoint.x}× (${Math.round(breakPoint.rps).toLocaleString()} rps): ${why}${culprit}.` };
  }

  const tests = [];
  for (const id of focus) {
    const found = findTest(id);
    if (!found) continue;
    const { area, test } = found;
    if (!forType(graph, area.types).length) {
      tests.push({ id, area: area.id, name: test.name, status: 'skipped', summary: `No ${area.label} in this design.`, advice: '' });
      continue;
    }
    const spec = test.build({ graph, baseline }) || {};
    const sim = simulate(graph, {
      traffic: { ...traffic, ...(spec.traffic || {}), multiplier: (traffic.multiplier || 1) * (spec.traffic?.multiplier || 1) },
      overrides: spec.overrides || {},
      kafka: spec.kafka,
      horizonSec: spec.kafka?.horizon_hours ? spec.kafka.horizon_hours * 3600 : undefined,
    });
    const status = judge(sim, latencyTarget, { async: test.async });
    tests.push({
      id, area: area.id, name: test.name, description: test.description,
      status, summary: describe(sim), advice: status === 'pass' ? '' : test.advice,
      metrics: { error_rate: sim.error_rate, latency_ms: sim.latency_ms, max_utilization: sim.max_utilization, backlog_rps: sim.async.backlog_rps },
      load: Object.fromEntries(sim.node_metrics.filter((n) => n.status !== 'source').map((n) => [n.id, n.status === 'down' ? -1 : Math.min(n.utilization, 3)])),
    });
  }

  return {
    targets: { latency_ms: latencyTarget, error_rate: 0.01 },
    baseline_rps: baseline.incoming_rps,
    ramp,
    breaks_at: breakPoint ? { x: breakPoint.x, rps: breakPoint.rps, node_id: breakPoint.bottleneck } : null,
    verdict,
    hops: baseline.latency_hops,
    tests,
  };
}

// ================================================================ solutions
// Works out how a failing test can pass. On a copy of the design it makes one targeted change
// at a time (a second copy for a part that failed alone, enough capacity for the busiest part,
// a deeper queue, a timeout for a slow provider), re-runs the test after every change and stops
// as soon as the test passes. Every suggested change is therefore checked by the simulator.

const MAX_STEPS = 16;
// Fixes aim for a comfortable score, not a bare pass at 60, so the result is not on a knife edge.
const GOOD_SCORE = 80;
const INSTANCE_TYPES = new Set(['Server', 'Service', 'WebSocketServer', 'Worker', 'LoadBalancer', 'ReverseProxy', 'RateLimiter', 'APIGateway', 'Cache']);
const SOLVER_AUTOSCALABLE = new Set(['Server', 'Service', 'WebSocketServer', 'Worker']);
const THROUGHPUT_TYPES = new Set(['Queue', 'BlobStorage', 'CDN', 'ThirdPartyAPI']);
// Providers whose answer the caller does not need before replying; these can move behind a queue.
const DEFERRABLE_PROVIDERS = new Set(['email', 'sms', 'push', 'other']);
// "Capacity per instance" means something else for parts that are not a pool of instances.
const CAPACITY_NAME = { ThirdPartyAPI: 'Rate limit', Queue: 'Throughput', BlobStorage: 'Throughput', CDN: 'Throughput' };

const cappedPct = (v, digits = 0) => `${(Math.min(v, 9.99) * 100).toFixed(digits)}%`;
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const settingName = (type, spec) => (spec.key === 'capacity' && CAPACITY_NAME[type]) || spec.label;
// Mid-sentence form of a setting name; abbreviations such as TTL keep their capitals.
const inSentence = (name) => (/^[A-Z]{2}/.test(name) ? name : name.charAt(0).toLowerCase() + name.slice(1));
const allSpecs = (comp) => [...comp.basic, ...comp.advanced, ...comp.security];
// Rounds up to two significant digits, so suggested limits look like 12,000 rather than 11,437.
const roundUp = (v) => {
  const step = 10 ** Math.max(0, Math.floor(Math.log10(Math.max(v, 1))) - 1);
  return Math.ceil(v / step) * step;
};

export function formatValue(v) {
  if (typeof v === 'boolean') return v ? 'on' : 'off';
  if (typeof v === 'number') return v.toLocaleString('en-US');
  return String(v ?? '').replace(/_/g, ' ');
}

// ---------------------------------------------------------------------------- changes
function withParams(graph, id, pairs) {
  const g = cloneGraph(graph);
  const node = g.nodes.find((n) => n.id === id);
  const changes = [];
  for (const [key, value] of pairs) {
    const spec = paramSpec(node.type, key);
    if (!spec) continue;
    const from = param(node, key);
    if (from === value) continue;
    node.config = node.config || {};
    node.config[spec.block] = { ...(node.config[spec.block] || {}), [key]: value };
    changes.push({ id, label: node.label, key, name: settingName(node.type, spec), from, to: value });
  }
  return { graph: g, changes };
}

// A fix is a cause (what is wrong, in this test) and an action (what to change).
function fixWith(graph, node, pairs, cause, action) {
  const { graph: g, changes } = withParams(graph, node.id, pairs);
  return changes.length ? { cause, action, text: `${cause} ${action}`, graph: g, changes } : null;
}

// The same, for one fix that changes several parts together (a topic and its consumers).
function fixMany(graph, edits, cause, action) {
  let g = graph;
  const changes = [];
  for (const [node, pairs] of edits) {
    const res = withParams(g, node.id, pairs);
    g = res.graph;
    changes.push(...res.changes);
  }
  return changes.length ? { cause, action, text: `${cause} ${action}`, graph: g, changes } : null;
}

// A provider with no alternative: put a router in front of it and add a backup provider.
function backupProvider(graph, node) {
  const g = cloneGraph(graph);
  const p = g.nodes.find((n) => n.id === node.id);
  const used = new Set(g.nodes.map((n) => n.id));
  const uid = (base) => {
    let id = base;
    for (let i = 2; used.has(id); i++) id = `${base}-${i}`;
    used.add(id);
    return id;
  };
  const gw = { id: uid(`${p.id}-router`), type: 'APIGateway', label: `${p.label} Router`, x: p.x, y: p.y, config: defaultsFor('APIGateway') };
  gw.config.advanced.replicas = 2;
  const backup = { ...cloneGraph(p), id: uid(`${p.id}-backup`), label: `${p.label} (backup)`, x: p.x + 230, y: p.y + 100 };
  p.x += 230;
  for (const e of g.edges) {
    if (e.target !== p.id) continue;
    e.target = gw.id;
    e.id = `${e.source}->${gw.id}`;
  }
  const edge = (source, target) => ({ id: `${source}->${target}`, source, target, kind: 'sync', ratio: null, optional: false, read_share: null });
  g.nodes.push(gw, backup);
  g.edges.push(edge(gw.id, p.id), edge(gw.id, backup.id));
  const cause = `${node.label}: the only provider, so when it goes down every call fails.`;
  const action = 'Add a backup provider behind a router (an API gateway) that sends calls to whichever one is up.';
  return {
    cause, action, text: `${cause} ${action}`, graph: g,
    changes: [{ add: 'APIGateway', id: gw.id, label: gw.label }, { add: 'ThirdPartyAPI', id: backup.id, label: backup.label }],
    structural: { subject: node.label, lines: [`New API Gateway: ${gw.label}`, `New Third-party API: ${backup.label}`, `Calls to ${node.label} now go through ${gw.label}`] },
  };
}

// A slow provider that callers wait for: hand the work to a queue and let workers call it.
function behindQueue(graph, node, latencyMs) {
  const g = cloneGraph(graph);
  const p = g.nodes.find((n) => n.id === node.id);
  const types = new Map(g.nodes.map((n) => [n.id, n.type]));
  const callers = g.edges.filter((e) => e.target === p.id && e.kind !== 'async' && ['Server', 'Service', 'WebSocketServer'].includes(types.get(e.source)));
  if (!callers.length) return null;
  const used = new Set(g.nodes.map((n) => n.id));
  const uid = (base) => {
    let id = base;
    for (let i = 2; used.has(id); i++) id = `${base}-${i}`;
    used.add(id);
    return id;
  };
  const queue = { id: uid(`${p.id}-queue`), type: 'Queue', label: `${p.label} Queue`, x: p.x, y: p.y, config: defaultsFor('Queue') };
  const workers = { id: uid(`${p.id}-workers`), type: 'Worker', label: `${p.label} Workers`, x: p.x + 210, y: p.y, config: defaultsFor('Worker') };
  Object.assign(workers.config.advanced, { concurrency: 16, idempotent: true, autoscaling: true, autoscale_min: 2, autoscale_max: 20 });
  p.x += 420;
  for (const e of callers) Object.assign(e, { id: `${e.source}->${queue.id}`, target: queue.id, kind: 'async' });
  const edge = (source, target) => ({ id: `${source}->${target}`, source, target, kind: 'sync', ratio: null, optional: false, read_share: null });
  g.nodes.push(queue, workers);
  g.edges.push(edge(queue.id, workers.id), edge(workers.id, p.id));
  const cause = `${node.label}: ${ms(latencyMs)} per call in this test, and callers wait for every call.`;
  const action = `Put it behind a queue: callers hand off the work and answer straight away, and workers call ${node.label} in the background.`;
  return {
    cause, action, text: `${cause} ${action}`, graph: g,
    changes: [{ add: 'Queue', id: queue.id, label: queue.label }, { add: 'Worker', id: workers.id, label: workers.label }],
    structural: { subject: node.label, lines: [`New Queue: ${queue.label}`, `New Worker: ${workers.label} (autoscaling 2 to 20)`, `Callers send work to ${queue.label} instead of waiting for ${node.label}`] },
  };
}

// ---------------------------------------------------------------------------- fixes
function redundancyFix(graph, node) {
  const p = (k) => param(node, k);
  const label = node.label;
  switch (node.type) {
    case 'Database':
      if (!(Number(p('read_replicas')) > 0)) return fixWith(graph, node, [['read_replicas', 1], ['failover', true]], `${label}: only one copy, so when it fails everything stops.`, 'Add a read replica and turn on automatic failover.');
      if (!p('failover')) return fixWith(graph, node, [['failover', true]], `${label}: a replica exists, but nothing promotes it when the primary fails.`, 'Turn on automatic failover.');
      return null;
    case 'SearchIndex':
      return fixWith(graph, node, [['replicas', Math.max(1, Number(p('replicas')) || 0)]], `${label}: one copy of each shard, so losing a node loses part of the index.`, 'Keep a replica of every shard.');
    case 'Cache':
      return fixWith(graph, node, [['replicas', Math.max(2, Number(p('replicas')) || 1)], ['request_coalescing', true]], `${label}: a single node, so losing it floods the store behind it.`, 'Use 2 nodes with request coalescing.');
    case 'ThirdPartyAPI':
      return backupProvider(graph, node);
    case 'KafkaTopic': {
      const brokers = Number(p('brokers')) || 1;
      return fixWith(graph, node, [['brokers', Math.max(3, brokers)], ['replication_factor', 3], ['min_insync_replicas', 2], ['acks', 'all']],
        `${label}: ${brokers === 1 ? 'a single broker' : 'too few copies'}, so losing a broker takes partitions offline.`, 'Run 3 brokers with 3 copies of every partition and min.insync.replicas 2.');
    }
    default:
      if (INSTANCE_TYPES.has(node.type) && !p('autoscaling') && (Number(p('replicas')) || 1) < 2) {
        return fixWith(graph, node, [['replicas', 2]], `${label}: a single copy, so one crash takes it down.`, 'Run at least 2 copies.');
      }
      return null;
  }
}

function capacityFix(graph, node, m, r) {
  const p = (k) => param(node, k);
  const label = node.label;
  const u = Math.min(m.utilization, 9.99);
  const busy = m.capacity_rps
    ? `${label}: ${fmt(m.demand_rps)} requests a second arrive, but only about ${fmt(m.capacity_rps)} can be handled.`
    : `${label}: ${cappedPct(u)} busy in this test.`;

  if (INSTANCE_TYPES.has(node.type)) {
    const running = Math.max(1, m.instances || 1);
    const need = Math.ceil((running * u) / HEADROOM);
    if (SOLVER_AUTOSCALABLE.has(node.type) && p('autoscaling')) {
      const max = Number(p('autoscale_max'));
      const min = Number(p('autoscale_min'));
      if (need > max) return fixWith(graph, node, [['autoscale_max', Math.min(500, need)]], busy, `Let autoscaling go up to ${Math.min(500, need)} instances.`);
      if (need > min) {
        return fixWith(graph, node, [['autoscale_min', need], ['autoscale_max', Math.min(500, Math.max(max, Math.ceil(need * 1.25)))]],
          `${busy} Slow calls tie up its threads, and the autoscaler only counts requests.`, `Keep at least ${need} instances running.`);
      }
      return null;
    }
    const replicas = Number(p('replicas')) || 1;
    const target = Math.min(SOLVER_AUTOSCALABLE.has(node.type) ? 500 : 200, Math.max(need, replicas + 1));
    if (SOLVER_AUTOSCALABLE.has(node.type) && r.scenario === 'spike') {
      return fixWith(graph, node, [['autoscaling', true], ['autoscale_min', replicas], ['autoscale_max', target]], busy, `Turn on autoscaling, from ${replicas} up to ${target} instances, so it grows with the spike.`);
    }
    const count = Math.min(200, target);
    const pairs = [['replicas', count]];
    if (node.type === 'Cache') pairs.push(['request_coalescing', true]);
    return fixWith(graph, node, pairs, busy, `Run ${plural(count, node.type === 'Cache' ? 'node' : 'copy', node.type === 'Cache' ? 'nodes' : 'copies')}.`);
  }

  if (node.type === 'Database') {
    if (!p('indexed')) return fixWith(graph, node, [['indexed', true]], `${label}: every query scans whole tables.`, 'Add indexes: about 4× the capacity and 5× faster queries.');
    const shards = Number(p('shards')) || 1;
    const replicas = Number(p('read_replicas')) || 0;
    const cap = Number(p('capacity')) || 2000;
    const reads = m.read_share ?? Number(p('read_ratio'));
    const writeLoad = (m.demand_rps * (1 - reads)) / (shards * cap);
    if (writeLoad < HEADROOM * 0.6 && reads > 0) {
      const wanted = Math.ceil((m.demand_rps * reads) / (shards * cap * (HEADROOM - writeLoad))) - 1;
      const next = Math.max(wanted, replicas + 1);
      if (next <= 15) return fixWith(graph, node, [['read_replicas', next], ['failover', true]], `${label}: ${cappedPct(u)} busy, and most of the work is reads (${cappedPct(reads)}).`, `Read replicas share reads: run ${next}.`);
    }
    const next = Math.max(Math.ceil((shards * u) / HEADROOM), shards + 1);
    if (next > 64) return null;
    return reads > 0.5
      ? fixWith(graph, node, [['shards', next]], `${label}: ${cappedPct(u)} busy, more than read replicas can fix.`, `Split it into ${next} shards: each takes a slice of all reads and writes.`)
      : fixWith(graph, node, [['shards', next]], `${label}: ${cappedPct(u)} busy, and ${cappedPct(1 - reads)} of the work is writes.`, `Only more primaries share writes, so split it into ${next} shards.`);
  }

  if (node.type === 'SearchIndex') {
    const shards = Number(p('shards')) || 1;
    const replicas = Number(p('replicas')) || 0;
    const units = shards * (1 + replicas);
    const need = Math.max(Math.ceil((units * u) / HEADROOM), units + 1);
    const nextReplicas = Math.max(replicas, 1);
    const nextShards = Math.min(64, Math.max(shards, Math.ceil(need / (1 + nextReplicas))));
    return fixWith(graph, node, [['replicas', nextReplicas], ['shards', nextShards]], `${label}: ${cappedPct(u)} busy.`, `Spread it over ${plural(nextShards, 'shard', 'shards')} with ${plural(nextReplicas, 'replica', 'replicas')} of each.`);
  }

  if (THROUGHPUT_TYPES.has(node.type)) {
    const cap = Number(p('capacity')) || 1;
    const next = roundUp(Math.max(cap * 1.25, m.demand_rps / HEADROOM));
    return node.type === 'ThirdPartyAPI'
      ? fixWith(graph, node, [['capacity', next]], `${label}: allows ${fmt(cap)} calls a second, but this test sends ${fmt(m.demand_rps)}.`, `Ask the provider for a limit of ${fmt(next)}, or cache answers so fewer calls are needed.`)
      : fixWith(graph, node, [['capacity', next]], `${label}: takes ${fmt(cap)} a second, but this test sends ${fmt(m.demand_rps)}.`, `Raise the throughput to ${fmt(next)}.`);
  }
  return null;
}

function limiterFix(graph, problem) {
  const t = problem.traffic || {};
  const perUser = (Number(t.requests_per_user) || 0) * (1 - (Number(t.abusive_share) || 0));
  for (const node of graph.nodes) {
    if (node.type !== 'RateLimiter') continue;
    const burst = Number(param(node, 'burst_multiplier')) || 1;
    const allowed = (Number(param(node, 'limit_per_client_rps')) || 0) * burst;
    if (perUser <= allowed) continue;
    const next = Math.ceil(((perUser * 1.5) / burst) * 10) / 10;
    return fixWith(graph, node, [['limit_per_client_rps', next]], `${node.label}: allows ${allowed} requests a second per client, but a normal user sends ${perUser.toFixed(2)}.`, `Raise the limit to ${next} per client.`);
  }
  return null;
}

function queueDepthFix(graph, r) {
  for (const id of r.sim.async.overflowing) {
    const m = r.sim.node_metrics.find((x) => x.id === id);
    const node = graph.nodes.find((n) => n.id === id);
    if (!m?.queue || !node) continue;
    const need = Math.min(100_000_000, roundUp(Math.max(m.queue.max_depth * 2, m.queue.backlog_rps * 600 * 1.2)));
    const fix = fixWith(graph, node, [['max_depth', need]], `${node.label}: fills up within ten minutes because the workers cannot keep up.`, `Make room for ${fmt(need)} messages, so it can ride out the gap.`);
    if (fix) return fix;
  }
  return null;
}

function slowProviderFix(graph, r, target, advice) {
  if (r.metrics.latency_ms.p95 <= target) return [];
  const slow = r.sim.node_metrics
    .filter((m) => m.type === 'ThirdPartyAPI' && m.on_request_path && m.latency_ms > target / 2)
    .sort((a, b) => b.latency_ms - a.latency_ms);
  const fixes = [];
  for (const m of slow) {
    const node = graph.nodes.find((n) => n.id === m.id);
    if (DEFERRABLE_PROVIDERS.has(param(node, 'provider'))) {
      const moved = behindQueue(graph, node, m.latency_ms);
      if (moved) fixes.push(moved);
    } else {
      advice.add(`If callers can live without ${node.label}'s answer for a moment, put it behind a queue so nobody waits for it.`);
    }
    const timeout = Math.max(500, Math.round((Number(param(node, 'base_latency')) || 100) * 2));
    const current = Number(param(node, 'timeout_ms')) || 0;
    const capped = fixWith(graph, node, [['timeout_ms', current > 0 ? Math.min(current, timeout) : timeout], ['circuit_breaker', true]],
      `${node.label}: ${ms(m.latency_ms)} per call in this test, and callers wait for it.`, `Give up after ${ms(timeout)} and stop calling it while it fails (timeout and circuit breaker).`);
    if (capped) fixes.push(capped);
  }
  return fixes;
}

// Everything downstream of a Kafka topic: its readers and whatever they call.
function consumersAndBeyond(graph) {
  const types = new Map(graph.nodes.map((n) => [n.id, n.type]));
  const out = new Set();
  const stack = graph.edges.filter((e) => types.get(e.source) === 'KafkaTopic').map((e) => e.target);
  while (stack.length) {
    const id = stack.pop();
    if (out.has(id) || types.get(id) === 'KafkaTopic') continue;
    out.add(id);
    for (const e of graph.edges) if (e.source === id) stack.push(e.target);
  }
  return out;
}

// Every change that might help, most important first.
function candidateFixes(graph, r, problem, advice, testCase) {
  const target = problem.scale?.latency_target_ms || 300;
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const out = [];
  const push = (fix) => { if (fix) out.push(fix); };
  if (r.failure && !r.failure.redundant && byId.has(r.failure.node_id)) push(redundancyFix(graph, byId.get(r.failure.node_id)));
  if (r.metrics.error_rate > 0.01) push(limiterFix(graph, problem));
  kafkaFixes(graph, r, testCase, { fixWith, fixMany, advice }).forEach(push);
  // When a test lets consumers fall behind, parts behind them are not worth enlarging.
  const behindConsumers = testCase?.ignore_freshness ? consumersAndBeyond(graph) : new Set();
  const busy = r.sim.node_metrics
    .filter((m) => m.type !== 'User' && (m.status === 'overloaded' || m.status === 'hot') && byId.has(m.id) && !behindConsumers.has(m.id))
    .sort((a, b) => b.utilization - a.utilization);
  for (const m of busy) push(capacityFix(graph, byId.get(m.id), m, r));
  push(queueDepthFix(graph, r));
  slowProviderFix(graph, r, target, advice).forEach(push);
  return out;
}

// The first candidate that does not lower the score. If every one does (more servers can push
// more traffic onto a database that is also full), take the most important one anyway: the
// next step usually fixes what it exposed.
function nextFix(graph, r, problem, advice, run, testCase) {
  let fallback = null;
  for (const fix of candidateFixes(graph, r, problem, advice, testCase)) {
    const next = run(fix.graph);
    if (next.score >= r.score) return { fix, result: next };
    fallback ??= { fix, result: next };
  }
  return fallback;
}

// ---------------------------------------------------------------------------- explanations
function explain(r, problem) {
  const target = problem.scale?.latency_target_ms || 300;
  const out = [];
  if (r.failure && !r.failure.redundant) out.push(`${r.failure.label} failed and nothing could take over.`);
  else if (r.failure) out.push(`${r.failure.label} lost a copy, and the others had to carry its share.`);
  const over = r.sim.node_metrics.filter((m) => m.type !== 'User' && m.status === 'overloaded').sort((a, b) => b.utilization - a.utilization).slice(0, 2);
  for (const m of over) out.push(`${m.label} got ${fmt(m.demand_rps)} requests a second but can handle about ${fmt(m.capacity_rps ?? 0)}.`);
  for (const id of r.sim.async.overflowing) out.push(`${r.sim.node_metrics.find((m) => m.id === id)?.label || id} filled up, so new work was turned away.`);
  if (r.metrics.error_rate > 0.01) out.push(`${cappedPct(r.metrics.error_rate, 1)} of requests failed.`);
  if (r.metrics.latency_ms.p95 > target) out.push(`The slowest 5% took ${ms(r.metrics.latency_ms.p95)}, over the ${target} ms target.`);
  out.push(...kafkaReasons(r, problem.scale?.freshness_target_ms));
  if (!out.length) out.push(`It scored ${r.score}; a test passes at 60.`);
  return out;
}

function generalAdvice(r, advice) {
  for (const m of r.sim.node_metrics) {
    if (m.status !== 'overloaded' && m.status !== 'down') continue;
    if (m.type === 'Database') advice.add(`A cache in front of ${m.label} would answer repeated reads before they reach it.`);
    if (m.type === 'Queue') advice.add(`${m.label} needs consumers that keep up: add workers or let them autoscale.`);
    if (m.type === 'ThirdPartyAPI') advice.add(`Call ${m.label} less: cache its answers, or move the calls behind a queue.`);
    if (m.kafka?.role === 'consumer' && m.kafka.idle > 0) advice.add(`${m.label} has idle consumers: only more partitions add parallelism now.`);
  }
}

const brief = (r) => ({ score: r.score, pass: r.pass, error_rate: r.metrics.error_rate, p95: r.metrics.latency_ms.p95 });

// The greedy search can overshoot (an early fix sized for a part slowed by a later one), so
// every raised number is brought back down to the smallest value that still passes.
const TRIM_ORDER = ['autoscale_min', 'replicas', 'read_replicas', 'shards', 'autoscale_max', 'capacity', 'max_depth'];
const ROUNDED = new Set(['capacity', 'max_depth']);

function setValue(graph, id, key, value) {
  const node = graph.nodes.find((n) => n.id === id);
  const spec = paramSpec(node.type, key);
  node.config[spec.block] = { ...(node.config[spec.block] || {}), [key]: value };
}

function trim(original, solved, passes) {
  const g = cloneGraph(solved);
  const before = new Map(original.nodes.map((n) => [n.id, n]));
  for (const n of g.nodes) {
    const old = before.get(n.id);
    if (!old) continue;
    for (const key of TRIM_ORDER) {
      if (!paramSpec(n.type, key)) continue;
      let lo = Number(param(old, key));
      let hi = Number(param(n, key));
      if (key === 'autoscale_max') lo = Math.max(lo, Number(param(n, 'autoscale_min')));
      // A fixed pool that now autoscales keeps at least the instances it had.
      if (key === 'autoscale_min' && !param(old, 'autoscaling')) lo = Math.max(lo, Number(param(old, 'replicas')) || 1);
      if (!(hi > lo)) continue;
      while (hi - lo > (ROUNDED.has(key) ? hi * 0.05 : 1)) {
        const mid = ROUNDED.has(key) ? roundUp((lo + hi) / 2) : Math.floor((lo + hi) / 2);
        if (mid >= hi) break;
        setValue(g, n.id, key, mid);
        if (passes(g)) hi = mid;
        else lo = mid;
      }
      setValue(g, n.id, key, hi);
    }
  }
  return g;
}

function cacheAdvice(original, fixes, advice) {
  if (original.nodes.some((n) => n.type === 'Cache')) return;
  for (const f of fixes) {
    const reads = f.changes?.find((c) => c.key === 'read_replicas' && c.to >= 3) || f.changes?.find((c) => c.key === 'shards' && c.to >= 4);
    if (reads) advice.add(`Cheaper option for ${f.label}: put a cache in front of it, so repeated reads never reach it.`);
  }
}

// One entry per part that ends up different, with its final values and why it needed changing.
function fixesFrom(original, final, steps) {
  const before = new Map(original.nodes.map((n) => [n.id, n]));
  const cause = (id) => steps.find((s) => s.changes.some((c) => c.id === id))?.cause || '';
  const added = new Set();
  const out = [];
  for (const s of steps) {
    if (!s.structural) continue;
    s.changes.forEach((c) => added.add(c.id));
    out.push({ id: s.changes[0].id, label: s.structural.subject, reason: s.cause, lines: s.structural.lines });
  }
  for (const n of final.nodes) {
    const comp = COMPONENTS[n.type];
    const old = before.get(n.id);
    if (!comp || added.has(n.id)) continue;
    if (!old) {
      out.push({ id: n.id, label: n.label, reason: cause(n.id), lines: [`New ${comp.label}`] });
      continue;
    }
    const changes = [];
    for (const spec of allSpecs(comp)) {
      const from = param(old, spec.key);
      const to = param(n, spec.key);
      if (from !== to) changes.push({ key: spec.key, name: settingName(n.type, spec), from, to });
    }
    if (!changes.length) continue;
    if (changes.some((c) => c.key === 'autoscaling' && c.to === true)) {
      for (const key of ['autoscale_min', 'autoscale_max']) {
        if (!changes.some((c) => c.key === key)) changes.push({ key, name: paramSpec(n.type, key).label, from: param(old, key), to: param(n, key) });
      }
    }
    out.push({ id: n.id, label: n.label, reason: cause(n.id), changes });
  }
  return out;
}

// Readable lines for one fix, e.g. "Autoscaling on, 9 to 54 instances" or "Shards: 1 → 3".
export function changeLines(fix) {
  if (fix.lines) return fix.lines;
  const by = new Map(fix.changes.map((c) => [c.key, c]));
  const skip = new Set();
  const lines = [];
  const min = by.get('autoscale_min');
  const max = by.get('autoscale_max');
  if (by.get('autoscaling')?.to === true) {
    lines.push(`Autoscaling on, ${min.to} to ${max.to} instances`);
    ['autoscaling', 'autoscale_min', 'autoscale_max', 'replicas'].forEach((k) => skip.add(k));
  } else if (min && max) {
    lines.push(`Autoscaling range: ${min.from}–${max.from} → ${min.to}–${max.to} instances`);
    ['autoscale_min', 'autoscale_max'].forEach((k) => skip.add(k));
  }
  for (const c of fix.changes) if (!skip.has(c.key)) lines.push(`${c.name}: ${formatValue(c.from)} → ${formatValue(c.to)}`);
  return lines;
}

// ---------------------------------------------------------------------------- public
export function solveTest(graph, problem, testCase, { maxSteps = MAX_STEPS } = {}) {
  const run = (g) => runTestCase(g, problem, testCase, { baseline: simulate(g, { traffic: problemTraffic(problem, 1) }) });
  let g = cloneGraph(graph);
  let r = run(g);
  const before = r;
  const steps = [];
  const advice = new Set();
  for (let i = 0; i < maxSteps && !before.pass && r.score < GOOD_SCORE; i++) {
    const found = nextFix(g, r, problem, advice, run, testCase);
    if (!found) break;
    g = found.fix.graph;
    const { graph: _, ...step } = found.fix;
    steps.push(step);
    r = found.result;
  }
  if (r.pass && steps.length) {
    const keep = Math.min(GOOD_SCORE, r.score);
    g = trim(graph, g, (x) => run(x).score >= keep);
    r = run(g);
  }
  if (!r.pass) generalAdvice(r, advice);
  const fixes = fixesFrom(graph, g, steps);
  cacheAdvice(graph, fixes, advice);
  return {
    id: testCase.id, name: testCase.name, before: brief(before), after: brief(r),
    why: before.pass ? [] : explain(before, problem), steps, fixes, advice: [...advice], graph: g,
  };
}

// Solves every failing test in turn on one copy of the design, so the changes add up.
export function solveSuite(graph, problem, { tiers = ['component', 'system'] } = {}) {
  const start = runSuite(graph, problem, { tiers });
  let g = cloneGraph(graph);
  const steps = [];
  const advice = new Set();
  const passingIds = (x) => new Set(runSuite(x, problem, { tiers }).results.filter((t) => t.pass).map((t) => t.id));
  for (let round = 0; round < 3; round++) {
    const failing = runSuite(g, problem, { tiers }).results.filter((t) => !t.pass);
    if (!failing.length) break;
    let changed = false;
    for (const t of failing) {
      const res = solveTest(g, problem, problem.tests.find((x) => x.id === t.id));
      res.advice.forEach((a) => advice.add(a));
      if (!res.steps.length) continue;
      // A fix for one test must not break a test that already passed.
      const was = passingIds(g);
      const now = passingIds(res.graph);
      if ([...was].some((id) => !now.has(id))) continue;
      g = res.graph;
      steps.push(...res.steps.map((s) => ({ ...s, test: t.name })));
      changed = true;
    }
    if (!changed) break;
  }
  if (steps.length) {
    const keep = new Map(runSuite(g, problem, { tiers }).results.filter((t) => t.pass).map((t) => [t.id, Math.min(GOOD_SCORE, t.score)]));
    g = trim(graph, g, (x) => runSuite(x, problem, { tiers }).results.every((t) => !keep.has(t.id) || t.score >= keep.get(t.id)));
  }
  const end = runSuite(g, problem, { tiers });
  const after = new Map(end.results.map((t) => [t.id, t]));
  const fixes = fixesFrom(graph, g, steps);
  cacheAdvice(graph, fixes, advice);
  return {
    before: start.summary,
    after: end.summary,
    tests: start.results.map((t) => ({ id: t.id, name: t.name, before: t.score, after: after.get(t.id).score, pass: after.get(t.id).pass })),
    steps,
    fixes,
    changes: describeChanges(graph, g),
    advice: [...advice],
    graph: g,
  };
}

// Plain-language list of what differs between two versions of a design.
export function describeChanges(before, after) {
  const out = [];
  const a = new Map(before.nodes.map((n) => [n.id, n]));
  const b = new Map(after.nodes.map((n) => [n.id, n]));
  const name = (id) => b.get(id)?.label || a.get(id)?.label || id;
  for (const n of after.nodes) if (!a.has(n.id)) out.push(`Add ${COMPONENTS[n.type]?.label || n.type}: ${n.label}.`);
  for (const n of before.nodes) if (!b.has(n.id)) out.push(`Remove ${n.label}.`);
  for (const n of after.nodes) {
    const old = a.get(n.id);
    const comp = COMPONENTS[n.type];
    if (!old || !comp) continue;
    for (const spec of allSpecs(comp)) {
      const from = param(old, spec.key);
      const to = param(n, spec.key);
      if (from !== to) out.push(`${n.label}: ${inSentence(settingName(n.type, spec))} ${formatValue(from)} → ${formatValue(to)}.`);
    }
  }
  const key = (e) => `${e.source}\u0000${e.target}`;
  const ea = new Map(before.edges.map((e) => [key(e), e]));
  const eb = new Map(after.edges.map((e) => [key(e), e]));
  for (const [k, e] of eb) {
    const old = ea.get(k);
    if (!old) out.push(`Connect ${name(e.source)} → ${name(e.target)}${e.kind === 'async' ? ' (async)' : ''}.`);
    else if ((old.kind || 'sync') !== (e.kind || 'sync')) out.push(`Arrow ${name(e.source)} → ${name(e.target)}: make it ${e.kind || 'sync'}.`);
  }
  for (const [k, e] of ea) if (!eb.has(k)) out.push(`Remove the arrow ${name(e.source)} → ${name(e.target)}.`);
  return out;
}
