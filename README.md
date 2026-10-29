# Kafka Lab

Kafka Lab is a system design and Apache Kafka simulator that runs on your own computer.

You build a system on a canvas from parts like servers, caches, databases and Kafka topics. Then you send pretend traffic through it and watch what happens: which part gets busy, which part breaks first, where consumers fall behind, and whether any message is lost or processed twice. It is made for learning high-level design and Kafka, and for practising system design interviews.

It started as a copy of HLD Lab (in `Learning/hld-lab`), so every system design feature is still here. Kafka is added on top: Kafka parts on the canvas, a Kafka model in the simulator, Kafka systems, incidents, lessons and interview questions.

There is no sign-in, no profile and no database. The server stores nothing: it only runs the simulations and grades answers. Every feature is on for everyone.

## Start it

You need [Node.js](https://nodejs.org) 22 or newer. There is nothing to install: the app has no dependencies.

```bash
cd Learning/kafka-lab
npm start
```

Then open <http://localhost:3100>. HLD Lab uses port 3000, so both can run side by side.

To use another port, set `PORT`, for example `PORT=8080 npm start`.

## What you can do

| Area | What it is |
| --- | --- |
| Learn | 32 real systems, 8 of them built around Kafka (clickstream analytics, order events with an outbox, driver locations, a bank ledger, a log pipeline, notification fan-out, catalog CDC, IoT telemetry across regions). Each one grows in 4 steps, and each step explains why a part was added. Use the **Kafka** filter to see only the Kafka systems. |
| Practice | Read a short brief and build the system yourself. **Run tests** checks normal traffic, a spike and failures. **Submit** runs bigger tests and gives you a score. Every failing test has a worked solution. |
| Build & stress test | Build anything. The stress test pushes traffic from half to ten times normal and shows the moment it breaks. Focus tests include Kafka ones: lose a broker, consumers down for a day, a burst of events, a poison message and a breaking schema change. |
| Design & Fix | 24 broken systems, 12 of them Kafka: consumer lag, idle consumers, a hot partition, slow producers, orders lost on broker failure, a write outage from min.insync.replicas, a rebalance storm, a poison message, double charges, data lost to retention, a breaking schema change and a dual write. |
| Mock interview | Four timed rounds: requirements, design (load-tested), quick questions and trade-offs. Kafka designs get Kafka questions (keys and partitions, delivery guarantees, broker failure, lag, the outbox). |
| Roadmap | 32 short lessons in 8 steps: 20 on system design, then 12 on Kafka. Each has a 3-question quiz and a link to try it on the canvas. |

## The Kafka parts

They are in the **Kafka** group of the parts list.

| Part | What it stands for | Settings you can change |
| --- | --- | --- |
| Kafka Topic | A topic and the cluster it lives on | partitions, brokers, replication factor, min.insync.replicas, producer acks, idempotent producer, message key (even, none, or skewed with a hot key share), linger.ms, compression, message size, retention, cleanup policy (delete or compact), rack awareness, tiered storage, KRaft or ZooKeeper, TLS, client authentication, ACLs |
| Consumer Group | Consumers sharing the work of reading topics | consumers, autoscaling, offset commit (after processing, before processing, transactional), idempotent processing, rebalance protocol (cooperative or eager), static membership, max.poll.records, max.poll.interval.ms, dead letter topic |
| Stream Processor | Kafka Streams, ksqlDB or Flink | instances, autoscaling, at-least-once or exactly-once, stateless or stateful, standby replicas, windowing, skipping bad records |
| Kafka Connect | Source and sink connectors, including CDC (Debezium) | source or sink, connector type, tasks, dead letter topic, exactly-once |
| Schema Registry | Schemas and compatibility rules | compatibility (backward, forward, full, none), format |
| MirrorMaker | Copies topics between clusters | tasks, offset sync |

Draw arrows the way messages flow: a service publishes to a topic (the arrow is always async), the topic points at every consumer group that reads it, and a database points at a Kafka Connect source to stream its changes (CDC).

## How Kafka is simulated

- **Partitions** split a topic. Each partition takes a set number of messages a second, so a topic carries partitions × that much, if its brokers can keep up.
- **Brokers** store every message once per copy, so the cluster carries about brokers × broker speed ÷ replication factor. **Batching** (linger.ms) and **compression** let the same brokers carry more.
- **Keys** pick the partition. A skewed key sends its share of all messages to one partition and one consumer, which then caps the whole topic and group.
- **Every consumer group reads every message.** Inside a group, each partition goes to one consumer, so at most as many consumers as partitions do any work, and the consumer with the most partitions sets the pace. Autoscaling stops at the partition count.
- **Lag** builds when a group reads slower than producers write. It is measured as messages behind and as **data age** (how old the newest data the group has handled is). Systems built on Kafka have a **freshness target**, and consumers that fall behind fail it.
- **Retention:** messages older than the retention time are deleted, read or not. A group that falls further behind loses them. Compacted topics keep the latest message per key.
- **Losing a broker:** partitions it led move to other brokers. With acks=all, a write needs min.insync.replicas copies, so too few copies means rejected writes. With one copy, acks=1 or min.insync.replicas 1, confirmed messages can be lost.
- **Consumer crashes and rebalances:** commit after processing replays the last batch (duplicates, unless processing is idempotent). Commit before processing skips it (lost messages). Transactional processing does neither. Eager rebalancing pauses the whole group. A lost stateful instance must rebuild its state unless it has a standby replica.
- **Rebalance storms:** if one poll's messages take longer than max.poll.interval.ms, Kafka keeps evicting consumers and the group makes almost no progress.
- **Poison messages** block their partition, and every other partition the stuck consumer owns, unless the consumer has a dead letter topic.
- **Schema changes** that are not backward compatible stop every consumer, unless a Schema Registry with a compatibility rule rejects them first. MirrorMaker copies raw bytes, so neither affects it.
- **Dual writes:** a service that writes a database and publishes to Kafka separately can leave them disagreeing. Change data capture (the outbox pattern) cannot.

Kafka tests can require delivery guarantees: **no message may be lost**, **no message may be processed twice**, **events for one entity stay in order**, **the database and the events never disagree**, and **bad messages never block the stream**. A test that breaks one cannot pass, and its row shows which guarantee broke and why.

## Using the canvas

- **Add a part:** click it in the list on the left, or drag it onto the canvas.
- **Connect two boxes:** drag from the small dot on the right of a box to another box. Or select a box and pick a target under **Arrows** in the right panel.
- **Build quickly:** select a box, then click a part. The new part is added after it, with an arrow.
- **Change settings:** click a box or an arrow. Settings show on the right, each with a short explanation. For topics and consumer groups, the panel also shows brokers up, rejected writes, working and idle consumers, lag and data age.
- The **live preview** colours every box by how busy it is, as you edit, and **Live hints** include a Kafka section.
- **Run simulation** sends live traffic through your design. See below.
- **Undo** is Ctrl/⌘ Z, **delete** is Del, and **Tidy** lines everything up.
- **Keep a design:** use the download button in the toolbar to save it as a file, and the upload button to open it again.

## Run a live simulation

Press **Run simulation** in the toolbar. It is there in Learn, Practice, Build and Design & Fix. Dots move along the arrows, boxes change colour as they get busy, and a bar at the top of the canvas shows requests per second, the share that is served and how slow the slowest 5% are.

The **Run** tab on the right is the control room:

- **Speed:** let time pass at 1×, 2×, 5× or 10×.
- **Traffic:** set the level from 0.1× to 10× normal, fire a spike, ramp up to 10× over two minutes, or play a day-and-night cycle. Add bots to see rate limiting at work.
- **Break something:** take a part down, lose one copy, make it slower, or empty a cache. For Kafka: take the whole cluster down, lose one broker, stop all consumers, crash one consumer, send a poison message, or deploy a breaking schema.
- **Parts and capacity:** see how busy every part is and add capacity with **+** and **−** while traffic flows. For a topic, **+** adds partitions. There is no **−**, because Kafka can never remove partitions.
- **What happened:** a plain-language log, such as "Payments: falling behind. Lag grows by 600 messages a second" or "orders: 100% of writes are rejected".

Things take time, as in real life. Autoscaled servers and consumers need 30 seconds to start. Queues fill and drain. Consumer lag builds while a group is down, and when it comes back it works flat out to catch up, which loads the database behind it harder than usual. Messages that sit unread longer than the retention time are deleted. A third chart shows messages behind and data age. Press **Stop** for a short report, including peak lag, the oldest data, and how many messages expired.

## Solutions for failing tests

In Learn and Practice, every failing test has a **Show the solution** link. It explains why the test fails in your design and lists what to change, part by part, with the reason for each change. **Apply to my design** makes the changes and runs the tests again. You can undo it. Above the tests, **Stuck? Show the solution** gives one set of changes that makes every failing test pass at once.

The solutions are worked out for your design. On a copy of it, Kafka Lab makes one change at a time and runs the test again after each one. Then it shrinks every number back to the smallest value that still passes. For Kafka it knows these fixes:

- acks=all with 3 copies and min.insync.replicas 2
- an idempotent producer, and keying by entity id
- commit after processing, idempotent consumers, or exactly-once
- dead letter topics, and a Schema Registry
- longer retention
- more partitions and brokers, batching and compression
- consumers up to the partition count, then more partitions (with two partitions per consumer when a crash must be survivable)
- smaller polls against rebalance storms
- standby replicas for stateful stream processors
- the outbox pattern with a CDC connector instead of a dual write

In Design & Fix, **Show the solution** (in step 2) explains the cause and the exact fix, and can apply it to the canvas.

## How the simulator works

Every part can handle a set number of requests per second. Traffic flows along your arrows.

- When a part gets more than it can handle, requests wait, slow down and then fail.
- Caches and CDNs answer some requests early, so less traffic reaches the database.
- Database copies (read replicas) help with reads. Only splitting the data (shards) helps with writes.
- Retries add load. Timeouts stop long waits. Circuit breakers fail fast.
- Queues and Kafka topics soak up bursts. Producers never wait for consumers, but consumers that fall too far behind lose data or freshness.
- Failure tests switch a part off to see if the rest keeps working.

The same engine runs in your browser (for the live preview, live runs and Practice tests) and on the server (for scores), so the numbers always match.

## AI feedback in interviews

A built-in checklist always grades your interview answers. To also get written feedback from an AI model, put the settings in a file named `.env` in the `kafka-lab` folder (it is never committed to git), then run `npm start` again.

**With OpenAI:**

```text
OPENAI_API_KEY=sk-...
```

**With a free model on your own computer**, for example [Ollama](https://ollama.com). No key is needed:

```text
OPENAI_BASE_URL=http://localhost:11434/v1
OPENAI_MODEL=llama3.2
```

Any OpenAI-compatible service works. `OPENAI_MODEL` defaults to `gpt-4o-mini`. If a call to the model fails, the checklist is used instead.

## What is kept, and where

Nothing is kept on the server. Your browser keeps your drafts, your place in incidents and Learn, and your latest mock interview, so a refresh does not lose your work. Kafka Lab uses its own browser storage, separate from HLD Lab. To start over, clear this site's data in your browser.

## Commands

| Command | What it does |
| --- | --- |
| `npm start` | Start the app on port 3100. |
| `npm run dev` | Start the app and restart it when a server file changes. |

## Deploy on Netlify

Connect the repository and set the base directory to `Learning/kafka-lab` when this folder is not the root of the repo. The build command and the publish directory are in `netlify.toml`: the command only prints the Node version, and the publish directory is `public`.

Netlify serves `app.js`, `app.css` and `engine.js` as files. Every page and every `/api` route runs as a serverless function (`netlify/functions/site.js`), which is the same server as local `npm start`. There is still no `index.html`.

For written interview feedback, add `OPENAI_API_KEY`, `OPENAI_BASE_URL` or `OPENAI_MODEL` in the Netlify UI. Do not commit them.

## Add a system

All systems live in `content.js`: system design systems in its `content: problems` and `content: more problems` sections, Kafka systems in `content: kafka problems`. Copy one that is close to yours and change the brief, the numbers and the 4 steps. Each step is a small design written with `node(...)` and `link(...)`; a typo in a part or setting name stops the server with a clear error. For Kafka systems, `kafkaTests(...)` builds the usual tests, and `requires` adds delivery guarantees to them.

Two rules keep the steps useful: the last step must pass every test, and the first step must fail at least one. Every failing test in every step should also have a worked solution.

## Project layout

```text
HLD-Visulaiser/
├── server.js          pages, JSON API and interview grading
├── content.js         systems, incidents, roadmap lessons and interview questions, Kafka included
├── netlify.toml       Netlify build command, publish directory and function settings
├── netlify/functions/
│   └── site.js        the same server, as a Netlify function
└── public/
    ├── engine.js      the simulator and the Kafka model, used by both the browser and the server
    ├── app.js         the browser code for every page
    └── app.css        the styles
```

Each file is split into sections with a banner comment, such as `// ==== kafka model` or `// ==== traffic simulator`, so you can jump to a part quickly.

## About PRISM

HLD Lab, and so Kafka Lab, was inspired by studying [PRISM](https://www.prismhld.com/), a system design simulator. It follows the same big ideas (a canvas you load-test, guided systems, broken systems to fix and mock interviews), but all of its code, text, problems, incidents and lessons are original.

## Troubleshooting

- **"Port 3100 is already in use":** another program is using that port. Start on another one with `PORT=3101 npm start`.
- **A syntax error when it starts:** your Node.js is too old. Install Node 22 or newer.
- **The page looks broken after an update:** do a hard refresh (Ctrl/⌘ Shift R).
- **The text is in a plain font:** the fonts (Manrope and Noto Serif Display) come from Google Fonts. Offline, the app uses your system fonts instead; everything else still works.
