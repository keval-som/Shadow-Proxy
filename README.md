# Shadow Proxy

A high-performance, stateless database proxy for PostgreSQL, built with Java 21 and Netty. Shadow Proxy sits transparently between your application and PostgreSQL, providing read-write splitting, two-level query caching, session management, circuit breaking, and a full async audit trail — all without the application knowing the proxy exists.

---

## Table of Contents

1. [The Idea](#the-idea)
2. [Feature Overview](#feature-overview)
3. [Technology Stack](#technology-stack)
4. [System Architecture](#system-architecture)
5. [Component Breakdown](#component-breakdown)
6. [Data Flow](#data-flow)
7. [Protocol Handling](#protocol-handling)
8. [Caching Strategy](#caching-strategy)
9. [Routing Strategy](#routing-strategy)
10. [Circuit Breaker](#circuit-breaker)
11. [Session Management](#session-management)
12. [Audit Telemetry](#audit-telemetry)
13. [Directory Structure](#directory-structure)
14. [Configuration](#configuration)
15. [Running the Project](#running-the-project)
16. [Testing](#testing)
17. [Design Decisions](#design-decisions)

---

## The Idea

Most web services talk to a single PostgreSQL instance. As traffic grows, two problems emerge: every read hits the same primary node, and repeated identical queries hit the database even when the result has not changed.

Shadow Proxy intercepts all database traffic at the network level using the native PostgreSQL wire protocol (v3). It requires zero changes to the application code or JDBC driver configuration. The proxy then:

- Routes write queries (INSERT, UPDATE, DELETE, DDL) to the primary node
- Routes read queries (SELECT) to a replica node
- Caches SELECT results in a two-level cache (in-process memory + Redis)
- Protects each backend node with a circuit breaker
- Persists client session state in Redis so the proxy itself stays stateless
- Publishes an async audit event for every proxied command to RabbitMQ

The goal is to demonstrate a Big Tech-level system design pattern: a transparent infrastructure layer that improves reliability, performance, and observability without any application-level changes.

---

## Feature Overview

| Feature | Implementation |
|---|---|
| PostgreSQL Wire Protocol v3 | Custom Netty decoder/encoder |
| Read-Write Splitting | JSQLParser AST-based routing |
| L1 Cache | Caffeine in-process cache (sub-microsecond) |
| L2 Cache | Redis via async Lettuce client |
| Cache Invalidation | Automatic on DDL commands |
| Session Management | Redis HSET with TTL |
| Circuit Breaker | Sliding-window state machine per node |
| Connection Pooling | HikariCP per backend node |
| Async Audit Trail | RabbitMQ via AMQP |
| Concurrency | Java 21 Virtual Threads on Netty worker pool |
| Stateless Proxy | All mutable state stored in Redis |
| Graceful Shutdown | JVM shutdown hook drains all resources |

---

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Network I/O | Netty | 4.1.107.Final |
| SQL Parsing | JSQLParser | 5.3 |
| L1 Cache | Caffeine | 3.1.8 |
| L2 Cache / Session | Lettuce (Redis client) | 6.3.2 |
| Message Queue | RabbitMQ AMQP client | 5.20.0 |
| Connection Pool | HikariCP | 5.1.0 |
| Database | PostgreSQL JDBC | 42.7.3 |
| Serialisation | Jackson | 2.17.1 |
| Runtime | Java 21 (Virtual Threads, preview) | 21 |
| Containerisation | Docker Compose | - |
| Build | Maven (Shade plugin for fat JAR) | - |

---

## System Architecture

```
+------------------+
|   Application    |
|  (psql / JDBC)   |
+--------+---------+
         |
         | PostgreSQL Wire Protocol v3
         | port 5432
         v
+--------+---------+
|                  |
|  Shadow Proxy    |  <-- Netty NIO server
|                  |      Java 21 Virtual Threads
|  +-----------+  |
|  | Pipeline  |  |
|  |-----------|  |
|  | Decoder   |  |  Parses raw TCP bytes into DatabaseCommand objects
|  | CacheLayer|  |  L1 hit? serve. L2 hit? serve + warm L1. Miss? pass on.
|  | Backend   |  |  Routes, checks circuit breaker, executes via JDBC
|  | Encoder   |  |  Encodes responses back to PostgreSQL wire format
|  +-----------+  |
|                  |
+--+--------+------+
   |        |
   |        |  session state / L2 cache
   |        v
   |  +-----+------+
   |  |   Redis    |
   |  | L2 Cache   |
   |  | Sessions   |
   |  +------------+
   |
   |  audit events
   v
+--+----------+
|  RabbitMQ   |
|  shadow.    |
|  audit      |
+-------------+

   Routing Decision:
   SELECT  -->  Replica Node (port 5434)
   INSERT  -->  Primary Node (port 5433)
   UPDATE  -->  Primary Node (port 5433)
   DELETE  -->  Primary Node (port 5433)
   DDL     -->  Primary Node (port 5433) + cache invalidation

+----------------------+      Streaming
|  postgres-primary    | <--- Replication ---> +--------------------+
|  port 5433 (writes)  |                       | postgres-replica   |
+----------------------+                       | port 5434 (reads)  |
                                               +--------------------+
```

---

## Component Breakdown

### ShadowProxyServer

The Netty `ServerBootstrap`. Uses a single-thread `NioEventLoopGroup` for the boss (accepts connections) and a `NioEventLoopGroup` backed by Java 21 Virtual Threads for workers (handles I/O). This means each I/O callback runs on a virtual thread, allowing blocking-style JDBC code without blocking the NIO event loop.

### ShadowProxyInitializer

A Netty `ChannelInitializer` that assembles the pipeline for every new TCP connection:

```
[PostgresCommandDecoder] --> [PostgresResponseEncoder] --> [CacheHandler] --> [BackendHandler]
```

Each handler is instantiated per-connection so there is no shared mutable state in the pipeline.

### PostgresCommandDecoder

A `ByteToMessageDecoder` implementing the PostgreSQL frontend wire protocol v3. It handles:

- SSL negotiation (`SSLRequest` magic `0x04D2162F`): responds with `N` (not supported) so the client falls back to cleartext
- `StartupMessage`: responds with `AuthenticationOk` + `ReadyForQuery`, saves session to Redis
- `'Q'` (simple query) messages: reads the null-terminated SQL string, classifies it via JSQLParser, emits a `PostgresCommand` into the pipeline
- `'X'` (terminate): closes the channel and deletes the session from Redis

### CacheHandler

A `ChannelDuplexHandler` implementing two-level read-through caching:

- **Inbound path**: checks L1, then L2 asynchronously. On a hit, writes cached bytes directly to the client and stops the pipeline. On a miss, records the pending cache key and passes the command downstream.
- **Outbound path**: when a response flows back from `BackendHandler`, captures the bytes, stores them in L1 and L2, then forwards them to the client.
- **Invalidation**: on DDL commands, calls `invalidateAll()` on both L1 and L2 before the command reaches the backend.

### BackendHandler

The terminal inbound handler. For every `DatabaseCommand`:

1. Calls `router.route(command)` to select the target `BackendNode`
2. Calls `breakerFor(node).allowRequest()` — fails fast if the circuit is open
3. Submits the JDBC call to a `VirtualThreadPerTaskExecutor` (never blocks the NIO thread)
4. On success: records success on the circuit breaker, writes the wire-encoded response, publishes an audit event
5. On failure: records failure on the circuit breaker, writes a PostgreSQL `ErrorResponse`, publishes a failure audit event

### NodeRegistry

Holds the primary node and the list of replica nodes. Creates one `CircuitBreaker` instance per node at startup. `getReplicaNode()` iterates replicas and returns the first healthy one (circuit not OPEN), falling back to primary if all replicas are circuit-open.

### ReadWriteRoutingStrategy

Implements `RoutingStrategy`. Routes `SELECT` to `nodeRegistry.getReplicaNode()` and all other command types to `nodeRegistry.getPrimaryNode()`.

---

## Data Flow

### Full Request Lifecycle (Cache Miss)

```
Client                  Proxy                           Redis        PostgreSQL
  |                       |                               |               |
  |-- TCP connect ------->|                               |               |
  |                       |-- HSET session:{id} -------->|               |
  |<- AuthOk + RFQ -------|                               |               |
  |                       |                               |               |
  |-- Q: SELECT * ------->|                               |               |
  |                       |-- HGET cache:{sha256} ------->|               |
  |                       |<- (nil) ----------------------|               |
  |                       |                               |               |
  |                       |-- [virtual thread] JDBC ------------------>   |
  |                       |<- ResultSet -----------------------------------|
  |                       |                               |               |
  |                       |-- SET cache:{sha256} -------->|               |
  |                       |-- Caffeine.put(key, bytes)    |               |
  |                       |                               |               |
  |<- RowDescription  ----|                               |               |
  |<- DataRow(s)     -----|                               |               |
  |<- CommandComplete ----|                               |               |
  |<- ReadyForQuery  -----|                               |               |
  |                       |                               |               |
  |                       |-- publish AuditEvent -------> RabbitMQ        |
```

### Cache Hit (L1)

```
Client                  Proxy
  |                       |
  |-- Q: SELECT * ------->|
  |                       |-- Caffeine.get(key) --> HIT (bytes in memory)
  |<- [cached response] --|
  |                       |
  |  No Redis call. No JDBC call. Response in microseconds.
```

### Cache Hit (L2, L1 cold)

```
Client                  Proxy                        Redis
  |                       |                            |
  |-- Q: SELECT * ------->|                            |
  |                       |-- HGET cache:{sha256} ---->|
  |                       |<- bytes -------------------|
  |                       |-- Caffeine.put(key, bytes) |  (warm L1 from L2)
  |<- [cached response] --|                            |
  |                       |                            |
  |  No JDBC call. Redis round-trip only.
```

### Write with Cache Invalidation

```
Client                  Proxy                        Redis        PostgreSQL
  |                       |                            |               |
  |-- Q: ALTER TABLE ---->|                            |               |
  |                       |-- Caffeine.invalidateAll() |               |
  |                       |-- DEL (all cache keys) --->|               |
  |                       |                            |               |
  |                       |-- [virtual thread] JDBC ------------------>|
  |<- CommandComplete -----|                           |               |
```

---

## Protocol Handling

Shadow Proxy speaks native PostgreSQL wire protocol v3. No middleware, no JDBC on the proxy-client side.

### Connection Handshake

```
Client                    Proxy
  |                          |
  |-- SSLRequest (8 bytes) ->|   magic: 0x04D2162F
  |<- 'N' (1 byte) ----------|   SSL not supported, fall back
  |                          |
  |-- StartupMessage ------->|   contains user, database, options
  |<- AuthenticationOk ('R')-|   type=0 means cleartext ok
  |<- ReadyForQuery ('Z') ---|   status='I' (idle)
  |                          |
  |  Connection established. |
```

### Simple Query Flow

```
Client sends:   'Q' | int32(length) | sql_string | '\0'
Proxy responds: RowDescription | DataRow(s) | CommandComplete | ReadyForQuery
     or:        ErrorResponse  | ReadyForQuery
```

### SQL Classification

JSQLParser builds an AST from the SQL string. The proxy inspects the root node type:

```
Select   statement  -->  CommandType.SELECT
Insert   statement  -->  CommandType.INSERT
Update   statement  -->  CommandType.UPDATE
Delete   statement  -->  CommandType.DELETE
Other    parseable  -->  CommandType.DDL
Parse    failure    -->  classifyByPrefix() fallback:
  Starts with SELECT/INSERT/UPDATE/DELETE  -->  corresponding type
  Starts with CREATE/DROP/ALTER/TRUNCATE   -->  CommandType.DDL
  Starts with BEGIN/COMMIT/ROLLBACK        -->  CommandType.CONTROL
  Unrecognised                             -->  CommandType.CONTROL (safe default, no cache wipe)
```

---

## Caching Strategy

### Two-Level Architecture

```
Request arrives
      |
      v
  L1 (Caffeine)
  in-process
  ~0.1 microseconds
  TTL: 60 seconds
  Max: 10,000 entries
      |
    HIT --> serve immediately, stop pipeline
      |
    MISS
      |
      v
  L2 (Redis)
  network hop
  ~1 millisecond
  TTL: 300 seconds
      |
    HIT --> warm L1, serve response
      |
    MISS
      |
      v
  PostgreSQL (replica)
  JDBC call on virtual thread
      |
      v
  Store in L1 + L2
```

### Cache Key

The key is a SHA-256 hash of the normalised (lowercased, trimmed) SQL string. This means:

- `SELECT * FROM users` and `select * from users` share the same cache entry
- `SELECT * FROM users WHERE id = 1` and `SELECT * FROM users WHERE id = 2` are separate entries

### Invalidation Policy

- **DDL commands** (ALTER, DROP, CREATE, TRUNCATE): full cache flush on both L1 and L2
- **DML commands** (INSERT, UPDATE, DELETE): currently triggers full flush as well (fine-grained table-level invalidation is a planned improvement using JSQLParser table name extraction)
- **Parse failures / CONTROL statements**: no cache invalidation

---

## Routing Strategy

```
Incoming command
       |
       v
  CommandType == SELECT?
       |
      YES --> NodeRegistry.getReplicaNode()
       |        |
       |        v
       |      iterate replicas
       |        |
       |      CircuitBreaker.allowRequest()?
       |        YES --> use this replica
       |        NO  --> try next replica
       |              all replicas open? --> fall back to PRIMARY
       |
       NO --> NodeRegistry.getPrimaryNode()
```

`ReadWriteRoutingStrategy` is the only implementation. Adding a new strategy (e.g. consistent-hash sharding) requires implementing the `RoutingStrategy` interface and wiring it in `ShadowProxyApplication`.

---

## Circuit Breaker

One `CircuitBreaker` instance exists per backend node, managed by `NodeRegistry`.

### State Machine

```
           failure rate >= 50%
           (window of 20 requests)
  CLOSED -----------------------------> OPEN
    ^                                     |
    |                                     | 30 seconds elapsed
    |                                     v
    |                                 HALF_OPEN
    |                                     |
    | probe succeeds                      | probe fails
    +-------------------------------------+---------> OPEN
```

### Sliding Window

The circuit breaker uses a fixed-size ring buffer of 20 boolean slots (`true` = failure). On each request result:

1. The oldest slot is overwritten with the new result
2. `failureCount` is adjusted (+1 for failure, -1 if overwriting an old failure)
3. Failure rate = `(failureCount / effectiveWindow) * 100`
4. If rate >= 50% and window is full, transition to OPEN

The window must be fully populated before tripping, so 1 failure out of 3 requests does not open the circuit.

### HALF_OPEN Behaviour

When the circuit transitions from OPEN to HALF_OPEN (after 30 seconds), exactly one probe request is allowed through. If it succeeds, the circuit closes. If it fails, it immediately re-opens and the 30-second timer resets.

---

## Session Management

Each client connection generates a session ID from the Netty channel ID. Session state is stored as a Redis hash:

```
HSET session:{channel-id}
    sessionId  <channel-id>
    username   <authenticated user>
    database   <target database>
    state      AUTHENTICATED
    createdAt  <ISO-8601 timestamp>

EXPIRE session:{channel-id} 1800    (30 minutes)
```

On channel disconnect, `PostgresCommandDecoder.channelInactive()` deletes the session key. This makes the proxy fully stateless: any proxy instance can serve any client connection as long as it can reach Redis.

---

## Audit Telemetry

Every proxied command (success or failure) publishes an `AuditEvent` JSON message to RabbitMQ asynchronously on a virtual thread, so the response to the client is never delayed by the publish operation.

### Event Schema

```json
{
  "eventId":   "3d148820-ee9f-41ec-9417-b27addc077e6",
  "sessionId": "08bfb8fffec0fb06-...",
  "type":      "SELECT",
  "sql":       "SELECT * FROM users;",
  "nodeId":    "replica-1",
  "cached":    false,
  "success":   true,
  "latencyMs": 19,
  "errorMsg":  null,
  "timestamp": "2026-03-23T19:39:56Z"
}
```

### RabbitMQ Topology

```
Exchange: shadow.audit  (topic, durable)
    |
    +--> Routing key: proxy.commands
    |
    v
Queue: shadow.audit.log  (durable)
```

The queue can be consumed by any downstream system: log aggregators, alerting pipelines, compliance auditors, or analytics dashboards.

---

## Directory Structure

```
shadow-proxy/
|
+-- src/main/java/com/shadow/proxy/
|   |
|   +-- ShadowProxyApplication.java        Main entry point, manual DI wiring
|   |
|   +-- core/
|   |   +-- ShadowProxyServer.java         Netty ServerBootstrap
|   |   +-- ShadowProxyInitializer.java    ChannelPipeline assembler
|   |
|   +-- protocol/
|   |   +-- CommandType.java               Enum: SELECT INSERT UPDATE DELETE DDL CONTROL
|   |   +-- DatabaseCommand.java           Interface for all proxied commands
|   |   +-- ProtocolCodec.java             Generic encode/decode interface
|   |   +-- ProtocolException.java         Protocol-level error with SQL state
|   |   +-- postgres/
|   |       +-- PostgresCommand.java       Concrete record implementing DatabaseCommand
|   |       +-- PostgresCommandDecoder.java ByteToMessageDecoder (wire protocol parser)
|   |       +-- PostgresResponseEncoder.java MessageToByteEncoder<PostgresCommand>
|   |
|   +-- routing/
|   |   +-- BackendNode.java               Record: node id, host, port, role
|   |   +-- NodeRegistry.java              Manages nodes and circuit breakers
|   |   +-- RoutingStrategy.java           Interface: route(DatabaseCommand) -> BackendNode
|   |   +-- ReadWriteRoutingStrategy.java  SELECT->replica, rest->primary
|   |
|   +-- resilience/
|   |   +-- CircuitBreaker.java            Sliding-window CLOSED/OPEN/HALF_OPEN machine
|   |
|   +-- cache/
|   |   +-- CacheKey.java                  SHA-256 of normalised SQL
|   |   +-- L1CaffeineCache.java           In-process Caffeine cache
|   |   +-- L2RedisCache.java              Async Redis cache via Lettuce
|   |   +-- CacheHandler.java              ChannelDuplexHandler: read-through + invalidation
|   |
|   +-- backend/
|   |   +-- BackendConnectionPool.java     HikariCP wrapper per node
|   |   +-- BackendHandler.java            JDBC execution on virtual threads
|   |   +-- PostgresWireEncoder.java       ResultSet/update/error -> wire bytes
|   |
|   +-- state/
|   |   +-- SessionState.java              Immutable record of client session
|   |   +-- SessionManager.java            Redis HSET/HGETALL/DEL with TTL
|   |
|   +-- telemetry/
|       +-- AuditEvent.java                Immutable audit event record
|       +-- RabbitMQAuditProducer.java     Async AMQP publisher on virtual threads
|
+-- src/main/resources/
|   +-- application.properties            All config with ${ENV_VAR:default} overrides
|   +-- logback.xml                       Console + rolling file logging
|
+-- docker/
|   +-- init-scripts/
|       +-- 01-init.sql                   Schema and seed data for postgres-primary
|
+-- test/
|   +-- test-shadow-proxy.ps1             PowerShell integration test suite
|
+-- docker-compose.yml                    Full infrastructure stack
+-- Dockerfile                            Proxy container (eclipse-temurin:21-jre-alpine)
+-- pom.xml                               Maven build with shade plugin
```

---

## Configuration

All settings live in `src/main/resources/application.properties`. Every value supports environment variable overrides using the `${ENV_VAR:default}` syntax, which is resolved at startup.

```properties
# Proxy
proxy.host=${PROXY_HOST:0.0.0.0}
proxy.port=${PROXY_PORT:5432}

# Backend databases
db.primary.host=${DB_PRIMARY_HOST:localhost}
db.primary.port=${DB_PRIMARY_PORT:5433}
db.replica.host=${DB_REPLICA_HOST:localhost}
db.replica.port=${DB_REPLICA_PORT:5434}
db.name=${DB_NAME:shadowdb}
db.username=${DB_USERNAME:shadowuser}
db.password=${DB_PASSWORD:shadowpass}

# HikariCP
pool.primary.max-size=20
pool.replica.max-size=20
pool.connection-timeout-ms=3000

# Redis
redis.host=${REDIS_HOST:localhost}
redis.port=${REDIS_PORT:6379}
redis.cache.ttl-seconds=300
redis.session.ttl-seconds=1800

# Caffeine L1
cache.l1.max-size=10000
cache.l1.expire-after-write-seconds=60

# RabbitMQ
rabbitmq.host=${RABBITMQ_HOST:localhost}
rabbitmq.port=${RABBITMQ_PORT:5672}
rabbitmq.username=${RABBITMQ_USERNAME:shadowmq}
rabbitmq.password=${RABBITMQ_PASSWORD:shadowmqpass}
rabbitmq.vhost=${RABBITMQ_VHOST:shadow}
rabbitmq.exchange=shadow.audit
rabbitmq.routing-key=proxy.commands

# Circuit breaker
circuit-breaker.failure-rate-threshold-pct=50
circuit-breaker.sliding-window-size=20
circuit-breaker.wait-duration-open-seconds=30
circuit-breaker.permitted-calls-in-half-open=5
```

---

## Running the Project

### Prerequisites

- Java 21 JDK
- Maven 3.8+
- Docker Desktop

### Step 1 - Start the infrastructure

```bash
docker compose up -d postgres-primary postgres-replica redis rabbitmq
```

This starts:
- `shadow-postgres-primary` on port 5433 (accepts writes)
- `shadow-postgres-replica` on port 5434 (streaming replica, accepts reads)
- `shadow-redis` on port 6379
- `shadow-rabbitmq` on ports 5672 (AMQP) and 15672 (management UI)

Wait for all services to be healthy:

```bash
docker compose ps
```

### Step 2 - Build the proxy

```powershell
mvn package -DskipTests
```

This produces `target/shadow-proxy-1.0.0-SNAPSHOT.jar` (fat JAR, ~25 MB).

### Step 3 - Start the proxy

```powershell
# With log capture for test assertions:
java --enable-preview -jar target\shadow-proxy-1.0.0-SNAPSHOT.jar | Tee-Object -FilePath test\proxy.log

# Without log capture:
java --enable-preview -jar target\shadow-proxy-1.0.0-SNAPSHOT.jar
```

The proxy binds on port 5432, the standard PostgreSQL port.

### Step 4 - Connect

```bash
# Using the Docker psql image:
docker run --rm -it registry.hub.docker.com/bitnami/postgresql:latest \
  psql "postgresql://shadowuser:shadowpass@host.docker.internal:5432/shadowdb?sslmode=disable"
```

### Step 5 - Run queries

```sql
-- Routed to replica
SELECT * FROM users;

-- Routed to primary
INSERT INTO users (username, email) VALUES ('newuser', 'newuser@example.com');

-- Verify row appears
SELECT * FROM users;
```

Watch the proxy logs to observe routing decisions, cache hits/misses, and audit events.

### RabbitMQ Management UI

Open `http://localhost:15672` in a browser. Log in with `shadowmq / shadowmqpass`. Navigate to Queues to inspect the `shadow.audit.log` queue and read published audit events.

---

## Testing

The project includes a PowerShell integration test suite at `test/test-shadow-proxy.ps1`.

### Run all tests

```powershell
.\test\test-shadow-proxy.ps1 -LogFile ".\test\proxy.log"
```

### Test groups

| Group | What is tested |
|---|---|
| 1. Read-Write Routing | SELECT goes to replica, INSERT/UPDATE/DELETE go to primary, row counts correct |
| 2. Two-Level Cache | Cold read is a MISS, L1 and L2 are populated, repeat read is an L1 HIT, different SQL has its own key |
| 3. Cache Invalidation | DDL flushes both L1 and L2, subsequent SELECT is a MISS, parse failure does not incorrectly wipe the cache |
| 4. Data Integrity | Inserts are readable, updates propagate, aggregate functions return correct values |
| 5. Audit Trail | RabbitMQ queue exists, audit events are published with type and latency |
| 6. Session Management | Session key created in Redis on connect |
| 7. Circuit Breaker | Circuit opens when replica is stopped, proxy recovers after replica restarts |

### Example output

```
=== PRE-FLIGHT ===
  [PASS] Log file found
  [PASS] Proxy is reachable
         PostgreSQL 16.x on x86_64-pc-linux-gnu

=== 1. READ-WRITE ROUTING ===
  [PASS] 1a -- SELECT routed to replica
  [PASS] 1b -- INSERT routed to primary
  [PASS] 1c -- UPDATE routed to primary
  [PASS] 1d -- DELETE routed to primary
  [PASS] 1e -- Row count sane after write cycle (6 rows)

=== 2. TWO-LEVEL CACHE (L1 Caffeine + L2 Redis) ===
  [PASS] 2a -- First SELECT is a Cache MISS
  [PASS] 2b -- L1 (Caffeine) populated
  [PASS] 2c -- L2 (Redis) populated
  [PASS] 2d -- Redis holds 1 key(s)
  [PASS] 2e -- Second SELECT served from L1
  ...

================================================
 Shadow Proxy -- Test Results
================================================
  PASSED : 22
  FAILED : 0
  WARNED : 0
================================================
```

---

## Design Decisions

### No dependency injection framework

Spring Boot adds several hundred milliseconds to startup and hundreds of megabytes of classpath. The proxy intentionally wires all components manually in `ShadowProxyApplication.main()`. The dependency graph is explicit, readable, and starts in under one second.

### Java 21 Virtual Threads for JDBC

JDBC is inherently blocking. Spawning a platform thread per query would consume significant memory under load. Virtual threads are created per task by `Executors.newVirtualThreadPerTaskExecutor()` and are parked (not blocked) during I/O, allowing the JVM to multiplex thousands of concurrent queries over a small number of OS threads.

### Netty for network I/O

Netty's non-blocking NIO model allows a single event loop thread to manage thousands of concurrent TCP connections. Combined with virtual threads for the blocking JDBC layer, the proxy can handle high connection concurrency without proportionally scaling OS thread counts.

### Immutability everywhere

`PostgresCommand`, `BackendNode`, `SessionState`, and `AuditEvent` are Java `record` types. All list fields use `List.copyOf()`. This eliminates the need for defensive copying and makes the code safe to pass across thread boundaries without synchronisation.

### Wire-level caching

The cache stores raw PostgreSQL wire-protocol bytes, not parsed Java objects. This means:
- Zero serialisation cost on cache hit — the bytes are written directly to the channel
- Zero deserialisation cost — no object allocation on the hot path
- The cache is format-agnostic and would work for any PostgreSQL-compatible response

### Stateless proxy design

No per-connection state is held in the proxy process itself beyond what Netty needs for the open channel. Session state (authentication status, database, username) is stored in Redis with a TTL. This means multiple proxy instances can serve the same pool of client connections, and a proxy restart does not lose session data.

### Circuit breaker per node

Each `BackendNode` has its own `CircuitBreaker` instance, held in a `ConcurrentHashMap` in `NodeRegistry`. Circuit state is checked on every request before acquiring a JDBC connection, preventing connection pool exhaustion when a backend is degraded.
