# Frappe Controller - AI Agent Guide

This document provides deep architectural context, data flow mechanics, and strict coding directives for AI agents interacting with or modifying the `frappe_controller` app.

## 🏗️ System Architecture & Data Flow

The `frappe_controller` replaces Frappe's default RQ worker system for specific queues (`low`, `medium`, `high`) using a hybrid architecture of MariaDB (for durable state/UI) and Redis Streams/Sorted Sets (for high-throughput messaging and scheduling).

### 1. Job Lifecycle
1. **Enqueue:** `frappe_controller.utils.background_jobs.enqueue` creates an `FS Job` record in MariaDB.
2. **Commit Hook:** Upon DB commit, the payload is pushed to the Redis stream `fs:queue:{queue}`.
3. **Ingestion:** The FastStream worker (`bench worker --namespace fs`) consumes the stream and places the job in a local `asyncio.PriorityQueue`.
4. **Execution:** The worker loop pops the job, acquires a Redis lock (`fs:started:{site}:{job_id}`), checks rate limits via a Lua script, and executes the Python method in a separate thread (`anyio.to_thread.run_sync`).
5. **Telemetry:** Status changes (`started`, `finished`, `failed`) are pushed to telemetry streams (`fs:started:*`, `fs:finished:*`, etc.).
6. **Orchestration:** The `bench control` process consumes telemetry streams and updates the `FS Job` records in MariaDB.

### 2. Suspension & Resumption (The `wait_for_event` pattern)
* When a job calls `wait_for_event(event_key)`, it raises a `SuspendJob` exception.
* The worker catches this, moves the job to the deferred sorted set (`fs:deferred:{queue}`) with an infinite score, and frees the thread.
* An `FS Match Condition` record is created in MariaDB.
* When `emit_event(event_key)` is called, it pushes to the `fs:events` stream.
* The Orchestrator (`bench control`) reads `fs:events`, evaluates the `FS Match Condition`, and if matched, removes the job from `fs:deferred:{queue}` and pushes it back to `fs:queue:{queue}`.
* **CRITICAL:** Resumed jobs are re-executed from the *beginning* of the function. They do not resume from the exact line of code.

### 3. State-Replay Workflows
* Workflows are standard Python functions that use `enqueue(method, **kwargs).result()` to execute child jobs.
* When a child job is enqueued and `.result()` is called, the workflow suspends (using `wait_for_event`).
* When the child job finishes, the orchestrator creates a `Controller Job Log` containing the result in `debug_log` and emits a wakeup event for the parent workflow.
* The workflow resumes from line 1. It skips already-completed steps by checking if the child `FS Job` is finished and reading its result from the linked `Controller Job Log`.
* **CRITICAL:** Workflows must be deterministic. The `idx` of each step is generated based on the execution order (`frappe.flags.current_job_step`).

## 🗄️ Redis Key Structure

* **Streams:**
  * `fs:queue:{queue}`: Active jobs waiting to be picked up by workers.
  * `fs:events`: System events emitted for job resumption.
  * `fs:started:{queue}`, `fs:finished:{queue}`, `fs:failed:{queue}`: Telemetry streams consumed by the orchestrator.
* **Sorted Sets (ZSETs):**
  * `fs:scheduled:{queue}`: Jobs delayed due to rate limits. Score = Unix timestamp to run.
  * `fs:deferred:{queue}`: Jobs delayed due to retries (Score = Unix timestamp) or suspension (Score = 9999999999).
* **Hashes:**
  * `fs:{method_path}:config`: Stores rate limits, retries, and timeouts for a specific job type.
* **Strings (Locks/Heartbeats):**
  * `fs:started:{site}:{job_id}`: Worker heartbeat lock. TTL is maintained while the job runs. If it expires, the orchestrator assumes the worker crashed and re-queues the job.

## 📜 Database Schema (MariaDB)

* **`Controller Job Type`**: Configuration for a specific Python method (rate limits, retries). Synced to Redis Hashes.
* **`FS Job`**: The durable record of a job instance. Can be linked to a `parent_job` for workflows.
* **`FS Event`**: A durable record of an emitted event.
* **`FS Match Condition`**: Links a suspended `FS Job` to an `event_key` and an optional evaluation `condition`.
* **`Controller Job Log`**: Execution logs for finished/failed jobs. Stores the execution context and results in `debug_log`.

## 🛑 Strict Directives & Anti-Patterns

### 1. Idempotency is Mandatory
* **Directive:** Any method that utilizes `wait_for_event` or workflow helpers MUST be idempotent.
* **Reason:** When an event resumes a job, the worker executes the function from line 1. The function must check external state (e.g., "Did I already charge the credit card?") before proceeding to the `wait_for_event` call.

### 2. No Blocking I/O in Async Loops
* **Directive:** Never place synchronous blocking calls (e.g., `time.sleep()`, `requests.get()`, Frappe ORM calls) directly inside `async def` functions in the worker.
* **Reason:** This starves the `asyncio` event loop.
* **Solution:** Frappe method execution is already wrapped in `anyio.to_thread.run_sync`. Keep Frappe logic synchronous, and keep worker orchestration asynchronous.

### 3. Database Transaction Boundaries
* **Directive:** Do not manually call `frappe.db.commit()` inside standard job logic unless absolutely necessary for external visibility before the job finishes.
* **Reason:** The worker automatically calls `frappe.db.commit()` upon successful execution and `frappe.db.rollback()` upon exception.
* **Directive:** `enqueue()` relies on `frappe.db.after_commit.add()`. If you enqueue a job but rollback the transaction, the job will *not* be pushed to Redis.

### 4. Telemetry vs. Direct DB Writes
* **Directive:** Workers MUST NOT write status updates directly to the `FS Job` table.
* **Reason:** High-concurrency DB writes cause deadlocks. Workers must push to Redis telemetry streams (`fs:started:*`, etc.), and the single-threaded Orchestrator (`bench control`) handles the DB updates.

### 5. Testing
* **Directive:** Use `IsolatedAsyncioTestCase` for testing FastStream worker internals.
* **Directive:** When testing `enqueue`, mock the Redis `xadd` call to verify the payload is constructed correctly without requiring a live Redis stream consumer.
* **Directive:** When testing `wait_for_event`, expect the `SuspendJob` exception to be raised.

# Code Context Engine (Probe)

Probe is configured for this workspace. Use Probe MCP tools to inspect and search code dynamically across target folder paths instead of raw static AST dumps:
- `probe search "<query>" [path]` - Search code semantically with Elasticsearch-style syntax.
- `probe extract <file>:<line>` - Extract complete AST semantic blocks.
- `probe query "<pattern>"` - Perform AST structural pattern matching.
- `probe symbols <file>` - List code symbols (functions, classes, constants) in target file.
