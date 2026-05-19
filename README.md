# Frappe Controller

A high-performance, event-driven orchestrator for the Frappe Framework. 

Frappe Controller replaces or augments the default Frappe background jobs (RQ) with a custom implementation using **FastStream** and **Redis Streams**. It provides advanced job processing capabilities including dynamic rate limiting, job suspension/resumption based on events, strict priority queuing, real-time telemetry, and state-replay workflows.

---

## 🚀 Features

*   **State-Replay Workflows:** Build complex, multi-step workflows that can suspend and resume execution deterministically, skipping already-completed steps.
*   **Event-Driven Suspension & Resumption:** Jobs can pause execution and wait for specific system events (e.g., a webhook callback, a document update) without blocking worker threads.
*   **Dynamic Rate Limiting:** Enforce strict execution limits (per second, minute, hour, day) on specific job types using atomic Redis Lua scripts.
*   **Strict Priority Queuing:** High-priority jobs instantly jump ahead of low-priority jobs in the worker's local queue.
*   **Crash Resilience & Orphan Recovery:** Continuous heartbeat monitoring via Redis keyspace events ensures that if a worker crashes, its jobs are immediately re-queued.
*   **Real-time Telemetry:** Job statuses (`queued`, `started`, `finished`, `failed`) are updated in MariaDB in near real-time via Redis streams.
*   **Seamless Integration:** Overrides `frappe.enqueue` for specific queues (`low`, `medium`, `high`) while falling back to native RQ for others.

---

## 🏗️ Architecture

The system consists of three main pillars:

1.  **The Database (MariaDB):** The source of truth for job definitions (`Controller Job Type`), job instances (`FS Job`), execution logs (`Controller Job Log`), and event matching conditions (`FS Match Condition`).
2.  **The Worker (`bench worker --namespace fs`):** A FastStream application that consumes jobs from Redis streams, enforces rate limits, executes the Python methods in isolated threads, and handles retries/timeouts.
3.  **The Orchestrator (`bench control`):** A continuous background process that monitors telemetry streams, handles job suspension/resumption based on events, and reconciles orphaned jobs.

---

## 📦 Installation

1. Get the app:
   ```bash
   bench get-app frappe_controller https://github.com/aurumor/frappe_controller.git
   ```
2. Install on your site:
   ```bash
   bench --site yoursite.local install-app frappe_controller
   ```
3. Start the Orchestrator (in a separate terminal or via supervisor/systemd):
   ```bash
   bench control
   ```
4. Start the FastStream Worker:
   ```bash
   bench worker --namespace fs
   ```

---

## 💻 Usage

### 1. Registering a Job Type

Before you can enqueue a job, the system needs to know about it. There are two ways to register a job type:

#### Method A: Automatic Registration via `hooks.py` (Recommended)

The best way to register jobs is by defining them in your app's `hooks.py` file. This ensures your job configurations are version-controlled and automatically synced across environments.

Add a `controller_events` dictionary to your `hooks.py`:

```python
# your_app/hooks.py

controller_events = {
    # Simple registration with default settings
    "your_app.tasks.simple_task": {},

    # Registration with specific rate limits and retries
    "your_app.tasks.process_payment": {
        "rate_limit_per_minute": 60,
        "rate_limit_per_hour": 1000,
        "retries": 3,
        "timeout": 300
    },
    
    # You can also group them by category if you prefer
    "Data Sync": [
        "your_app.sync.sync_customers",
        {
            "method": "your_app.sync.sync_orders",
            "rate_limit_per_day": 5000
        }
    ]
}
```

After updating `hooks.py`, run the following command to sync the configurations to the database and Redis:

```bash
bench migrate
```
*Note: `bench migrate` automatically calls the `sync_jobs` function which parses these hooks and creates/updates the `Controller Job Type` records.*

#### Method B: Manual Registration via the Frappe UI

1. Log in to your Frappe Desk.
2. Search for **Controller Job Type** and create a new document.
3. Set the **Method** to the dotted path of your Python function (e.g., `your_app.tasks.send_email`).
4. Configure the desired rate limits, retries, and timeout.
5. Save the document. The configuration is instantly synced to Redis.

---

### 2. Enqueuing Jobs

Once registered, use the custom `enqueue` method provided by the app. It acts as a drop-in replacement for `frappe.enqueue` for `low`, `medium`, and `high` queues.

```python
from frappe_controller.utils.background_jobs import enqueue

# Enqueue a job to the high priority queue
job_id = enqueue(
    method="your_app.tasks.process_payment",
    queue="high",
    payment_id="PAY-001",
    amount=150.00
)

print(f"Job queued with ID: {job_id}")
```

**Important Note on Transactions:** The job is only pushed to the Redis stream *after* the current database transaction is committed. If you call `enqueue()` but the transaction rolls back, the job will not be executed.

---

### 3. State-Replay Workflows

Frappe Controller supports complex, multi-step workflows that can suspend and resume execution deterministically.

```python
from frappe_controller.utils.background_jobs import enqueue

def process_order_workflow(order_id):
    # 1. Sequential Step
    # The workflow will suspend here until extract_data finishes.
    # When it resumes, it will skip this step and return the cached result.
    data = enqueue("your_app.tasks.extract_data", order_id=order_id).result()
    
    # 2. Parallel Steps
    # Both tasks are enqueued simultaneously. The workflow suspends until BOTH finish.
    p1 = enqueue("your_app.tasks.process_payment", amount=data["total"])
    p2 = enqueue("your_app.tasks.send_email", to=data["email"])
    results = [p1.result(), p2.result()]
    
    # 3. Final Step
    enqueue("your_app.tasks.finalize_order", order_id=order_id).result()
    
    return "Order Processed Successfully"
```

**How it works:**
* When a child job is enqueued, the workflow suspends.
* When the child job finishes, the orchestrator creates a `Controller Job Log` containing the result and emits a wakeup event.
* The workflow resumes from line 1. It skips already-completed steps by checking if the child `FS Job` is finished and reading its result from the linked `Controller Job Log`.
* **CRITICAL:** Workflows must be deterministic. The `idx` of each step is generated based on the execution order.

---

### 4. Suspending a Job (Waiting for an Event)

A powerful feature of Frappe Controller is the ability to suspend a running job until a specific event occurs. This frees up the worker thread immediately, allowing it to process other jobs.

```python
# your_app/tasks.py
from frappe_controller.utils.controller import wait_for_event

def process_payment(payment_id, amount):
    # 1. Do some initial work (e.g., call an external payment gateway)
    initiate_external_payment(payment_id, amount)
    
    # 2. Suspend the job until the gateway sends a webhook callback
    # The condition ensures we only resume when the callback matches our payment_id
    wait_for_event(
        event_key="payment_gateway_callback",
        condition=f"argument.get('payment_id') == '{payment_id}'"
    )
    
    # 3. This code will ONLY execute after the event is emitted and the job is resumed.
    # CRITICAL: When resumed, the worker executes this function from the BEGINNING (line 1).
    # Therefore, your function MUST be idempotent.
    
    # Example of making it idempotent:
    status = check_payment_status(payment_id)
    if status == "Pending":
        initiate_external_payment(payment_id, amount)
        wait_for_event(...)
    elif status == "Completed":
        finalize_order(payment_id)
```

### 5. Emitting Events

Events can be emitted manually to resume suspended jobs. For example, inside a webhook controller:

```python
# your_app/api.py
import frappe
from frappe_controller.utils.controller import emit_event

@frappe.whitelist(allow_guest=True)
def payment_webhook():
    data = frappe.request.json
    payment_id = data.get("payment_id")
    status = data.get("status")
    
    # Emit the event, passing relevant data in the argument dictionary
    emit_event(
        key="payment_gateway_callback",
        argument={"payment_id": payment_id, "status": status}
    )
    
    return "OK"
```

*Note: Standard Frappe DocType events (`after_insert`, `on_update`, `on_trash`) are automatically broadcasted as events (e.g., `doc:Sales Invoice:on_update`). You can wait for these natively without manually emitting them.*

---

## 🤖 For AI Agents

If you are an AI agent working with this codebase, please read [AGENTS.md](./AGENTS.md) for deep architectural insights, Redis key structures, and strict coding directives.
