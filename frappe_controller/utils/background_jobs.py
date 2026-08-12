# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json

import frappe
from frappe.utils import now_datetime


class JobPromise:
	def __init__(self, job_id):
		self.job_id = job_id

	def result(self):
		import frappe

		from frappe_controller.utils.controller import JobResult, wait_for_event

		job = frappe.db.get_value("FS Job", self.job_id, ["status", "name"], as_dict=True)
		if not job:
			raise Exception(f"Job {self.job_id} not found.")

		if job.status == "finished":
			return get_job_result(self.job_id)
		elif job.status == "failed":
			res = get_job_result(self.job_id)
			raise Exception(f"Job {self.job_id} failed: {res.exc_info if res else ''}")

		# Suspend and wait for this specific job to finish
		wait_for_event(f"fs_job_finished:{self.job_id}")
		return get_job_result(self.job_id)

def enqueue(method, queue="low", timeout=None, is_async=True, as_child=True, **kwargs):
	"""
	Replacement for frappe.enqueue.
	Instead of enqueuing directly to Redis, it creates a Controller Job record in MariaDB.
	Returns a JobPromise object.
	"""
	import json

	import frappe

	if queue not in ("low", "medium", "high"):
		import frappe.utils.background_jobs as native_bg
		return native_bg.enqueue(method, queue=queue, timeout=timeout, is_async=is_async, **kwargs)

	parent_job_id = getattr(frappe.flags, "current_job_id", None) if as_child else None
	current_idx = None

	if parent_job_id:
		current_idx = getattr(frappe.flags, "current_job_step", 0)
		frappe.flags.current_job_step = current_idx + 1

		existing_job = frappe.db.get_value("FS Job", {"parent_job": parent_job_id, "idx": current_idx}, "name")
		if existing_job:
			return JobPromise(existing_job)

	# Find or create the Controller Job Type for this method
	job_type_name = frappe.db.exists("Controller Job Type", {"method": method})
	if not job_type_name:
		job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": method,
			"create_log": 1
		}).insert(ignore_permissions=True)
		job_type_name = job_type.name

	# Create the Controller Job (the task instance)
	job = frappe.get_doc({
		"doctype": "FS Job",
		"job_type": job_type_name,
		"job_name": method,
		"queue": queue,
		"status": "queued",
		"total_tried": 0,
		"parent_job": parent_job_id,
		"idx": current_idx,
		"arguments": json.dumps(kwargs, default=str)
	})
	job.insert(ignore_permissions=True)
	job.db_set("job_id", job.name)
	job.job_id = job.name

	job_payload = job.as_dict()
	job_payload["site"] = frappe.local.site

	# Push to Redis Stream for FastStream after the database transaction is committed.
	if frappe.db:
		frappe.db.after_commit.add(
			lambda: frappe.cache().xadd(f"fs:queue:{queue}", {"payload": json.dumps(job_payload, default=str)})
		)

	return JobPromise(job.name)


def get_job_result(job_name: str):
	from frappe_controller.utils.controller import JobResult

	job = frappe.db.get_value(
		"FS Job",
		job_name,
		["status", "result", "exc_info", "time_taken", "started_at", "ended_at"],
		as_dict=True,
	)
	if not job:
		return None

	parsed_result = json.loads(job.result) if job.result else None
	if isinstance(parsed_result, dict) and "status" in parsed_result:
		return JobResult(job_id=job_name, **parsed_result)

	return JobResult(
		job_id=job_name,
		status=job.status,
		result=parsed_result,
		exc_info=job.exc_info,
		time_taken=frappe.utils.flt(job.time_taken),
		started_at=job.started_at,
		ended_at=job.ended_at,
	)

import asyncio
import time
from typing import Any, Dict

import anyio
import redis.asyncio as aioredis
from faststream import FastStream
from faststream.redis import RedisBroker, StreamSub


def create_app(redis_url="redis://localhost:13000"):
    import anyio
    import frappe

    redis_client = aioredis.from_url(redis_url)
    broker = RedisBroker(url=redis_url)
    app = FastStream(broker)

    priority_queue = asyncio.PriorityQueue(maxsize=1000)
    queues = ["high", "medium", "low"]

    async def check_rate_limits(method: str) -> float:
        lua_script = """
        local method = KEYS[1]
        local current_time = tonumber(ARGV[1])
        
        local config_key = "fs:" .. method .. ":config"
        local limits = redis.call('HGETALL', config_key)
        
        if #limits == 0 then
            return 0 -- no limits found
        end
        
        local config = {}
        for i=1, #limits, 2 do
            config[limits[i]] = tonumber(limits[i+1])
        end
        
        local keys = {
            sec = "fs:" .. method .. ":rate:1s",
            min = "fs:" .. method .. ":rate:1m",
            hour = "fs:" .. method .. ":rate:1h",
            day = "fs:" .. method .. ":rate:1d"
        }
        
        local windows = {
            sec = 1,
            min = 60,
            hour = 3600,
            day = 86400
        }
        
        if config['rate_limit_per_second'] and tonumber(redis.call('GET', keys.sec) or 0) >= config['rate_limit_per_second'] then
            return current_time + windows.sec
        end
        if config['rate_limit_per_minute'] and tonumber(redis.call('GET', keys.min) or 0) >= config['rate_limit_per_minute'] then
            return current_time + windows.min
        end
        if config['rate_limit_per_hour'] and tonumber(redis.call('GET', keys.hour) or 0) >= config['rate_limit_per_hour'] then
            return current_time + windows.hour
        end
        if config['rate_limit_per_day'] and tonumber(redis.call('GET', keys.day) or 0) >= config['rate_limit_per_day'] then
            return current_time + windows.day
        end
        
        if config['rate_limit_per_second'] then
            local count = redis.call('INCR', keys.sec)
            if count == 1 then redis.call('EXPIRE', keys.sec, windows.sec) end
        end
        if config['rate_limit_per_minute'] then
            local count = redis.call('INCR', keys.min)
            if count == 1 then redis.call('EXPIRE', keys.min, windows.min) end
        end
        if config['rate_limit_per_hour'] then
            local count = redis.call('INCR', keys.hour)
            if count == 1 then redis.call('EXPIRE', keys.hour, windows.hour) end
        end
        if config['rate_limit_per_day'] then
            local count = redis.call('INCR', keys.day)
            if count == 1 then redis.call('EXPIRE', keys.day, windows.day) end
        end
        
        return 0
        """
        delay_until = await redis_client.eval(lua_script, 1, method, time.time())
        return delay_until

    @app.on_startup
    async def worker_loop():
        import logging
        import traceback
        worker_logger = logging.getLogger("faststream.worker")

        async def process_jobs():
            while True:
                priority, timestamp, job_data = await priority_queue.get()

                if job_data is None:
                    break

                msg = job_data["msg"]
                queue_name = job_data["queue_name"]
                event = job_data["event"]

                try:
                    payload_str = msg.get("payload")
                    if not payload_str:
                        continue

                    if isinstance(payload_str, bytes):
                        payload_str = payload_str.decode()

                    import json
                    try:
                        payload = json.loads(payload_str)
                    except Exception:
                        continue

                    job_id = payload.get("name")
                    if not job_id:
                        continue

                    method_path = payload.get("job_name")
                    args_str = payload.get("arguments")
                    total_tried = int(payload.get("total_tried", 0))

                    site_name = payload.get("site")
                    if not site_name:
                        site_name = frappe.utils.get_sites()[0]

                    # Pickup Lock / Heartbeat Key
                    lock_key = f"fs:started:{site_name}:{job_id}"
                    is_locked = await redis_client.setnx(lock_key, "1")
                    if not is_locked:
                        continue

                    await redis_client.expire(lock_key, 60) # Initial 60s

                    # Clear stale promoted flag to avoid skipping future suspensions
                    await redis_client.delete(f"fs:promoted:{job_id}")

                    args = json.loads(args_str) if args_str else {}

                    delay_until = await check_rate_limits(method_path)

                    if delay_until > 0:
                        DELAYED_JOBS_ZSET = f"fs:scheduled:{queue_name}"
                        await redis_client.zadd(DELAYED_JOBS_ZSET, {json.dumps(msg): delay_until})

                        await redis_client.xadd(f"fs:scheduled:{queue_name}", {
                            "payload": json.dumps({
                                "job_id": job_id,
                                "status": "scheduled",
                                "site": site_name,
                                "total_tried": total_tried
                            }, default=str)
                        })

                        await redis_client.delete(lock_key)
                        continue

                    start_time = time.time()

                    STARTED_STREAM = f"fs:started:{queue_name}"

                    await redis_client.xadd(STARTED_STREAM, {
                        "payload": json.dumps({
                            "job_id": job_id,
                            "status": "started",
                            "site": site_name,
                            "total_tried": total_tried + 1
                        }, default=str)
                    })

                    async def run_frappe():
                        def execute():
                            if getattr(frappe.local, "site", None):
                                frappe.destroy()

                            frappe.init(site=site_name, force=True)
                            frappe.connect()

                            # Set current job ID for wait_for_event
                            frappe.flags.current_job_id = job_id
                            frappe.flags.current_job_step = 0

                            try:
                                func = frappe.get_attr(method_path)
                                result = func(**args)
                                frappe.db.commit()
                                return result
                            except Exception:
                                frappe.db.rollback()
                                raise
                            finally:
                                frappe.destroy()

                        return await anyio.to_thread.run_sync(execute)

                    error = None
                    status = "finished"
                    result = None

                    config_key = f"fs:{method_path}:config"
                    job_config = await redis_client.hgetall(config_key)
                    job_config = {k.decode(): v.decode() for k, v in job_config.items()}

                    max_retries = int(job_config.get("retries") or 0)
                    job_timeout = int(job_config.get("timeout") or 3600)

                    execution_done = anyio.Event()

                    # Worker Heartbeat Task - uses the pickup lock key
                    async def emit_heartbeat():
                        while not execution_done.is_set():
                            try:
                                await redis_client.expire(lock_key, 30) # Maintain 30s TTL
                            except Exception:
                                pass
                            await asyncio.sleep(5)

                    heartbeat_task = asyncio.create_task(emit_heartbeat())

                    try:
                        # Enforce Execution Timeout (SLA)
                        with anyio.fail_after(job_timeout):
                            result = await run_frappe()
                    except (Exception, TimeoutError) as e:
                        from frappe_controller.utils.controller import SuspendJob

                        if isinstance(e, SuspendJob):
                            status = "suspended"
                            worker_logger.info(f"Job {job_id} suspended waiting for {e.event_key}")

                            # Race condition fix: Check if orchestrator already promoted this job
                            # while we were in the process of suspending.
                            is_promoted = await redis_client.get(f"fs:promoted:{job_id}")
                            if is_promoted:
                                worker_logger.info(f"Job {job_id} was already promoted. Re-queuing directly.")
                                await redis_client.delete(f"fs:promoted:{job_id}")

                                # Push back to the end of the queue directly
                                await broker.publish(msg, stream=f"fs:queue:{queue_name}")
                            else:
                                # Move to deferred with infinite score
                                await redis_client.zadd(f"fs:deferred:{queue_name}", {json.dumps(msg): 9999999999})

                            # Send telemetry
                            await redis_client.xadd(f"fs:telemetry:deferred:{queue_name}", {
                                "payload": json.dumps({
                                    "job_id": job_id,
                                    "status": "deferred",
                                    "site": site_name,
                                    "total_tried": total_tried + 1,
                                    "error": f"Suspended: waiting for {e.event_key}"
                                }, default=str)
                            })
                        else:
                            status = "failed"
                            if isinstance(e, TimeoutError):
                                error = f"Job timed out after {job_timeout} seconds"
                            else:
                                error = str(e)

                            worker_logger.error(f"Job {job_id} failed (Attempt {total_tried + 1}): {error}")

                            if total_tried + 1 < max_retries:
                                new_total_tried = total_tried + 1
                                backoff = min(30 * (2 ** new_total_tried), 3600)

                                payload["total_tried"] = new_total_tried
                                msg["payload"] = json.dumps(payload, default=str)

                                await redis_client.zadd(f"fs:deferred:{queue_name}", {json.dumps(msg): time.time() + backoff})

                                await redis_client.xadd(f"fs:telemetry:deferred:{queue_name}", {
                                    "payload": json.dumps({
                                        "job_id": job_id,
                                        "status": "deferred",
                                        "site": site_name,
                                        "total_tried": new_total_tried,
                                        "error": error
                                    }, default=str)
                                })

                                status = "retrying"
                    finally:
                        execution_done.set()
                        heartbeat_task.cancel()

                    time_taken = time.time() - start_time
                    await redis_client.delete(lock_key)

                    if status not in ("retrying", "suspended"):
                        telemetry_stream = f"fs:finished:{queue_name}" if status == "finished" else f"fs:failed:{queue_name}"
                        await redis_client.xadd(telemetry_stream, {
                            "payload": json.dumps({
                                "job_id": job_id,
                                "status": status,
                                "error": error,
                                "result": result,
                                "time_taken": time_taken,
                                "site": site_name,
                                "total_tried": total_tried + 1
                            }, default=str)
                        })

                    if status == "failed":
                        job_data["status"] = "failed"
                        job_data["error"] = error

                except Exception as outer_e:
                    worker_logger.error(f"Worker loop error: {traceback.format_exc()}")
                    job_data["status"] = "failed"
                    job_data["error"] = str(outer_e)
                finally:
                    event.set()

        asyncio.create_task(process_jobs())
        asyncio.create_task(process_jobs())

    @app.on_startup
    async def init_promoter():
        async def promote():
            while True:
                current_time = time.time()
                try:
                    for q in queues:
                        # 1. Handle Rate-Limited Jobs
                        scheduled_zset = f"fs:scheduled:{q}"
                        jobs_sch = await redis_client.zrangebyscore(scheduled_zset, "-inf", current_time)
                        if jobs_sch:
                            for job_str in jobs_sch:
                                job_data = json.loads(job_str)
                                await broker.publish(job_data, stream=f"fs:queue:{q}")
                            await redis_client.zremrangebyscore(scheduled_zset, "-inf", current_time)

                        # 2. Handle Deferred Retries (per-priority)
                        deferred_zset = f"fs:deferred:{q}"
                        jobs_def = await redis_client.zrangebyscore(deferred_zset, "-inf", current_time)
                        if jobs_def:
                            for job_str in jobs_def:
                                job_data = json.loads(job_str)
                                await broker.publish(job_data, stream=f"fs:queue:{q}")
                            await redis_client.zremrangebyscore(deferred_zset, "-inf", current_time)

                except Exception:
                    pass
                await asyncio.sleep(1)

        asyncio.create_task(promote())

    @app.on_shutdown
    async def shutdown_worker():
        await priority_queue.put((-1, time.time(), None))

    async def ingest_high(msg: dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "high", "event": event, "status": "finished", "error": None}
        await priority_queue.put((1, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "failed":
            raise Exception(job_data["error"])

    async def ingest_medium(msg: dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "medium", "event": event, "status": "finished", "error": None}
        await priority_queue.put((2, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "failed":
            raise Exception(job_data["error"])

    async def ingest_low(msg: dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "low", "event": event, "status": "finished", "error": None}
        await priority_queue.put((3, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "failed":
            raise Exception(job_data["error"])

    broker.subscriber(stream=StreamSub("fs:queue:high", group="faststream_workers", consumer="consumer-1"))(ingest_high)
    broker.subscriber(stream=StreamSub("fs:queue:medium", group="faststream_workers", consumer="consumer-1"))(ingest_medium)
    broker.subscriber(stream=StreamSub("fs:queue:low", group="faststream_workers", consumer="consumer-1"))(ingest_low)

    return app, broker, priority_queue

def start_worker(queue="default"):
    import frappe
    if not getattr(frappe.local, "site", None):
        frappe.init(frappe.utils.get_sites()[0])
    redis_url = frappe.conf.get("redis_cache") or "redis://localhost:13000"

    import redis
    sync_redis = redis.Redis.from_url(redis_url)
    queues = ["high", "medium", "low"]
    for q in queues:
        try:
            sync_redis.xgroup_create(f"fs:queue:{q}", "faststream_workers", id="0", mkstream=True)
        except Exception:
            pass
    sync_redis.close()

    app, broker, priority_queue = create_app(redis_url)
    anyio.run(app.run)
