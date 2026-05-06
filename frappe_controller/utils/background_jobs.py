# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json
import frappe
from frappe.utils import now_datetime

def enqueue(method, queue="low", timeout=None, is_async=True, **kwargs):
	"""
	Replacement for frappe.enqueue. 
	Instead of enqueuing directly to Redis, it creates a Controller Job record in MariaDB.
	"""
	if queue not in ("low", "medium", "high"):
		import frappe.utils.background_jobs as native_bg
		return native_bg.enqueue(method, queue=queue, timeout=timeout, is_async=is_async, **kwargs)

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
		"status": "Queued",
		"arguments": json.dumps(kwargs, default=str)
	})
	job.insert(ignore_permissions=True)
	job.db_set("job_id", job.name)
	job.job_id = job.name
	
	job_payload = job.as_dict()
	job_payload["site"] = frappe.local.site

	# Push to Redis Stream for FastStream after the database transaction is committed.
	# This prevents race conditions where the worker picks up the job before the DB record is visible.
	if frappe.db:
		frappe.db.after_commit.add(
			lambda: frappe.cache().xadd(f"fs:queue:{queue}", {"payload": json.dumps(job_payload, default=str)})
		)

	return job.name


import asyncio
import time
from typing import Dict, Any

import redis.asyncio as aioredis
from faststream import FastStream
from faststream.redis import RedisBroker, StreamSub
import anyio



def create_app(redis_url="redis://localhost:13000"):
    import frappe
    import redis.asyncio as aioredis
    from faststream import FastStream
    from faststream.redis import RedisBroker, StreamSub
    import anyio
    
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
        
        -- Check all limits first
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
        
        -- If allowed, increment
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
                
                # Check for poison pill during graceful shutdown
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
                    
                    lock_key = f"fs:started:{job_id}"
                    is_locked = await redis_client.setnx(lock_key, "1")
                    if not is_locked:
                        continue

                    await redis_client.expire(lock_key, 3660)
                    
                    args = json.loads(args_str) if args_str else {}
                    
                    delay_until = await check_rate_limits(method_path)
                    
                    if delay_until > 0:
                        # Rate limited: Move to scheduled ZSET
                        DELAYED_JOBS_ZSET = f"fs:scheduled:{queue_name}"
                        await redis_client.zadd(DELAYED_JOBS_ZSET, {json.dumps(msg): delay_until})
                        await redis_client.delete(lock_key)
                        continue
                        
                    start_time = time.time()
                    
                    site_name = payload.get("site")
                    if not site_name:
                        # BEWARE: Accessing frappe.local in async loop might be dangerous
                        site_name = frappe.utils.get_sites()[0]

                    STARTED_STREAM = f"fs:started:{queue_name}"
                    
                    import datetime
                    start_time_str = str(datetime.datetime.now())

                    await redis_client.xadd(STARTED_STREAM, {
                        "payload": json.dumps({
                            "job_id": job_id,
                            "status": "Started",
                            "started_at": start_time_str,
                            "site": site_name
                        }, default=str)
                    })

                    async def run_frappe():
                        def execute():
                            # We must ensure we are in a clean state
                            if getattr(frappe.local, "site", None):
                                frappe.destroy()
                                
                            frappe.init(site=site_name, force=True)
                            frappe.connect()
                            try:
                                func = frappe.get_attr(method_path)
                                func(**args)
                                frappe.db.commit()
                            except Exception:
                                frappe.db.rollback()
                                raise
                            finally:
                                frappe.destroy()
                        
                        await anyio.to_thread.run_sync(execute)
                            
                    error = None
                    status = "Finished"
                    
                    try:
                        await run_frappe()
                    except Exception as e:
                        status = "Failed"
                        error = str(e)
                        worker_logger.error(f"Job {job_id} failed: {traceback.format_exc()}")
                        
                    time_taken = time.time() - start_time
                    
                    telemetry_stream = f"fs:finished:{queue_name}" if status == "Finished" else f"fs:failed:{queue_name}"
                    await redis_client.xadd(telemetry_stream, {
                        "payload": json.dumps({
                            "job_id": job_id,
                            "status": status,
                            "error": error,
                            "time_taken": time_taken,
                            "site": site_name
                        }, default=str)
                    })
                    
                    if status == "Failed":
                        job_data["status"] = "Failed"
                        job_data["error"] = error
                        
                except Exception as outer_e:
                    worker_logger.error(f"Worker loop error: {traceback.format_exc()}")
                    job_data["status"] = "Failed"
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
                        delayed_zset = f"fs:scheduled:{q}"
                        ingestion_stream = f"fs:queue:{q}"
                        jobs = await redis_client.zrangebyscore(delayed_zset, "-inf", current_time)
                        if jobs:
                            for job_str in jobs:
                                job_data = json.loads(job_str)
                                await broker.publish(job_data, stream=ingestion_stream)
                            await redis_client.zremrangebyscore(delayed_zset, "-inf", current_time)
                except Exception:
                    pass
                await asyncio.sleep(1)

        asyncio.create_task(promote())

    @app.on_shutdown
    async def shutdown_worker():
        # Poison pill for graceful shutdown
        await priority_queue.put((-1, time.time(), None))

    async def ingest_high(msg: Dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "high", "event": event, "status": "Success", "error": None}
        await priority_queue.put((1, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "Failed":
            raise Exception(job_data["error"])

    async def ingest_medium(msg: Dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "medium", "event": event, "status": "Success", "error": None}
        await priority_queue.put((2, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "Failed":
            raise Exception(job_data["error"])

    async def ingest_low(msg: Dict[str, Any]):
        event = anyio.Event()
        job_data = {"msg": msg, "queue_name": "low", "event": event, "status": "Success", "error": None}
        await priority_queue.put((3, time.time(), job_data))
        await event.wait()
        if job_data["status"] == "Failed":
            raise Exception(job_data["error"])

    # Dynamically bind the subscribers
    broker.subscriber(stream=StreamSub("fs:queue:high", group="faststream_workers", consumer="consumer-1"))(ingest_high)
    broker.subscriber(stream=StreamSub("fs:queue:medium", group="faststream_workers", consumer="consumer-1"))(ingest_medium)
    broker.subscriber(stream=StreamSub("fs:queue:low", group="faststream_workers", consumer="consumer-1"))(ingest_low)

    return app, broker, priority_queue

def start_worker(queue="default"):
    """
    Programmatic entry point to start the FastStream worker.
    Handles all queues (high, medium, low) using a single unified priority queue.
    """
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

    import anyio
    app, broker, priority_queue = create_app(redis_url)

    # Run the application
    anyio.run(app.run)
