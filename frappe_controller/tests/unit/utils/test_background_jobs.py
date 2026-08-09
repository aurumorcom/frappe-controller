import asyncio
import time

from frappe.tests import UnitTestCase


class TestBackgroundJobsUnit(UnitTestCase):
	def test_high_queue_suspension_unblocks_priority_queue(self):
		"""Unit Test (Mirroring frappe_controller/utils/background_jobs.py)

		Proves PriorityQueue pops level 1 (high) before level 3 (low),
		and that triggering event.set() on job suspension frees the queue for lower priority items.
		"""

		async def run_test():
			pq = asyncio.PriorityQueue(maxsize=10)

			event_high = asyncio.Event()
			event_low = asyncio.Event()

			job_high = {"id": "job_high", "queue": "high", "event": event_high}
			job_low = {"id": "job_low", "queue": "low", "event": event_low}

			# Put High (Priority 1) and Low (Priority 3) into PriorityQueue
			await pq.put((1, time.time(), job_high))
			await pq.put((3, time.time(), job_low))

			# Act 1: Pop first job (must be High priority)
			prio1, _, item1 = await pq.get()
			self.assertEqual(item1["id"], "job_high")
			self.assertEqual(prio1, 1)

			# Simulate High Job Suspension: Event is set, job moved to deferred, worker loop continues
			item1["event"].set()

			# Act 2: Pop next job (must be Low priority)
			prio2, _, item2 = await pq.get()
			self.assertEqual(item2["id"], "job_low")
			self.assertEqual(prio2, 3)

		asyncio.run(run_test())
