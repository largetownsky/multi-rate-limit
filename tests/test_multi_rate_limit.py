import asyncio
import pytest
import time

from typing import Any, Coroutine, List

from multi_rate_limit.rate_limit import RateLimit, ResourceOverwriteError
from multi_rate_limit.multi_rate_limit import MultiRateLimit, RateLimitStats


@pytest.mark.parametrize(
    "limits, max_async_run",
    [
      ([], 1),
      ([[RateLimit(10, 60)], []], 2),
      ([[RateLimit(10, 60)]], 0),
    ]
)
@pytest.mark.asyncio
async def test_multi_rate_limit_init_error(limits: List[List[RateLimit]], max_async_run: int):
  with pytest.raises(ValueError):
    await MultiRateLimit.create(limits, None, max_async_run)


async def wait_and_return(wait_in_seconds: float, result: Any):
  await asyncio.sleep(wait_in_seconds)
  return result

async def wait_and_error(wait_in_seconds: float, error: Exception):
  await asyncio.sleep(wait_in_seconds)
  raise error

async def check_stats(mrl: MultiRateLimit, limits: List[List[RateLimit]]
    , past_uses: List[List[int]], current_uses: List[int], next_uses: List[int]):
  # Wait for a minimum amount of time until the situation calms down
  await asyncio.sleep(0.01)
  stats = await mrl.stats()
  assert stats.limits == limits
  assert stats.past_uses == past_uses
  assert stats.current_uses == current_uses
  assert stats.next_uses == next_uses

async def cosume_coroutine_to_avoid_warnings(*args: Coroutine[Any, Any, Any]):
  tasks = [asyncio.create_task(coro) for coro in args]
  for task in tasks:
    task.cancel()
  await asyncio.wait(tasks)

@pytest.mark.asyncio
async def test_multi_rate_limit():
  # (relative time, resources)
  # (0.3, [3, 3])
  # (0.6, [1, 2])
  # (1.2, [2, 1])
  # (1.5, [4, 20])
  # (2.7, [5, 50])
  # (3.3, [1, 20])
  # (4.5, [0, 25])
  limits = [[RateLimit(10, 1.5), RateLimit(15, 3)], [RateLimit(100, 3)]]
  mrl = await MultiRateLimit.create(limits, None, 2)
  with pytest.raises(ValueError):
    mrl.reserve([1, 2], None)
  with pytest.raises(ValueError):
    mrl.reserve([1, 2], 0)
  assert mrl.cancel(0) is None
  await check_stats(mrl, limits, [[0, 0], [0]], [0, 0], [0, 0])
  coro1 = wait_and_return(0.6, (None, 'r1'))
  t1 = mrl.reserve([1, 2], coro1)
  assert t1.reserve_number == 0
  assert t1.future.done() == False
  await check_stats(mrl, limits, [[0, 0], [0]], [1, 2], [0, 0])
  coro2 = wait_and_error(0.3, ResourceOverwriteError(time.time() + 0.3, [3, 3], ValueError()))
  t2 = mrl.reserve([2, 3], coro2)
  assert t2.reserve_number == 1
  assert t2.future.done() == False
  await check_stats(mrl, limits, [[0, 0], [0]], [3, 5], [0, 0])
  coro3 = wait_and_return(0.9, ((time.time() + 1.2, [2, 1]), 'r3'))
  t3 = mrl.reserve([3, 4], coro3)
  assert t3.reserve_number == 2
  assert t3.future.done() == False
  await check_stats(mrl, limits, [[0, 0], [0]], [3, 5], [3, 4]) # Get caught up in the max async run
  await asyncio.wait([t1.future, t2.future, t3.future], return_when=asyncio.FIRST_COMPLETED)
  assert t1.future.done() == False
  assert t2.future.done() == True
  with pytest.raises(ValueError):
    await t2.future
  assert t3.future.done() == False
  await check_stats(mrl, limits, [[3, 3], [3]], [4, 6], [0, 0])
  await asyncio.wait([t1.future, t3.future], return_when=asyncio.FIRST_COMPLETED)
  assert t1.future.done() == True
  assert await t1.future == 'r1'
  assert t3.future.done() == False
  await check_stats(mrl, limits, [[4, 4], [5]], [3, 4], [0, 0])
  assert await t3.future == 'r3'
  await check_stats(mrl, limits, [[6, 6], [6]], [0, 0], [0, 0])
  assert mrl._in_process is None
  # Add routines again
  coro1 = wait_and_return(0.3, (None, 'r1'))
  t1 = mrl.reserve([4, 20], coro1)
  assert t1.reserve_number == 3
  assert t1.future.done() == False
  coro2 = wait_and_return(0.3, (None, 'r2'))
  t2 = mrl.reserve([1, 2], coro2)
  assert t2.reserve_number == 4
  assert t2.future.done() == False
  coro3 = wait_and_return(0, (None, 'r3'))
  t3 = mrl.reserve([5, 50], coro3)
  assert t3.reserve_number == 5
  assert t3.future.done() == False
  await check_stats(mrl, limits, [[6, 6], [6]], [4, 20], [6, 52]) # Get caught up in the limits[0][0]
  assert mrl._in_process is not None
  assert mrl.cancel(3) == None
  assert mrl.cancel(4) == ([1, 2], coro2)
  await cosume_coroutine_to_avoid_warnings(coro2)
  assert t2.future.done() == True
  assert t2.future.cancelled() == True
  await check_stats(mrl, limits, [[6, 6], [6]], [4, 20], [5, 50])
  await asyncio.wait([t1.future, t3.future], return_when=asyncio.FIRST_COMPLETED)
  assert t1.future.done() == True
  assert await t1.future == 'r1'
  assert t3.future.done() == False
  await check_stats(mrl, limits, [[10, 10], [26]], [0, 0], [5, 50]) # Get caught up in the limits[0][0]
  coro1 = wait_and_return(0, (None, 'r1'))
  t1 = mrl.reserve([1, 20], coro1)
  assert t1.reserve_number == 6
  assert t1.future.done() == False
  coro2 = wait_and_return(0, (None, 'r2'))
  t2 = mrl.reserve([0, 25], coro2)
  assert t2.reserve_number == 7
  assert t2.future.done() == False
  await asyncio.wait([t1.future, t2.future, t3.future], return_when=asyncio.FIRST_COMPLETED)
  assert t1.future.done() == False
  assert t2.future.done() == False
  assert t3.future.done() == True
  assert await t3.future == 'r3'
  await check_stats(mrl, limits, [[9, 15], [76]], [0, 0], [1, 45]) # Get caught up in the limits[0][1]
  await asyncio.wait([t1.future, t2.future], return_when=asyncio.FIRST_COMPLETED)
  assert t1.future.done() == True
  assert await t1.future == 'r1'
  assert t2.future.done() == False
  await check_stats(mrl, limits, [[6, 13], [93]], [0, 0], [0, 25]) # Get caught up in the limits[1][0]
  assert await t2.future == 'r2'
  await check_stats(mrl, limits, [[1, 6], [95]], [0, 0], [0, 0])

@pytest.mark.asyncio
async def test_multi_rate_limit_auto_close():
  limits = [[RateLimit(10, 1.5), RateLimit(15, 3)], [RateLimit(100, 3)]]
  mrl = await MultiRateLimit.create(limits, None, 2)
  ticket = mrl.reserve([1, 2], wait_and_return(1, (None, None)))
  mrl.cancel(ticket.reserve_number, True)
  await mrl.term()
  mrl = await MultiRateLimit.create(limits, None, 2)
  mrl.reserve([1, 2], wait_and_return(1, (None, None)))
  await mrl.term(True)
