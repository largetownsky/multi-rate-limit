import asyncio
import pytest

from typing import Any

from multi_rate_limit.rate_limit import ResourceOverwriteError
from multi_rate_limit.resource_queue import CurrentResourceBuffer, NextResourceQueue


async def wait_and_return(wait_in_seconds: float, result: Any):
  await asyncio.sleep(wait_in_seconds)
  return result

async def wait_and_error(wait_in_seconds: float, error: Exception):
  await asyncio.sleep(wait_in_seconds)
  raise error

@pytest.mark.asyncio
async def test_current():
  loop = asyncio.get_running_loop()
  # Empty buffer
  buf = CurrentResourceBuffer(2, 2)
  assert buf.is_empty() == True
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, None]
  assert buf.task_buffer == [None, None]
  assert buf.future_buffer == [None, None]
  assert buf.next == 0
  assert buf.active_run == 0
  assert buf.sum_resources == [0, 0]
  # Start a coroutine
  f1 = loop.create_future()
  coro1 = wait_and_return(0.1, (None, 'r1'))
  assert buf.start_coroutine([1, 2], coro1, f1) == True
  assert buf.is_empty() == False
  assert buf.is_full() == False
  assert buf.resource_buffer == [[1, 2], None]
  assert buf.task_buffer[0].get_name() == '0'
  assert buf.task_buffer[1] is None
  assert buf.future_buffer[0].done() == False
  assert buf.future_buffer[1] is None
  assert buf.next == 1
  assert buf.active_run == 1
  assert buf.sum_resources == [1, 2]
  # End a coroutine
  await buf.task_buffer[0]
  assert buf.end_coroutine(100, buf.task_buffer[0]) == (100, [1, 2])
  assert await f1 == 'r1'
  assert buf.is_empty() == True
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, None]
  assert buf.task_buffer == [None, None]
  assert buf.future_buffer == [None, None]
  assert buf.next == 1
  assert buf.active_run == 0
  assert buf.sum_resources == [0, 0]
  # Start many coroutines
  f1 = loop.create_future()
  coro1 = wait_and_return(0.1, ((90 , [1, 1]), 'r1'))
  assert buf.start_coroutine([1, 2], coro1, f1) == True
  f2 = loop.create_future()
  coro2 = wait_and_error(0.2, ResourceOverwriteError(110, [3, 3], ValueError()))
  assert buf.start_coroutine([2, 3], coro2, f2) == True
  f3 = loop.create_future()
  coro3 = wait_and_return(0.3, (None, 'r3'))
  assert buf.start_coroutine([3, 4], coro3, f3) == False
  assert buf.is_empty() == False
  assert buf.is_full() == True
  assert buf.resource_buffer == [[2, 3], [1, 2]]
  assert buf.task_buffer[0].get_name() == '0'
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0].done() == False
  assert buf.future_buffer[1].done() == False
  assert buf.next == 1
  assert buf.active_run == 2
  assert buf.sum_resources == [3, 5]
  # End many coroutines
  await asyncio.wait([*buf.task_buffer, asyncio.create_task(coro3)])
  assert buf.end_coroutine(100, buf.task_buffer[1]) == (90, [1, 1])
  assert buf.end_coroutine(100, buf.task_buffer[0]) == (110, [3, 3])
  assert await f1 == 'r1'
  with pytest.raises(ValueError):
    await f2
  assert buf.is_empty() == True
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, None]
  assert buf.task_buffer == [None, None]
  assert buf.future_buffer == [None, None]
  assert buf.next == 1
  assert buf.active_run == 0
  assert buf.sum_resources == [0, 0]
  # First In Last Out
  f1 = loop.create_future()
  coro1 = wait_and_error(0.3, ValueError())
  assert buf.start_coroutine([1, 2], coro1, f1) == True
  assert buf.is_empty() == False
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, [1, 2]]
  assert buf.task_buffer[0] is None
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0] is None
  assert buf.future_buffer[1].done() == False
  assert buf.next == 0
  assert buf.active_run == 1
  assert buf.sum_resources == [1, 2]
  f2 = loop.create_future()
  coro2 = wait_and_return(0.1, ((110, [3]), 'r2')) # Invalid resource length
  assert buf.start_coroutine([2, 3], coro2, f2) == True
  assert buf.is_empty() == False
  assert buf.is_full() == True
  assert buf.resource_buffer == [[2, 3], [1, 2]]
  assert buf.task_buffer[0].get_name() == '0'
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0].done() == False
  assert buf.future_buffer[1].done() == False
  assert buf.next == 1
  assert buf.active_run == 2
  assert buf.sum_resources == [3, 5]
  await asyncio.wait([buf.task_buffer[0]])
  assert buf.end_coroutine(100, buf.task_buffer[0]) == (100, [2, 3])
  with pytest.raises(ValueError):
    await f2
  assert buf.is_empty() == False
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, [1, 2]]
  assert buf.task_buffer[0] is None
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0] is None
  assert buf.future_buffer[1].done() == False
  assert buf.next == 1
  assert buf.active_run == 1
  assert buf.sum_resources == [1, 2]
  f2 = loop.create_future()
  coro2 = wait_and_return(0.1, ((110, [3, -1]), 'r2')) # Negative resource value
  assert buf.start_coroutine([2, 3], coro2, f2) == True
  assert buf.is_empty() == False
  assert buf.is_full() == True
  assert buf.resource_buffer == [[2, 3], [1, 2]]
  assert buf.task_buffer[0].get_name() == '0'
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0].done() == False
  assert buf.future_buffer[1].done() == False
  assert buf.next == 1
  assert buf.active_run == 2
  assert buf.sum_resources == [3, 5]
  await asyncio.wait([buf.task_buffer[0]])
  assert buf.end_coroutine(100, buf.task_buffer[0]) == (100, [2, 3])
  with pytest.raises(ValueError):
    await f2
  assert buf.is_empty() == False
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, [1, 2]]
  assert buf.task_buffer[0] is None
  assert buf.task_buffer[1].get_name() == '1'
  assert buf.future_buffer[0] is None
  assert buf.future_buffer[1].done() == False
  assert buf.next == 1
  assert buf.active_run == 1
  assert buf.sum_resources == [1, 2]
  await asyncio.wait([buf.task_buffer[1]])
  assert buf.end_coroutine(100, buf.task_buffer[1]) == (100, [1, 2])
  with pytest.raises(ValueError):
    await f1
  assert buf.is_empty() == True
  assert buf.is_full() == False
  assert buf.resource_buffer == [None, None]
  assert buf.task_buffer == [None, None]
  assert buf.future_buffer == [None, None]
  assert buf.next == 1
  assert buf.active_run == 0
  assert buf.sum_resources == [0, 0]


@pytest.mark.asyncio
async def test_next():
  dummy = wait_and_error(0.1, ValueError())
  f = asyncio.get_running_loop().create_future()
  # Empty queue
  queue = NextResourceQueue(2)
  assert queue.is_empty() == True
  assert len(queue.number_to_resource_coro_future) == 0
  assert queue.next_add == 0
  assert queue.next_run == 0
  assert queue.sum_resources == [0, 0]
  assert queue.peek() is None
  assert queue.pop() is None
  assert queue.cancel(-1) is None
  assert queue.cancel(0) is None
  assert queue.cancel(1) is None
  # Push and cancel
  assert queue.push([1, 2], dummy, f) == 0
  assert queue.is_empty() == False
  assert len(queue.number_to_resource_coro_future) == 1
  assert queue.next_add == 1
  assert queue.next_run == 0
  assert queue.sum_resources == [1, 2]
  assert queue.cancel(-1) is None
  assert queue.cancel(1) is None
  assert queue.peek() == ([1, 2], dummy, f)
  assert queue.is_empty() == False
  assert len(queue.number_to_resource_coro_future) == 1
  assert queue.next_add == 1
  assert queue.next_run == 0
  assert queue.sum_resources == [1, 2]
  assert queue.cancel(0) == ([1, 2], dummy, f, True)
  assert queue.is_empty() == True
  assert len(queue.number_to_resource_coro_future) == 0
  assert queue.next_add == 1
  assert queue.next_run == 0
  assert queue.sum_resources == [0, 0]
  # Push and pop
  assert queue.push([1, 2], dummy, f) == 1
  assert queue.is_empty() == False
  assert len(queue.number_to_resource_coro_future) == 1
  assert queue.next_add == 2
  assert queue.next_run == 0
  assert queue.sum_resources == [1, 2]
  assert queue.cancel(0) is None
  assert queue.cancel(2) is None
  assert queue.peek() == ([1, 2], dummy, f)
  assert queue.is_empty() == False
  assert len(queue.number_to_resource_coro_future) == 1
  assert queue.next_add == 2
  assert queue.next_run == 1
  assert queue.sum_resources == [1, 2]
  assert queue.pop() == ([1, 2], dummy, f)
  assert queue.is_empty() == True
  assert len(queue.number_to_resource_coro_future) == 0
  assert queue.next_add == 2
  assert queue.next_run == 2
  assert queue.sum_resources == [0, 0]
  # Combine various operations
  assert queue.peek() is None
  assert queue.pop() is None
  assert queue.push([1, 2], dummy, f) == 2
  assert queue.push([2, 3], dummy, f) == 3
  assert queue.cancel(3) == ([2, 3], dummy, f, False)
  assert queue.push([3, 4], dummy, f) == 4
  assert queue.pop() == ([1, 2], dummy, f)
  assert queue.push([4, 5], dummy, f) == 5
  assert queue.peek() == ([3, 4], dummy, f)
  assert queue.cancel(4) == ([3, 4], dummy, f, True)
  assert queue.is_empty() == False
  assert len(queue.number_to_resource_coro_future) == 1
  assert queue.next_add == 6
  assert queue.next_run == 4
  assert queue.sum_resources == [4, 5]
  assert queue.pop() == ([4, 5], dummy, f)
  # Avoiding a warning for an unfinished coroutine
  await asyncio.wait([asyncio.create_task(dummy)])
