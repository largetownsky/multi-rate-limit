import os
import pytest
import shutil

from aiofiles.os import wrap
from os.path import isfile

from multi_rate_limit.rate_limit import RateLimit, SecondRateLimit, MinuteRateLimit, HourRateLimit, DayRateLimit
from multi_rate_limit.rate_limit import FilePastResourceQueue, IPastResourceQueue, ResourceOverwriteError

@pytest.mark.parametrize(
    "limit, period",
    [
      (2, 0.1),
      (3, 4)
    ]
)
def test_rate_limit(limit: int, period: float):
  rl = RateLimit(limit, period)
  assert rl.period_in_seconds == period
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit, period",
    [
      (0, 2),
      (3, -1)
    ]
)
def test_rate_limit_error(limit: int, period: float):
  with pytest.raises(ValueError):
    RateLimit(limit, period)

@pytest.mark.parametrize(
    "limit, period",
    [
      (1, 0.5),
      (4, 3)
    ]
)
def test_second_rate_limit(limit: int, period: float):
  rl = SecondRateLimit(limit, period)
  assert rl.period_in_seconds == period
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit, period",
    [
      (1, 0.5),
      (4, 3)
    ]
)
def test_minute_rate_limit(limit: int, period: float):
  rl = MinuteRateLimit(limit, period)
  assert rl.period_in_seconds == 60 * period
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit, period",
    [
      (1, 0.5),
      (4, 3)
    ]
)
def test_hour_rate_limit(limit: int, period: float):
  rl = HourRateLimit(limit, period)
  assert rl.period_in_seconds == 3600 * period
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit, period",
    [
      (1, 0.5),
      (4, 3)
    ]
)
def test_day_rate_limit(limit: int, period: float):
  rl = DayRateLimit(limit, period)
  assert rl.period_in_seconds == 86400 * period
  assert rl.resource_limit == limit


def test_resource_overwrite_error():
  use_time = 100
  use_resources = [1, 2]
  cause = ValueError()
  error = ResourceOverwriteError(use_time, use_resources, cause)
  assert error.use_time == use_time
  assert error.use_resources == use_resources
  assert error.cause == cause
  text = str(error)
  assert text.find(str(use_time)) >= 0
  assert text.find(str(use_resources)) >= 0
  assert text.find(str(cause)) >= 0


@pytest.mark.asyncio
async def test_past():
  # Empty queue
  queue = FilePastResourceQueue(2, 60)
  assert len(queue._time_resource_queue) == 1
  assert queue.pos_time_after(-0.01) == 0
  assert queue.pos_time_after(0) == 1
  assert await queue.sum_resource_after(-0.01, 0) == 0
  assert await queue.sum_resource_after(0, 1) == 0
  assert queue.pos_accum_resouce_within(0, 0) == 0
  assert queue.pos_accum_resouce_within(1, 1) == 0
  assert await queue.time_accum_resource_within(0, 0) == 0
  assert await queue.time_accum_resource_within(1, 1) == 0
  # Single data queue
  await queue.add(100, [1, 2])
  assert len(queue._time_resource_queue) == 2
  assert queue.pos_time_after(-0.01) == 0
  assert queue.pos_time_after(0) == 1
  assert queue.pos_time_after(99) == 1
  assert queue.pos_time_after(100) == 2
  assert await queue.sum_resource_after(-0.01, 0) == 1
  assert await queue.sum_resource_after(0, 0) == 1
  assert await queue.sum_resource_after(99, 1) == 2
  assert await queue.sum_resource_after(100, 1) == 0
  assert queue.pos_accum_resouce_within(0, 0) == 1
  assert queue.pos_accum_resouce_within(0, 1) == 0
  assert queue.pos_accum_resouce_within(1, 1) == 1
  assert queue.pos_accum_resouce_within(1, 2) == 0
  assert await queue.time_accum_resource_within(0, 0) == 100
  assert await queue.time_accum_resource_within(0, 1) == 0
  assert await queue.time_accum_resource_within(1, 1) == 100
  assert await queue.time_accum_resource_within(1, 2) == 0
  # Single data queue
  await queue.add(200, [1, 10])
  assert len(queue._time_resource_queue) == 2
  assert queue.pos_time_after(99) == 0
  assert queue.pos_time_after(100) == 1
  assert queue.pos_time_after(199) == 1
  assert queue.pos_time_after(200) == 2
  assert await queue.sum_resource_after(99, 0) == 1
  assert await queue.sum_resource_after(100, 0) == 1
  assert await queue.sum_resource_after(199, 1) == 10
  assert await queue.sum_resource_after(200, 1) == 0
  assert queue.pos_accum_resouce_within(0, 0) == 1
  assert queue.pos_accum_resouce_within(0, 1) == 0
  assert queue.pos_accum_resouce_within(1, 9) == 1
  assert queue.pos_accum_resouce_within(1, 10) == 0
  assert await queue.time_accum_resource_within(0, 0) == 200
  assert await queue.time_accum_resource_within(0, 1) == 100
  assert await queue.time_accum_resource_within(1, 9) == 200
  assert await queue.time_accum_resource_within(1, 10) == 100
  # Single data queue with a added value
  # Total: (200, [3, 10])
  await queue.add(199, [2, 0])
  assert len(queue._time_resource_queue) == 2
  assert queue.pos_time_after(99) == 0
  assert queue.pos_time_after(100) == 1
  assert queue.pos_time_after(199) == 1
  assert queue.pos_time_after(200) == 2
  assert await queue.sum_resource_after(99, 0) == 3
  assert await queue.sum_resource_after(100, 0) == 3
  assert await queue.sum_resource_after(199, 1) == 10
  assert await queue.sum_resource_after(200, 1) == 0
  assert queue.pos_accum_resouce_within(0, 2) == 1
  assert queue.pos_accum_resouce_within(0, 3) == 0
  assert queue.pos_accum_resouce_within(1, 9) == 1
  assert queue.pos_accum_resouce_within(1, 10) == 0
  assert await queue.time_accum_resource_within(0, 2) == 200
  assert await queue.time_accum_resource_within(0, 3) == 100
  assert await queue.time_accum_resource_within(1, 9) == 200
  assert await queue.time_accum_resource_within(1, 10) == 100
  # Many data queue
  # Inherited: (200, [3, 10])
  await queue.add(210, [1, 1])
  await queue.add(220, [2, 3])
  assert len(queue._time_resource_queue) == 4
  assert queue.pos_time_after(99) == 0
  assert queue.pos_time_after(100) == 1
  assert queue.pos_time_after(199) == 1
  assert queue.pos_time_after(200) == 2
  assert queue.pos_time_after(209) == 2
  assert queue.pos_time_after(210) == 3
  assert queue.pos_time_after(219) == 3
  assert queue.pos_time_after(220) == 4
  assert await queue.sum_resource_after(99, 0) == 6
  assert await queue.sum_resource_after(100, 0) == 6
  assert await queue.sum_resource_after(199, 1) == 14
  assert await queue.sum_resource_after(200, 1) == 4
  assert await queue.sum_resource_after(209, 0) == 3
  assert await queue.sum_resource_after(210, 0) == 2
  assert await queue.sum_resource_after(219, 1) == 3
  assert await queue.sum_resource_after(220, 1) == 0
  assert queue.pos_accum_resouce_within(0, 1) == 3
  assert queue.pos_accum_resouce_within(0, 2) == 2
  assert queue.pos_accum_resouce_within(0, 3) == 1
  assert queue.pos_accum_resouce_within(0, 5) == 1
  assert queue.pos_accum_resouce_within(0, 6) == 0
  assert queue.pos_accum_resouce_within(1, 2) == 3
  assert queue.pos_accum_resouce_within(1, 3) == 2
  assert queue.pos_accum_resouce_within(1, 4) == 1
  assert queue.pos_accum_resouce_within(1, 13) == 1
  assert queue.pos_accum_resouce_within(1, 14) == 0
  assert await queue.time_accum_resource_within(0, 1) == 220
  assert await queue.time_accum_resource_within(0, 2) == 210
  assert await queue.time_accum_resource_within(0, 3) == 200
  assert await queue.time_accum_resource_within(0, 5) == 200
  assert await queue.time_accum_resource_within(0, 6) == 100
  assert await queue.time_accum_resource_within(1, 2) == 220
  assert await queue.time_accum_resource_within(1, 3) == 210
  assert await queue.time_accum_resource_within(1, 4) == 200
  assert await queue.time_accum_resource_within(1, 13) == 200
  assert await queue.time_accum_resource_within(1, 14) == 100

@pytest.mark.asyncio
async def test_past_with_file(datadir):
  # Actually, the contents of datadir are copied to a temporary folder
  original_path = (datadir / 'original.tsv')
  # Copy to backup the original
  test_path = (datadir / 'test.tsv')
  await wrap(shutil.copyfile)(original_path, test_path)
  queue = await FilePastResourceQueue.create(2, 60, test_path)
  assert len(queue._time_resource_queue) == 4
  assert queue._time_resource_queue[0] == (0, [0, 0])
  assert queue._time_resource_queue[1] == (100, [1, 10])
  assert queue._time_resource_queue[2] == (110, [2, 15])
  assert queue._time_resource_queue[3] == (120, [4, 30])
  await queue.add(175, [10, 30])
  await queue.term()
  queue = await FilePastResourceQueue.create(2, 60, test_path)
  assert len(queue._time_resource_queue) == 3
  assert queue._time_resource_queue[0] == (110, [2, 15])
  assert queue._time_resource_queue[1] == (120, [4, 30])
  assert queue._time_resource_queue[2] == (175, [14, 60])
  # Delete the temporary folder just in case
  if await wrap(isfile)(test_path):
    await wrap(os.remove)(test_path)

@pytest.mark.asyncio
async def test_past_parse_line():
  queue = await FilePastResourceQueue.create(2, 60)
  with pytest.raises(ValueError):
    # Lack of line terminator
    queue._parse_line('100\t1\t2')
  with pytest.raises(ValueError):
    # Length mismatch
    queue._parse_line('100\t1\n')
  with pytest.raises(ValueError):
    # Number format error
    queue._parse_line('100\t1\tq\n')

@pytest.mark.asyncio
async def test_past_not_found(datadir):
  not_found_file = (datadir / 'not_found.tsv')
  queue = await FilePastResourceQueue.create(2, 60, not_found_file)
  assert len(queue._time_resource_queue) == 1
