import asyncio
import pytest

from multi_rate_limit.rate_limit import RateLimit, SecondRateLimit, MinuteRateLimit, HourRateLimit, DayRateLimit, FilePastResourceQueue

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
