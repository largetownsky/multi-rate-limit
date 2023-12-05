import pytest
from multi_rate_limit.rate_limit import RateLimit, SecondRateLimit, MinuteRateLimit, HourRateLimit, DayRateLimit

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
