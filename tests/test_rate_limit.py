import pytest
from multi_rate_limit.rate_limit import RateLimit, SecondRateLimit, MinuteRateLimit, HourRateLimit, DayRateLimit

@pytest.mark.parametrize(
    "period, limit",
    [
      (0.1, 2),
      (3, 4)
    ]
)
def test_rate_limit(period: float, limit: int) -> None:
  rl = RateLimit(period, limit)
  assert rl.period_in_seconds == period
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "period, limit",
    [
      (0, 2),
      (3, -1)
    ]
)
def test_rate_limit_error(period: float, limit: int) -> None:
  with pytest.raises(ValueError) as e:
    RateLimit(period, limit)

@pytest.mark.parametrize(
    "limit",
    [
      (1),
      (4)
    ]
)
def test_second_rate_limit(limit: int) -> None:
  rl = SecondRateLimit(limit)
  assert rl.period_in_seconds == 1
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit",
    [
      (1),
      (4)
    ]
)
def test_minute_rate_limit(limit: int) -> None:
  rl = MinuteRateLimit(limit)
  assert rl.period_in_seconds == 60
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit",
    [
      (1),
      (4)
    ]
)
def test_hour_rate_limit(limit: int) -> None:
  rl = HourRateLimit(limit)
  assert rl.period_in_seconds == 3600
  assert rl.resource_limit == limit

@pytest.mark.parametrize(
    "limit",
    [
      (1),
      (4)
    ]
)
def test_day_rate_limit(limit: int) -> None:
  rl = DayRateLimit(limit)
  assert rl.period_in_seconds == 86400
  assert rl.resource_limit == limit
