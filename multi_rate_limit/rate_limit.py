from typing import List

class RateLimit:
  def __init__(self, resource_limit: int, period_in_seconds: float):
    if period_in_seconds > 0 and resource_limit > 0:
      self._resource_limit = resource_limit
      self._period_in_seconds = period_in_seconds
    else:
      raise ValueError(f'{resource_limit} / {period_in_seconds}')
  
  @property
  def period_in_seconds(self):
    return self._period_in_seconds
  
  @property
  def resource_limit(self):
    return self._resource_limit

class SecondRateLimit(RateLimit):
  def __init__(self, resource_limit: int, period_in_seconds = 1.0):
    super().__init__(resource_limit, period_in_seconds)

class MinuteRateLimit(RateLimit):
  def __init__(self, resource_limit: int, period_in_minutes = 1.0):
    super().__init__(resource_limit, 60 * period_in_minutes)

class HourRateLimit(RateLimit):
  def __init__(self, resource_limit: int, period_in_hours = 1.0):
    super().__init__(resource_limit, 3600 * period_in_hours)

class DayRateLimit(RateLimit):
  def __init__(self, resource_limit: int, period_in_days = 1.0):
    super().__init__(resource_limit, 86400 * period_in_days)


class RateLimitError(Exception):
  def __init__(self, use_time: float, use_resources: List[int], cause: Exception):
    self.use_time = use_time
    self.use_resources = use_resources
    self.cause = cause
  
  def __str__(self):
    return f'MultiRateLimitError: time={self.use_time}, res={self.use_resources}, cause={self.cause}'
