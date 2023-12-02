from typing import List

class RateLimit:
  def __init__(self, period_in_seconds: float, resource_limit: int):
    if period_in_seconds > 0 and resource_limit > 0:
      self.period_in_seconds = period_in_seconds
      self.resource_limit = resource_limit
    else:
      raise ValueError(f'{resource_limit} / {period_in_seconds}')

class SecondRateLimit(RateLimit):
  def __init__(self, resource_limit: int):
    super().__init__(1, resource_limit)

class MinuteRateLimit(RateLimit):
  def __init__(self, resource_limit: int):
    super().__init__(60, resource_limit)

class HourRateLimit(RateLimit):
  def __init__(self, resource_limit: int):
    super().__init__(3600, resource_limit)

class DayRateLimit(RateLimit):
  def __init__(self, resource_limit: int):
    super().__init__(86400, resource_limit)


class RateLimitError(Exception):
  def __init__(self, use_time: float, use_resources: List[int], cause: Exception):
    self.use_time = use_time
    self.use_resources = use_resources
    self.cause = cause
  
  def __str__(self):
    return f'MultiRateLimitError: time={self.use_time}, res={self.use_resources}, cause={self.cause}'
