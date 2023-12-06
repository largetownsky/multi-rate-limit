"""Classes for users of multi_rate_limit.
"""
import abc
import bisect

from collections import deque
from typing import List, Optional, Tuple

class RateLimit:
  """A class to define a single resource limit.

  Attributes:
    _resource_limit (int): Resource limit that can be used within the period.
    _period_in_seconds (float): Resource limit period.
  """
  def __init__(self, resource_limit: int, period_in_seconds: float):
    """Create a class to define a single resource limit.

    Args:
        resource_limit (int): Resource limit that can be used within the period.
        period_in_seconds (float): Resource limit period in seconds.

    Raises:
        ValueError: Error when resource cap or period is non-positive.
    """
    if period_in_seconds > 0 and resource_limit > 0:
      self._resource_limit = resource_limit
      self._period_in_seconds = period_in_seconds
    else:
      raise ValueError(f'{resource_limit} / {period_in_seconds}')
  
  @property
  def period_in_seconds(self) -> float:
    """Return the resource limit period in seconds.

    Returns:
        float: Resource limit period in seconds.
    """
    return self._period_in_seconds
  
  @property
  def resource_limit(self) -> int:
    """Return the resource limit that can be used within the period.

    Returns:
        int: Resource limit that can be used within the period.
    """
    return self._resource_limit

class SecondRateLimit(RateLimit):
  """An alias of RateLimit. Specify duration in seconds.
  """
  def __init__(self, resource_limit: int, period_in_seconds = 1.0):
    """Create a class to define a single resource limit.

    Args:
        resource_limit (int): Resource limit that can be used within the period.
        period_in_seconds (float, optional): Resource limit period in seconds. Defaults to 1.0.
    """
    super().__init__(resource_limit, period_in_seconds)

class MinuteRateLimit(RateLimit):
  """A variant of RateLimit. Specify duration in minutes.
  """
  def __init__(self, resource_limit: int, period_in_minutes = 1.0):
    """Create a class to define a single resource limit.

    Args:
        resource_limit (int): Resource limit that can be used within the period.
        period_in_minutes (float, optional): Resource limit period in minutes. Defaults to 1.0.
    """
    super().__init__(resource_limit, 60 * period_in_minutes)

class HourRateLimit(RateLimit):
  """A variant of RateLimit. Specify duration in hours.
  """
  def __init__(self, resource_limit: int, period_in_hours = 1.0):
    """Create a class to define a single resource limit.

    Args:
        resource_limit (int): Resource limit that can be used within the period.
        period_in_hours (float, optional): Resource limit period in hours. Defaults to 1.0.
    """
    super().__init__(resource_limit, 3600 * period_in_hours)

class DayRateLimit(RateLimit):
  """A variant of RateLimit. Specify duration in days.
  """
  def __init__(self, resource_limit: int, period_in_days = 1.0):
    """Create a class to define a single resource limit.

    Args:
        resource_limit (int): Resource limit that can be used within the period.
        period_in_days (float, optional): Resource limit period in days. Defaults to 1.0.
    """
    super().__init__(resource_limit, 86400 * period_in_days)


class ResourceOverwriteError(Exception):
  """An error to customize resource usage.
  
  You can use this error when you want to change the amount or timing of resource usage
  while returning an exception from within a coroutine that applies RateLimit.

  Attributes:
      use_time (float): Resource usage time compatible with time.time() to be overwritten.
      use_resources (List[int]): Resource usage amounts to be overwritten.
          The length of list must be same as the number of resources.
          Each resource usage amaount must not be negative.
      cause (Exception): Wrap and pass the exception you originally wanted to return.
  """
  def __init__(self, use_time: float, use_resources: List[int], cause: Exception):
    """Create an error to customize resource usage.

    You can use this error when you want to change the amount or timing of resource usage
    while returning an exception from within a coroutine that applies RateLimit.

    Args:
        use_time (float): Resource usage time compatible with time.time() to be overwritten.
        use_resources (List[int]): Resource usage amounts to be overwritten.
            The length of list must be same as the number of resources.
            Each resource usage amaount must not be negative.
        cause (Exception): Wrap and pass the exception you originally wanted to return.
    """
    self.use_time = use_time
    self.use_resources = use_resources
    self.cause = cause
  
  def __str__(self) -> str:
    """Return exception information as a string.

    Returns:
        str: Exception information as a string.
    """
    return f'MultiRateLimitError: time={self.use_time}, res={self.use_resources}, cause={self.cause}'


class IPastResourceQueue(metaclass=abc.ABCMeta):
  """An interface to customize how used resources are managed.
  """
  @abc.abstractmethod
  def sum_resource_after(self, time: float, order: int) -> int:
    """Returns the amount of resources of specified order used after the specified time.

    If the specified time is before the last resource use beyond the period passed at the constructor,
    it is okay to return incorrect information.
    This allows old information unrelated to resource limit management to be forgotten.

    Args:
        time (float): The specified time compatible with time.time().
        order (int): The order of resource.

    Returns:
        int: The amount of resources of specified order used after the specified time.
    """
    raise NotImplementedError()
  
  @abc.abstractmethod
  def time_accum_resource_within(self, order: int, amount: int) -> float:
    """Returns the last timing when resource usage falls within the specified amount.

    Returns the latest timing at which the cumulative amount of resource usage
    exceeds the specified amount, going back from the current time.

    Args:
        order (int): The order of resource.
        amount (int): The specified amout.

    Returns:
        float: The last timing compatible with time.time() when resource usage falls within the specified amount.
    """
    raise NotImplementedError()
  
  @abc.abstractmethod
  def add(self, use_time: float, use_resources: List[int]) -> None:
    """Add resource usage information.

    Args:
        use_time (float): Resource usage time compatible with time.time().
        use_resources (List[int]): Resource usage amounts.
            The length of list must be same as the number of resources.
            Each resource usage amaount must not be negative.
    """
    raise NotImplementedError()
  
  @abc.abstractmethod
  async def term(self) -> None:
    """Called when finished.

    Can be used to persist unrecorded data.
    It is not guaranteed that it will be called, so you should make sure
    that it does not cause a fatal situation even if it is not called.
    """
    raise NotImplementedError()


class FilePastResourceQueue(IPastResourceQueue):
  """An class to manage resource usage with memory and file(Optional).

  Attributes:
      time_resource_queue (deque[Tuple[float, List[int]]]): A queue to manage resource usage.
      longest_period_in_seconds (float): Information before this is forgotten.


      
  """
  def __init__(self, len_resource: int, longest_period_in_seconds: float, save_file_path: Optional[str] = None):
    # Append the first element with time and accumulated resource usages.
    self.time_resource_queue: deque[Tuple[float, List[int]]] = deque([(0, [0 for _ in range(len_resource)])])
    self.longest_period_in_seconds: float = longest_period_in_seconds
  
  def pos_time_after(self, time: float) -> int:
    return bisect.bisect_right(self.time_resource_queue, time, key=lambda t: t[0])
  
  def sum_resource_after(self, time: float, order: int) -> int:
    pos = self.pos_time_after(time)
    return self.time_resource_queue[-1][1][order] - self.time_resource_queue[max(0, pos - 1)][1][order]

  def pos_accum_resouce_within(self, order: int, amount: int) -> int:
    last_amount = self.time_resource_queue[-1][1][order]
    return bisect.bisect_left(self.time_resource_queue, last_amount - amount, key=lambda t: t[1][order])
  
  def time_accum_resource_within(self, order: int, amount: int) -> float:
    pos = self.pos_accum_resouce_within(order, amount)
    return self.time_resource_queue[pos][0]
  
  def add(self, use_time: float, use_resources: List[int]) -> None:
    last_elem = self.time_resource_queue[-1]
    if use_time <= last_elem[0]:
      # Never add before last registered time
      use_time = last_elem[0]
      # For search uniqueness, information from the same time is merged.
      use_resources = [x + y for x, y in zip(last_elem[1], use_resources)]
      # Replace the last
      self.time_resource_queue[-1] = use_time, use_resources
      return
    # Append the last
    self.time_resource_queue.append((use_time, [x + y for x, y in zip(last_elem[1], use_resources)]))
    # Delete old unnecessary information
    pos = self.pos_time_after(use_time - self.longest_period_in_seconds)
    # To obtain the difference, the previous information is required.
    for i in range(max(0, pos - 1)):
      self.time_resource_queue.popleft()

  async def term(self) -> None:
    """Called when finished. Do nothing.
    """
    pass
