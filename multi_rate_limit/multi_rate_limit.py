import time

from asyncio import Future
from dataclasses import dataclass
from typing import Any, Coroutine, List

from multi_rate_limit.rate_limit import RateLimit, RateLimitError


@dataclass
class ReservationTicket:
  number_for_cancel: int
  future: Future[Any]


'''
class MultiRateLimit:
  def __init__(self, limits: List[List[RateLimit]], max_async_run = 1):
    pass

  def available_time(self, use_resources: List[int]) -> float:
    pass

  def wait_in_seconds_to_avail(self, use_resources: List[int]) -> float:
    available_time = self.available_time(use_resources)
    current = time.time()
    return max(0, available_time - current)

  def regist(self, limits: List[int], coro: Coroutine[None | (time, ress), Any]) -> ReservationTicket:
    psss
'''