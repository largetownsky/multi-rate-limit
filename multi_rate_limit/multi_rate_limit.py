import asyncio
import time

from asyncio import Future, Task
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional, Tuple

from multi_rate_limit.rate_limit import FilePastResourceQueue, IPastResourceQueue, RateLimit
from multi_rate_limit.resource_queue import CurrentResourceBuffer, NextResourceQueue, check_resources


@dataclass
class ReservationTicket:
  reserve_number: int
  future: Future[Any]


@dataclass
class RateLimitStats:
  limits: List[List[RateLimit]]
  past_uses: List[List[int]]
  current_uses: List[int]
  next_uses: List[int]

  def past_use_percents(self) -> List[List[float]]:
    return [[p / l.resource_limit for l, p in zip(ls, ps)] for ls, ps in zip(self.limits, self.past_uses)]

  def current_use_percents(self) -> List[List[float]]:
    return [[(p + c) / l.resource_limit for l, p in zip(ls, ps)]
        for ls, ps, c in zip(self.limits, self.past_uses, self.current_uses)]

  def next_use_percents(self) -> List[List[float]]:
    return [[(p + c + n) / l.resource_limit for l, p in zip(ls, ps)]
        for ls, ps, c, n in zip(self.limits, self.past_uses, self.current_uses, self.next_uses)]


class MultiRateLimit:
  def __init__(self, limits: List[List[RateLimit]]
      , past_queue_factory: Callable[[int, float], IPastResourceQueue] = None, max_async_run = 1):
    if len(limits) <= 0 or min([len(ls) for ls in limits]) <= 0 or max_async_run <= 0:
      raise ValueError(f'Invalid None positive length or values : {[len(ls) for ls in limits]}, {max_async_run}')
    if past_queue_factory is None:
      past_queue_factory = lambda len_resource, longest_period_in_seconds: FilePastResourceQueue(len_resource, longest_period_in_seconds)
    # Copy for overwrite safety
    self._limits = [[*ls] for ls in limits]
    self._past_queue = past_queue_factory(len(limits), max([max([l.period_in_seconds for l in ls]) for ls in limits]))
    self._current_buffer = CurrentResourceBuffer(len(limits), max_async_run)
    self._next_queue = NextResourceQueue(len(limits))
    self._loop = asyncio.get_running_loop()
    self._in_process: Optional[Task] = None
    self._teminated: bool = False
  
  async def _process(self) -> None:
    ex: Optional[Exception] = None
    while True:
      try:
        # The only time next is swapped during await is if it is canceled,
        # in which case it will start over from the beginning,
        # so unless "changing a state that cannot maintain consistency",
        # do not worry about the discrepancy between before and after await.
        delay = 0
        # Stuff into the current buffer
        if self._next_queue.is_empty():
          if self._current_buffer.is_empty():
            # Since it is completely empty, exit the process for now
            # Kicked when added from outside again
            break
        else:
          current_time = time.time()
          resource_margin_from_past: Optional[List[int]] = None
          while not self._next_queue.is_empty():
            if self._current_buffer.is_full():
              break
            next_resources, coro, future = self._next_queue.peek()
            # Check the resource usage of current and next within their limits 
            sum_resources = [c + r for c, r in zip(self._current_buffer.sum_resources, next_resources)]
            if any([any([l.resource_limit < sr for l in ls]) for ls, sr in zip(self._limits, sum_resources)]):
              break
            # Check the total resource usage within their limits
            if resource_margin_from_past is None:
              resource_margin_from_past = await self._resource_margin_from_past(current_time)
            if all([rm >= sr for rm, sr in zip(resource_margin_from_past, sum_resources)]):
              self._next_queue.pop()
              self._current_buffer.start_coroutine(next_resources, coro, future)
              continue
            # Predict time to accept
            time_to_start = await self._time_to_start(sum_resources)
            delay = max(0, time_to_start - current_time)
            if delay <= 0:
              raise ValueError('Internal logic error')
            break
        # Wait for current buffer (and past queue to free up space)
        tasks = [t for t in self._current_buffer.task_buffer if t is not None]
        if delay > 0:
          tasks.append(asyncio.create_task(asyncio.sleep(delay), name=''))
        if len(tasks) <= 0:
          raise ValueError('Internal logic error')
        dones, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        current_time = time.time()
        for done in dones:
          name = done.get_name()
          if name == '':
            # Since the resource usage may change, the interpretation of next queue is passed to the next loop
            continue
          use_time, use_resources = self._current_buffer.end_coroutine(current_time, done)
          # The only time when there is a possibility that consistency will not be maintained if it is canceled.
          # By shielding, the await itself is canceled, but the internal add task continues to be executed.
          await asyncio.shield(self._past_queue.add(use_time, use_resources))
      except asyncio.exceptions.CancelledError:
        break
      except Exception as ex:
        break
    self._in_process = None
    if ex is not None:
      raise ex

  def _try_process(self) -> None:
    if self._in_process is not None:
      self._in_process.cancel()
    self._in_process = asyncio.create_task(self._process())
  
  async def _resouce_sum_from_past(self, current_time: float) -> List[List[int]]:
    return [[await self._past_queue.sum_resource_after(current_time - l.period_in_seconds, i) for l in ls]
        for i, ls in enumerate(self._limits)]

  async def _resource_margin_from_past(self, current_time: float) -> List[int]:
    return [min([l.resource_limit - r for l, r in zip(ls, rs)])
        for ls, rs in zip(self._limits, await self._resouce_sum_from_past(current_time))]

  async def _time_to_start(self, sum_resourcs_without_past: List[int]) -> float:
    return max([max([l.period_in_seconds + await self._past_queue.time_accum_resource_within(i, l.resource_limit - sr) for l in ls])
        for i, (ls, sr) in enumerate(zip(self._limits, sum_resourcs_without_past))])
  
  def _add_next(self, use_resources: List[int], coro: Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]
      , future: Future[Any]) -> ReservationTicket:
    reserve_number = self._next_queue.push(use_resources, coro, future)
    return ReservationTicket(reserve_number, future)

  def reserve(self, use_resources: List[int]
      , coro: Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]) -> ReservationTicket:
    if self._teminated:
      raise Exception('Already terminated')
    use_resources = check_resources(use_resources, len(self._limits))
    if any([any([l.resource_limit < r for l in ls]) for ls, r in zip(self._limits, use_resources)]):
      raise ValueError(f'Using resources exceed the capacity : {use_resources}')
    if not asyncio.iscoroutine(coro):
      raise ValueError('Parameter is not a coroutine')
    is_next_empty = self._next_queue.is_empty()
    ticket = self._add_next(use_resources, coro, self._loop.create_future())
    # The current buffer is the bottleneck, so adding it to the queue does not change what is monitored
    if not is_next_empty or self._current_buffer.is_full():
      return ticket
    rest_resources = [min([l.resource_limit for l in ls]) - cr - ur
        for ls, cr, ur in zip(self._limits, self._current_buffer.sum_resources, use_resources)]
    if 0 <= min(rest_resources):
      self._try_process()
    return ticket

  def cancel(self, number: int) -> Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]]]:
    if self._teminated:
      raise Exception('Already terminated')
    res = self._next_queue.cancel(number)
    if res is None:
      return None
    use_resources, coro, future, is_next_pop = res
    # Cancel it so you don't have to wait forever due to client's logic mistakes
    future.cancel()
    if is_next_pop and not self._current_buffer.is_full():
      self._try_process()
    return use_resources, coro

  async def stats(self, current_time = None) -> RateLimitStats:
    if self._teminated:
      raise Exception('Already terminated')
    if current_time is None:
      current_time = time.time()
    return RateLimitStats([[*ls] for ls in self._limits], await self._resouce_sum_from_past(current_time)
        , [*self._current_buffer.sum_resources], [*self._next_queue.sum_resources])
  
  async def term(self) -> None:
    if self._teminated:
      raise Exception('Already terminated')
    self._teminated = True
    # Dispose all next coroutines
    while True:
      res = self._next_queue.pop()
      if res is None:
        break
      res[2].cancel()
    # The internal process continues to run until all current tasks are completed
    await self._past_queue.term()
