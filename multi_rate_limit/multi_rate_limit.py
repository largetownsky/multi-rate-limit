"""Classes for using multiple resources while observing multiple RateLimits.
"""
import asyncio
import time

from asyncio import Future, Task
from collections.abc import KeysView
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional, Tuple

from multi_rate_limit.rate_limit import FilePastResourceQueue, IPastResourceQueue, RateLimit
from multi_rate_limit.resource_queue import CurrentResourceBuffer, NextResourceQueue, check_resources


@dataclass
class ReservationTicket:
  """Class for receiving the results of processing executed through MultiRateLimit.

  Attributes:
    reserve_number (int): Number to interrupt execution.
    future (Future[Any]): Future to receive execution results.
  """
  reserve_number: int
  future: Future[Any]


@dataclass
class RateLimitStats:
  """Class that represents resource usage status.

  Attributes:
    limits (List[List[RateLimit]]): Resource limits
    past_uses (List[List[int]]): Total resource usage that has been executed for each resource limit.
        (For 1 minute limit, resource usage for the past 1 minute.)
    current_uses (List[int]): Total running resource usage for each resource.
    next_uses (List[int]): Total waiting resource usage for each resource.
  """
  limits: List[List[RateLimit]]
  past_uses: List[List[int]]
  current_uses: List[int]
  next_uses: List[int]

  def past_use_percents(self) -> List[List[float]]:
    """Returns the percentage of total executed resource usage against each resource limit.

    Returns:
        List[List[float]]: The percentage of total executed resource usage against each resource limit.
    """
    return [[p * 100 / l.resource_limit for l, p in zip(ls, ps)] for ls, ps in zip(self.limits, self.past_uses)]

  def current_use_percents(self) -> List[List[float]]:
    """Returns the percentage of total executed and running resource usage relative to each resource limit.

    Returns:
        List[List[float]]: The percentage of total executed and running resource usage relative to each resource limit.
    """
    return [[(p + c) * 100 / l.resource_limit for l, p in zip(ls, ps)]
        for ls, ps, c in zip(self.limits, self.past_uses, self.current_uses)]

  def next_use_percents(self) -> List[List[float]]:
    """Returns the total usage of executed, running, and waiting resources as a percentage of each resource limit.

    Returns:
        List[List[float]]: The total usage of executed, running, and waiting resources as a percentage of each resource limit.
    """
    return [[(p + c + n) * 100 / l.resource_limit for l, p in zip(ls, ps)]
        for ls, ps, c, n in zip(self.limits, self.past_uses, self.current_uses, self.next_uses)]


class MultiRateLimit:
  """Class for using multiple resources while observing multiple RateLimits.

  Attributes:
    _limits (List[List[RateLimit]]): Resource limits.
    _past_queue (IPastResourceQueue): Executed resource usage manager.
    _current_buffer (CurrentResourceBuffer): Running resource usage manager.
    _next_queue (NextResourceQueue): Waiting resource usage manager.
    _loop (AbstractEventLoop): Cached event loop.
    _in_process (Optional[Task]): Asynchronous execution tasks for internal processing.
    _terminated (bool): Whether term() has been called.
  """
  @classmethod
  async def create(cls, limits: List[List[RateLimit]]
      , past_queue_factory: Callable[[int, float], Coroutine[Any, Any, IPastResourceQueue]] = None, max_async_run = 1):
    """Create an object for using multiple resources while observing multiple RateLimits.

    Args:
        limits (List[List[RateLimit]]): Resource limits.
        past_queue_factory (Callable[[int, float], Coroutine[Any, Any, IPastResourceQueue]], optional):
            Pass the factory method to make the executed resource usage manager.
            The default is None, in which case it is managed only in memory.
        max_async_run (int, optional): Maximum asynchronous concurrency. Defaults to 1.

    Raises:
        ValueError: If the resource limit array length is 0, or if any value of the resource limit or max_async_run is non-positive.

    Returns:
        _type_: Object for using multiple resources while observing multiple RateLimits.
    """
    if len(limits) <= 0 or min([len(ls) for ls in limits]) <= 0 or max_async_run <= 0:
      raise ValueError(f'Invalid None positive length or values : {[len(ls) for ls in limits]}, {max_async_run}')
    if past_queue_factory is None:
      past_queue_factory = lambda len_resource, longest_period_in_seconds: FilePastResourceQueue.create(
          len_resource, longest_period_in_seconds)
    mrl = cls()
    # Copy for overwrite safety
    mrl._limits = [[*ls] for ls in limits]
    mrl._past_queue = await past_queue_factory(len(limits), max([max([l.period_in_seconds for l in ls]) for ls in limits]))
    mrl._current_buffer = CurrentResourceBuffer(len(limits), max_async_run)
    mrl._next_queue = NextResourceQueue(len(limits))
    mrl._loop = asyncio.get_running_loop()
    mrl._in_process: Optional[Task] = None
    mrl._teminated: bool = False
    return mrl
  
  def termed(self) -> bool:
    """Returns whether this object is termed.

    Returns:
        bool: Whether this object is termed.
    """
    return self._teminated
  
  def runnings(self) -> int:
    """Returns the number of currently running coroutines.

    Returns:
        int: The number of currently running coroutines.
    """
    return self._current_buffer.active_run
  
  def waitings(self) -> int:
    """Returns the number of waiting coroutines.

    Returns:
        int: The number of waiting coroutines.
    """
    return len(self._next_queue.number_to_resource_coro_future)
  
  def waiting_numbers(self) -> KeysView[int]:
    """Returns waiting coroutines' reservation numbers.

    Returns:
        KeysView[int]: Waiting coroutines' reservation numbers.
    """
    return self._next_queue.number_to_resource_coro_future.keys()
  
  async def _process(self) -> None:
    """Internal processing that manages waiting, running, and executed state transitions.

    Raises:
        Exception: In case of unknown logic errors.
    """
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
              raise Exception('Internal logic error')
            break
        # Wait for current buffer (and past queue to free up space)
        tasks = [t for t in self._current_buffer.task_buffer if t is not None]
        if delay > 0:
          tasks.append(asyncio.create_task(asyncio.sleep(delay), name=''))
        if len(tasks) <= 0:
          raise Exception('Internal logic error')
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
    """Trigger internal processing.
    """
    if self._in_process is not None:
      self._in_process.cancel()
    self._in_process = asyncio.create_task(self._process())
  
  async def _resouce_sum_from_past(self, current_time: float) -> List[List[int]]:
    """For each resource limit, calculate the resource usage during the limit period given the current time.

    Args:
        current_time (float): The current time compatible with time.time().

    Returns:
        List[List[int]]: The resource usage during the limit period for each resource limit.
    """
    times = [[(current_time - l.period_in_seconds) for l in ls] for ls in self._limits]
    return await asyncio.gather(*[asyncio.gather(*[self._past_queue.sum_resource_after(t, i) for t in ts]) for i, ts in enumerate(times)])

  async def _resource_margin_from_past(self, current_time: float) -> List[int]:
    """Calculate how much of each resource can be allocated to resource consumption during execution.

    Args:
        current_time (float): The current time compatible with time.time().

    Returns:
        List[int]: How much of each resource can be allocated to resource consumption during execution.
    """
    return [min([l.resource_limit - r for l, r in zip(ls, rs)])
        for ls, rs in zip(self._limits, await self._resouce_sum_from_past(current_time))]

  async def _time_to_start(self, sum_resourcs_without_past: List[int]) -> float:
    """Returns the time when the next execution can start based on the current and next execution's resource usage.

    Args:
        sum_resourcs_without_past (List[int]): The current and next execution's resource usage.

    Returns:
        float: The time compatible with time.time() when the next execution can start.
    """
    base_times = await asyncio.gather(*[asyncio.gather(*[self._past_queue.time_accum_resource_within
        (i, l.resource_limit - sr) for l in ls]) for i, (ls, sr) in enumerate(zip(self._limits, sum_resourcs_without_past))])
    return max([max([l.period_in_seconds + t for l, t in zip(ls, bt)]) for ls, bt in zip(self._limits, base_times)])
  
  def _add_next(self, use_resources: List[int], coro: Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]
      , future: Future[Any]) -> ReservationTicket:
    """Puts the task on a waiting queue and returns a ticket to receive the result.

    Args:
        use_resources (List[int]): Resource reservation amount.
        coro (Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]):
            Coroutine object that is the process to reserve
        future (Future[Any]): Future for receiving processing results.

    Returns:
        ReservationTicket: Ticket for receiving processing results.
    """
    reserve_number = self._next_queue.push(use_resources, coro, future)
    return ReservationTicket(reserve_number, future)

  def reserve(self, use_resources: List[int]
      , coro: Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]) -> ReservationTicket:
    """Schedules the task and returns a ticket to receive the result.

    Unless explicitly stated in the return value or exception parameter of coroutine,
    the use_resources of this function are considered to have been consumed at the end of coroutine execution.
    If you want to change this behavior because you cannot know the exact resource consumption until after execution,
    please override the resource consumption timing and amount using coroutine's return value or ResourceOverwriteError parameter.

    The return value of coroutine is in the following format.
    ((use_time, [use_resource1, use_resource2,,,]), return_value_to_user)
    If you do not want to overwrite, please use the followin format.
    (None, return_value_to_user)

    Args:
        use_resources (List[int]): Resource reservation amount.
        coro (Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]):
            Coroutine object that is the process to reserve

    Raises:
        Exception: If already terminated.
        ValueError: In case of resources list length mismatch or any single resource reservation exceeds its limit. 
        ValueError: If the passed process is not a coroutine.

    Returns:
        ReservationTicket: Ticket to receive the result.
    """
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

  def cancel(self, number: int, auto_close: bool = False) -> Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]]]:
    """Cancel the reservation of a waiting coroutine.

    This process automatically cancels the future of the ticket.

    Args:
        number (int): Ticket number.
        auto_close (bool, optional): If true, automatically close the canceled coroutine. Defaults to False.
            This coroutine can be reused. But you don't reuse it, Runtime warning will occure when the program finish.
            Automatic close can suppress this warning.

    Raises:
        Exception: If already terminated.

    Returns:
        Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]]]:
            Reserved resource amount and coroutine object.
    """
    if self._teminated:
      raise Exception('Already terminated')
    res = self._next_queue.cancel(number)
    if res is None:
      return None
    use_resources, coro, future, is_next_pop = res
    # Cancel it so you don't have to wait forever due to client's logic mistakes
    future.cancel()
    if auto_close:
      coro.close()
    if is_next_pop and not self._current_buffer.is_full():
      self._try_process()
    return use_resources, coro

  async def stats(self, current_time: Optional[float] = None) -> RateLimitStats:
    """Returns resource usage.

    Args:
        current_time (Optional[float], optional): The current time.
            The default is None, in which case the result of time.time() is used.

    Raises:
        Exception: If already terminated.

    Returns:
        RateLimitStats: Resource usage.
    """
    if self._teminated:
      raise Exception('Already terminated')
    if current_time is None:
      current_time = time.time()
    return RateLimitStats([[*ls] for ls in self._limits], await self._resouce_sum_from_past(current_time)
        , [*self._current_buffer.sum_resources], [*self._next_queue.sum_resources])
  
  async def term(self, auto_close: bool = False) -> List[Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]]:
    """End processing.

    Cancels all waiting processes and waits for all currently running processes to finish
    and for resource managers that have already been executed to terminate.

    Args:
        auto_close (bool, optional): If true, automatically close the canceled coroutine. Defaults to False.
            This coroutine can be reused. But you don't reuse it, Runtime warning will occure when the program finish.
            Automatic close can suppress this warning.

    Raises:
        Exception: If already terminated.

    Returns:
        List[Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]]:
            Waiting coroutines.
    """
    coros: List[Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]]] = []
    if self._teminated:
      raise Exception('Already terminated')
    self._teminated = True
    # Dispose all next coroutines
    while True:
      res = self._next_queue.pop()
      if res is None:
        break
      _, coro, future = res
      coros.append(coro)
      future.cancel()
      if auto_close:
        coro.close()
    # The internal process continues to run until all current tasks are completed
    # Reset the internal process bacause the it already start to consume the canceled task
    self._try_process()
    await self._in_process
    await self._past_queue.term()
    return coros
