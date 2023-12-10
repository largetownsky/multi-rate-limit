"""Classes for internal use.
"""
from asyncio import create_task, Future, Task
from typing import Any, Coroutine, Dict, List, Optional, Tuple

from multi_rate_limit.rate_limit import ResourceOverwriteError


def check_resources(resources: List[float], len_res: int) -> List[float]:
  if len(resources) != len_res or 0 > min(resources):
    raise ValueError(f'Invalid resources with invalid length or negative values : {resources} : {len_res}')
  # Copy for overwrite safety
  return [*resources]


class CurrentResourceBuffer:
  def __init__(self, len_resource: int, max_async_run: int):
    # Candidate amount list to use resources
    self.resource_buffer: List[Optional[List[int]]] = [None for i in range(max_async_run)]
    self.task_buffer: List[Optional[Task[Tuple[Optional[Tuple[float, List[int]]], Any]]]] = [None for i in range(max_async_run)]
    # Future list returned to client
    self.future_buffer: List[Optional[Future[Any]]] = [None for i in range(max_async_run)]
    # Next buffer position for fast search
    self.next: int = 0
    self.active_run: int = 0
    self.sum_resources: List[int] = [0 for _ in range(len_resource)]
  
  def is_empty(self) -> bool:
    return self.active_run <= 0

  def is_full(self) -> bool:
    return self.active_run >= len(self.resource_buffer)
  
  def start_coroutine(self, use_resources: List[int]
      , coro: Coroutine[Any, Any, Tuple[Optional[Tuple[float, List[int]]], Any]], future: Future[Any]) -> bool:
    if self.is_full():
      return False
    # Search an empty index
    pos = self.next
    while True:
      if self.resource_buffer[pos] is None:
        break
      pos = (pos + 1) % len(self.resource_buffer)
      if pos == self.next:
        raise Exception(f'Unexpected buffer full with {self.active_run} / {len(self.resource_buffer)}')
    # Start a coroutine
    task = create_task(coro, name=pos)
    self.resource_buffer[pos] = use_resources
    self.task_buffer[pos] = task
    self.future_buffer[pos] = future
    self.next = (pos + 1) % len(self.resource_buffer)
    self.active_run += 1
    self.sum_resources = [x + y for x, y in zip(self.sum_resources, use_resources)]
    return True
  
  def end_coroutine(self, use_time: float
      , finished_task: Task[Tuple[Optional[Tuple[float, List[int]]], Any]]) -> Tuple[float, List[int]]:
    pos = int(finished_task.get_name())
    use_resources = self.resource_buffer[pos]
    # Finish a futuer for the client
    try:
      overwrite_time_resources, result = finished_task.result()
      if overwrite_time_resources is not None:
        use_resources = check_resources(overwrite_time_resources[1], len(self.sum_resources))
        use_time = overwrite_time_resources[0]
      self.future_buffer[pos].set_result(result)
    except ResourceOverwriteError as e:
      try:
        use_resources = check_resources(e.use_resources, len(self.sum_resources))
        use_time = e.use_time
        self.future_buffer[pos].set_exception(e.cause)
      except Exception as e2:
        self.future_buffer[pos].set_exception(e2)
    except Exception as e:
      self.future_buffer[pos].set_exception(e)
    # Update parameters
    self.sum_resources = [x - y for x, y in zip(self.sum_resources, self.resource_buffer[pos])]
    self.resource_buffer[pos] = None
    self.task_buffer[pos] = None
    self.future_buffer[pos] = None
    self.active_run -= 1
    return use_time, use_resources


class NextResourceQueue:
  def __init__(self, len_resource: int):
    self.number_to_resource_coro_future: Dict[int, Tuple[List[int]
        , Coroutine[Any, Any, Tuple[Optional[List[int]], Any]], Future[Any]]] = {}
    self.next_add: int = 0
    self.next_run: int = 0
    self.sum_resources: List[int] = [0 for _ in range(len_resource)]
  
  def is_empty(self) -> bool:
    return len(self.number_to_resource_coro_future) <= 0
    
  def push(self, use_resources: List[int]
      , coro: Coroutine[Any, Any, Tuple[Optional[List[int]], Any]], future: Future[Any]) -> int:
    pos = self.next_add
    self.number_to_resource_coro_future[pos] = use_resources, coro, future
    self.next_add += 1
    self.sum_resources = [x + y for x, y in zip(self.sum_resources, use_resources)]
    return pos

  def pop(self) -> Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[List[int]], Any]], Future[Any]]]:
    while self.next_run < self.next_add:
      val = self.number_to_resource_coro_future.pop(self.next_run, None)
      self.next_run += 1
      if val is not None:
        self.sum_resources = [x - y for x, y in zip(self.sum_resources, val[0])]
        return val
    return None

  def peek(self) -> Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[List[int]], Any]], Future[Any]]]:
    while self.next_run < self.next_add:
      val = self.number_to_resource_coro_future.get(self.next_run)
      if val is not None:
        return val
      self.next_run += 1
    return None
  
  def cancel(self, number: int) -> Optional[Tuple[List[int], Coroutine[Any, Any, Tuple[Optional[List[int]], Any]], Future[Any], bool]]:
    val = self.number_to_resource_coro_future.pop(number, None)
    if val is None:
      return None
    self.sum_resources = [x - y for x, y in zip(self.sum_resources, val[0])]
    is_next_pop = len(self.number_to_resource_coro_future) == 0 or number < min([n for n in self.number_to_resource_coro_future.keys()])
    return (*val, is_next_pop)
