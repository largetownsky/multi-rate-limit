multi-rate-limit
================

[![PyPI](https://img.shields.io/pypi/v/multi-rate-limit.svg)](https://pypi.python.org/pypi/multi-rate-limit)
[![CI badge](https://github.com/largetownsky/multi-rate-limit/actions/workflows/python-package.yml/badge.svg)](https://github.com/largetownsky/multi-rate-limit/actions)
![Tests](https://raw.githubusercontent.com/largetownsky/multi-rate-limit/main/tests.svg)
![Code coverage](https://raw.githubusercontent.com/largetownsky/multi-rate-limit/main/coverage.svg)
[![Python versions](https://img.shields.io/pypi/pyversions/multi-rate-limit.svg)](https://github.com/largetownsky/multi-rate-limit)


[multi-rate-limit](https://largetownsky.github.io/multi-rate-limit/) is a package for using multiple resources while observing multiple RateLimits.

![multi-rate-limit image](https://raw.githubusercontent.com/largetownsky/multi-rate-limit/main/multi-rate-limit.png)

# Install

```
pip install multi-rate-limit
```
or
```
poetry add multi-rate-limit
```

# How to use

## Simple example

```py:main.py
import asyncio
import time

from multi_rate_limit import MultiRateLimit, RateLimit, FilePastResourceQueue

async def work(name: str, time_required: float):
  print(f'Start {name} at {time.time()}')
  await asyncio.sleep(time_required)
  print(f'End {name} at {time.time()}')
  # Must return a tuple with 2 elements.
  # You can overwrite resource usage information with the 1st element.
  # The 2nd element is the true return value that you want to obtain externally.
  return None, None

async def main():
  # Create MultiRateLimit with 3 RateLimits and 3 max async run.
  # The 1st resource is limited to no more than 3 units per 1s and 10 units in 10s.
  # The 2nd resource is limited to no more than 6 units per 3s.
  mrl = await MultiRateLimit.create([[RateLimit(3, 1), RateLimit(10, 10)], [RateLimit(6, 3)]],
      None, 3)
  ticket1 = mrl.reserve([1, 3], work('1', 1))
  ticket2 = mrl.reserve([1, 3], work('2', 1))
  ticket3 = mrl.reserve([1, 1], work('3', 1)) # Throttled by the 2nd resource limit of RateLimit(6, 3)
  ticket4 = mrl.reserve([3, 0], work('4', 1)) # Throttled by the 1st resource limit of RateLimit(3, 1)
  ticket5 = mrl.reserve([3, 0], work('5', 1)) # Throttled by the 1st resource limit of RateLimit(3, 1)
  ticket6 = mrl.reserve([3, 0], work('6', 1)) # Throttled by the 1st resource limit of RateLimit(10, 10)
  await asyncio.gather(ticket1.future, ticket2.future, ticket3.future, ticket4.future, ticket5.future, ticket6.future)
  await mrl.term()

asyncio.run(main())
```
The result will be as follows.
```
poetry run python .\main.py
Start 1 at 1702054558.9240694
Start 2 at 1702054558.9240694 <- If there are sufficient resources and number of executions, execute concurrently.
End 1 at 1702054559.929741
End 2 at 1702054559.929741
Start 3 at 1702054562.932087 <- Throttled by the 2nd resource limit of RateLimit(6, 3)
End 3 at 1702054563.9323323
Start 4 at 1702054564.9366117 <- Throttled by the 1st resource limit of RateLimit(3, 1)
End 4 at 1702054565.9401765
Start 5 at 1702054566.941147 <- Throttled by the 1st resource limit of RateLimit(3, 1)
End 5 at 1702054567.9440823
Start 6 at 1702054569.9346018 <- Throttled by the 1st resource limit of RateLimit(10, 10)
End 6 at 1702054570.9466405
```

## How to overwrite resource consumption information

Unless explicitly stated in the return value or exception parameter of coroutine,
the use_resources of this function are considered to have been consumed at the end of coroutine execution.
If you want to change this behavior because you cannot know the exact resource consumption until after execution,
please override the resource consumption timing and amount using coroutine's return value or ResourceOverwriteError parameter.

The return value of coroutine is in the following format.
```
((use_time, [use_resource1, use_resource2,,,]), return_value_to_user)
```
If you do not want to overwrite, please use the followin format.
```
(None, return_value_to_user)
```

If you want to overwrite when you raise a exception.
```
raise ResourceOverwriteError(use_time, [use_resource1, use_resource2,,,], cause_exception)
```
If you do not want to overwrite, simply raise a exception.
```
raise cause_exception
```

## How to reuse resource consumption information

In the simple example above, resource consumption information is managed only in memory and disappears after execution.
If you want to manage long-term consumption, such as when re-executing,
please specify the factory method of the IPastResourceQueue implementation in the second parameter of MultiRateLimit.create().

A simple file-managed IPastResourceQueue implementation is available below.
```py
  # Create MultiRateLimit with 3 RateLimits and 3 max async run.
  # If you do not need to inherit the rate limit information from one execution to another via a file,
  # you can replace the lambda function with None.
  file_name = 'res-log.tsv'
  mrl = await MultiRateLimit.create([[RateLimit(3, 1), RateLimit(10, 10)], [RateLimit(6, 3)]],
      lambda len_resource, longest_period_in_seconds: FilePastResourceQueue.create(
      len_resource, longest_period_in_seconds, file_name),
      3)
```

## How to cancel a coroutine's execution reservation

Only while waiting for execution, you can cancel using the ticket number as shown below.
```py
  ticket1 = mrl.reserve([1, 3], work('1', 1))
  mrl.cancel(ticket1.reserve_number)
```

## How to monitor resource consumption

```py:main.py
import asyncio
import time

from multi_rate_limit import MultiRateLimit, MinuteRateLimit, DayRateLimit, FilePastResourceQueue
from typing import List, Optional

async def work(name: str, time_required: float, overwrite_resources: Optional[List[int]]):
  print(f'Start {name} at {time.time()}')
  await asyncio.sleep(time_required)
  print(f'End {name} at {time.time()}')
  # Must return a tuple with 2 elements.
  # You can overwrite resource usage information with the 1st element.
  # The 2nd element is the true return value that you want to obtain externally.
  if overwrite_resources is None:
    return None, None
  else:
    return (time.time(), overwrite_resources), None

async def print_stats(mrl: MultiRateLimit):
  # Sleep short time to run the internal dispatch task.
  await asyncio.sleep(0.1)
  stats = await mrl.stats()
  print(f'Past resource percentage : {stats.past_use_percents()}')
  print(f'Past + current resource percentage : {stats.current_use_percents()}')
  print(f'Past + current + next resource percentage : {stats.next_use_percents()}')

async def cancel_coroutine_to_avoid_runtime_warning(coro):
  task = asyncio.create_task(coro)
  task.cancel()

async def main():
  # Create MultiRateLimit with 3 RateLimits and 3 max async run.
  mrl = await MultiRateLimit.create([[MinuteRateLimit(3, 0.1), DayRateLimit(100)], [MinuteRateLimit(10)]],
      None,
      3)
  ticket1 = mrl.reserve([1, 3], work('1', 1, None))
  ticket2 = mrl.reserve([3, 2], work('2', 1, [2, 2]))
  _, coro = mrl.cancel(ticket1.reserve_number)
  await cancel_coroutine_to_avoid_runtime_warning(coro)
  await print_stats(mrl)
  ticket3 = mrl.reserve([1, 0], work('3', 1, [0, 1]))
  ticket4 = mrl.reserve([0, 2], work('4', 1, None))
  ticket5 = mrl.reserve([1, 1], work('5', 1, None))
  ticket6 = mrl.reserve([2, 2], work('6', 1, None))
  await ticket2.future
  await print_stats(mrl)
  coros = await mrl.term()
  for coro in coros:
    await cancel_coroutine_to_avoid_runtime_warning(coro)

asyncio.run(main())
```
The result will be as follows.
```
poetry run python .\main.py
Start 2 at 1702059926.158294
Past resource percentage : [[0.0, 0.0], [0.0]]
Past + current resource percentage : [[100.0, 3.0], [20.0]]
Past + current + next resource percentage : [[100.0, 3.0], [20.0]]
End 2 at 1702059927.1678545
Start 3 at 1702059927.1678545
Start 4 at 1702059927.1678545
Past resource percentage : [[66.66666666666667, 2.0], [20.0]]
Past + current resource percentage : [[100.0, 3.0], [40.0]]
Past + current + next resource percentage : [[200.0, 6.0], [70.0]]
End 4 at 1702059928.1696303
End 3 at 1702059928.1696303
```
