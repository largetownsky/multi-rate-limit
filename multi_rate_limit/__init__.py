"""Package for using multiple resources while observing multiple RateLimits.
"""
from multi_rate_limit.rate_limit import RateLimit, SecondRateLimit, MinuteRateLimit, HourRateLimit, DayRateLimit
from multi_rate_limit.rate_limit import ResourceOverwriteError
from multi_rate_limit.rate_limit import FilePastResourceQueue, IPastResourceQueue
from multi_rate_limit.multi_rate_limit import MultiRateLimit, RateLimitStats, ReservationTicket

__all__ = [
  "RateLimit",
  "SecondRateLimit",
  "MinuteRateLimit",
  "HourRateLimit",
  "DayRateLimit",
  "ResourceOverwriteError",
  "FilePastResourceQueue",
  "IPastResourceQueue",
  "MultiRateLimit",
  "RateLimitStats",
  "ReservationTicket",
]

__copyright__    = 'Copyright 2023-present largetownsky'
__version__      = '0.2.0'
__license__      = 'MIT'
__author__       = 'largetownsky'
__author_email__ = 'large.town.sky@gmail.com'
__url__          = 'https://github.com/largetownsky/multi-rate-limit'
