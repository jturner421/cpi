import functools
import time
from typing import Callable, Any, Optional
import asyncio
import aiohttp
from aiohttp import ClientSession
from colorama import Fore, Style


def async_timed():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args, **kwargs) -> Any:
            print(f'starting {func} with args {args} {kwargs}')
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.time()
                total = end - start
                print(f'finished {func} in {total:.4f} second(s)')

        return wrapped

    return wrapper


async def delay(delay_seconds: int) -> int:
    print(f'sleeping for {delay_seconds} second(s)')
    await asyncio.sleep(delay_seconds)
    print(f'finished sleeping for {delay_seconds} second(s)')
    return delay_seconds


async def get(session: ClientSession, url: str, params: Optional = None, headers: Optional = None) -> int:
    to = aiohttp.ClientTimeout(total=5 * 60)
    caseid = url.split('/')[-1]
    print(Fore.YELLOW + f'Getting docket entries for case {caseid}...', flush=True)
    if headers:
        async with session.get(url, timeout=to, params=params, headers=headers) as result:
            return result.status
    else:
        async with session.get(url, timeout=to, params=params) as result:
            return result.status
