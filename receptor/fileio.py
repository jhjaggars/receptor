import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

pool = ThreadPoolExecutor()
loop = asyncio.get_event_loop()
run_in_executor = partial(loop.run_in_executor, pool)


async def aopen(path, mode):
    fp = await run_in_executor(open, path, mode)
    op = Opened(path, mode)
    op._fp = fp
    return op


class Opened:
    def __init__(self, name, mode):
        self.name = name
        self.mode = mode
        self._fp = None

    async def __aenter__(self):
        self._fp = await run_in_executor(open, self.name, self.mode)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()

    async def read(self, size=-1):
        return await run_in_executor(self._fp.read, size)

    async def write(self, data):
        return await run_in_executor(self._fp.write, data)

    def close(self):
        self._fp.close()
