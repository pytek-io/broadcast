import asyncio
from itertools import count

import pytest

from mpmc import Broadcast


@pytest.mark.asyncio
async def test_queue():
    queue = Broadcast()
    total_sum = 0
    nb_items = 10
    nb_readers = 3

    async def read():
        nonlocal total_sum
        index = count()
        async for i in queue:
            await asyncio.sleep(0.2)
            assert i == next(index)
            total_sum += i

    tasks = [asyncio.create_task(read()) for _ in range(nb_readers)]
    for i in range(nb_items):
        await asyncio.sleep(0.1)
        queue.put_nowait(i)
    queue.close()
    await asyncio.gather(*tasks)
    assert total_sum == ((nb_items - 1) * nb_items / 2) * nb_readers
