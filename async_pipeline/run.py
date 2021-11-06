import asyncio
import time
from os import listdir, path
from async_pipeline import tasks
from async_pipeline.extractor import extract
from async_pipeline.transformer import transform
from async_pipeline.loader import load


async def main():
    to_read = asyncio.Queue()
    to_transform = asyncio.Queue()
    to_load = asyncio.Queue()

    extract_task = extract(to_read, [to_transform], "extract", "")
    asyncio.create_task(extract_task)

    transform_task = transform(to_transform, [to_load], "transform", "")
    asyncio.create_task(transform_task)

    load_task = load(to_load, [], "load", "")
    asyncio.create_task(load_task)

    start_time = time.time()

    for filename in listdir("tests/data/set_1"):
        await to_read.put(f"tests/data/set_1/{filename}")

    await to_read.join()
    await to_transform.join()
    await to_load.join()
    
    await asyncio.gather(*tasks)
    print(f"Duration: {time.time() - start_time}")

if __name__ == '__main__':
    asyncio.run(main())