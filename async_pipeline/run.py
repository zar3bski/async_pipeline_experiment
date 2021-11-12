import asyncio
import logging
import time
from os import listdir, path
from async_pipeline.extractor import Extractor
from async_pipeline.transformer import Transformer
from async_pipeline.loader import Loader


async def main():
    logging.basicConfig(level=logging.DEBUG)
    # dumbest conf ever: which function to use for each stage
    # easily scalable
    conf = {"extract": "read_file", "transform": "i2a", "load": "print"}
    to_read = asyncio.Queue()
    to_transform = asyncio.Queue()
    to_load = asyncio.Queue()

    extractor = Extractor(conf, to_read, [to_transform])
    transformer = Transformer(conf, to_transform, [to_load], worker_nb=2)
    loader = Loader(conf, to_load, [])

    start_time = time.time()

    for filename in listdir("tests/data/set_1"):
        await to_read.put(f"tests/data/set_1/{filename}")

    await to_read.join()
    await to_transform.join()
    await to_load.join()

    del extractor
    del transformer
    del loader
    
    print(f"Duration: {time.time() - start_time}")


if __name__ == "__main__":
    asyncio.run(main())
