import asyncio
import random
import aiofiles
from async_pipeline.stage import PipelineStage, pipeline_operation
from asyncio.queues import Queue

class Extractor(PipelineStage):
    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["extract"]
        super().__init__(*args, **kwargs)

    @pipeline_operation
    async def read_file(self, inpt):
        await asyncio.sleep(random.randint(1, 8))  # simulated IO delay
        async with aiofiles.open(inpt, mode="r") as f:
            outp: str = await f.read()
        return outp