import asyncio

import random
from asyncio.queues import Queue
from async_pipeline.stage import PipelineStage, pipeline_operation
from async_pipeline import tasks


class Transformer(PipelineStage):
    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["transform"]
        super().__init__(*args, **kwargs)

    @pipeline_operation
    async def i2a(self, inpt):
        await asyncio.sleep(random.randint(1, 5))  # simulated IO delay
        outp = inpt.replace("\n", " ").replace("i", "a") + "some_padding string"
        return outp

