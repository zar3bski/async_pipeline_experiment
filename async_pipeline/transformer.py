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
    async def i2a(self, message: str):
        await asyncio.sleep(random.randint(1, 2))  # simulated IO delay
        return message.replace("i", "a")
