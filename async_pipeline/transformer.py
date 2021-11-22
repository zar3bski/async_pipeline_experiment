import asyncio

import random
from asyncio.queues import Queue
from typing import Generator
from async_pipeline.stage import PipelineStage, pipeline_operation


class Transformer(PipelineStage):
    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["transform"]
        super().__init__(*args, **kwargs)

    @pipeline_operation
    async def i2a(self, inpt) -> Generator:
        await asyncio.sleep(random.randint(1, 5))  # simulated IO delay
        return (
            line.replace("i", "I").replace("a", "A") + " some_padding"
            for line in inpt.split("\n")
        )
