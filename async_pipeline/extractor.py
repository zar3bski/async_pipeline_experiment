import asyncio
import aiofiles
from async_pipeline.stage import PipelineStage
from asyncio.queues import Queue
from async_pipeline import tasks


class Extractor(PipelineStage):
    task_id: str = "extractor"
    target_qs: list
    input_q: Queue

    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["extract"]
        super().__init__(*args, **kwargs)

    async def read_file(self, inpt):
        print(f"{self.task_id}: Recieved input: {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        async with aiofiles.open(inpt, mode="r") as f:
            outp: str = await f.read()
        await self.send_objects_to_target_queues(outp)
