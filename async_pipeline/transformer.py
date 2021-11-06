import asyncio
from asyncio.queues import Queue
from async_pipeline.stage import PipelineStage
from async_pipeline import tasks


class Transformer(PipelineStage):
    task_id: str = "tranformation"

    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["transform"]
        super().__init__(*args, **kwargs)

    async def i2a(self, message: str):
        print(f"{self.task_id}: Recieved input: {message}")
        await asyncio.sleep(1)  # simulated IO delay
        outp = message.replace("i", "a")
        await self.send_objects_to_target_queues(outp)
