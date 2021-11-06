from async_pipeline import tasks
import asyncio

from async_pipeline.stage import PipelineStage


class Loader(PipelineStage):
    def __init__(self, conf, *args, **kwargs) -> None:
        self._operation = conf["load"]
        super().__init__(*args, **kwargs)

    async def print(self, message: str):
        print(
            f"""
        got {message} from god knows where
        """
        )
        await self.send_objects_to_target_queues(None)
