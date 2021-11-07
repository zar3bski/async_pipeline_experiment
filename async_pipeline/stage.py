import logging
from abc import ABC, abstractmethod
from asyncio.queues import Queue
from typing import Any
from async_pipeline import tasks
import asyncio
import functools

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """
    Every pipeline stage
      - takes its jobs from an input queue
      - deliver them to N target queues
    """

    target_qs: list
    input_q: Queue
    _operation: str

    @property
    def stage_name(self) -> str:
        return f"{self.__class__.__name__}"

    def __init__(self, input_q: Queue, target_qs: list) -> None:
        self.input_q = input_q
        self.target_qs = target_qs

    async def _send_objects_to_target_queues(self, outp):
        """
        Send processed data to the stage's target queues
        """
        for target_q in self.target_qs or []:
            logger.debug(f"{self.stage_name}: sending {str(outp)}")
            await target_q.put(outp)
        self.input_q.task_done()

    async def __call__(self, param: Any) -> Any:
        """
        Pipeline stage execution
        """
        logger.info(f"{self.stage_name}: Initialised with param: {param}")

        while True:
            inpt = await self.input_q.get()
            logger.debug(
                f"{self.stage_name}: Creating task with {self.stage_name}_inner, input {str(inpt)}."
            )
            operation = getattr(self, self._operation)
            tasks.append(asyncio.create_task(operation(inpt)))


def pipeline_operation(func):
    """
    Decorator for PipelineStage methods. Log inputs and send returned output
    the stage's output queues

    usage:
        @pipeline_operation\n
        async def your_pipeline_stage_method():\n
            return what_to_add_to_append_to_target_queues
    """

    @functools.wraps(func)
    async def wrapper_pipeline_operation(self: PipelineStage, inpt, *args, **kwargs):
        logger.debug(f"{self.stage_name}: recieved input: {str(inpt)}")
        out = await func(self, inpt, *args, **kwargs)
        await self._send_objects_to_target_queues(out)

    return wrapper_pipeline_operation
