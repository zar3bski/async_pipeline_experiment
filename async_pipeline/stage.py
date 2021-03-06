import logging
from abc import ABC
from asyncio.queues import Queue
from typing import Any, Generator
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

    def __init__(self, input_q: Queue, target_qs: list, worker_nb: int = 1) -> None:
        self.input_q = input_q
        self.target_qs = target_qs
        self.tasks = [
            asyncio.create_task(self._perform_tasks(i)) for i in range(worker_nb)
        ]

    def __del__(self):
        for task in self.tasks:
            task.cancel()
            logger.info(f"{self.stage_name}: deleting worker")

    async def _send_objects_to_target_queues(self, outp: Any):
        """
        Send processed data to stage's target queues
        Input: Any item or list/generator of items (which will be looped through)
        """

        def _send_target(item):
            for target_q in self.target_qs or []:
                target_q.put_nowait(item)
                logger.debug(
                    f"{self.stage_name}=OUT=>[{id(target_q)}:{target_q.qsize()}]"
                )

        if isinstance(outp, list) or isinstance(outp, Generator):
            for item in outp:
                _send_target(item)
        else:
            _send_target(outp)

    async def _perform_tasks(self, worker_id: int):
        logger.info(f"{self.stage_name} worker {worker_id}: initialized")
        while True:
            input = await self.input_q.get()
            logger.debug(
                f"[{id(self.input_q)}:{self.input_q.qsize()}]=IN=>{self.stage_name} worker {worker_id}: {str(input)}"
            )
            operation = getattr(self, self._operation)
            try:
                await operation(input)
            except Exception as e: 
                logger.error(f"[{id(self.input_q)}:{self.input_q.qsize()}]=IN=>{self.stage_name} worker {worker_id}: {e}")
            self.input_q.task_done()


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
    async def wrapper_pipeline_operation(self: PipelineStage, input, *args, **kwargs):
        out = await func(self, input, *args, **kwargs)
        if isinstance(out, Exception):
            logger.warning(f"{self.stage_name}: exception {str(out)}")
        else:
            await self._send_objects_to_target_queues(out)

    return wrapper_pipeline_operation
