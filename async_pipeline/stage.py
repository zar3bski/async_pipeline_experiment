from abc import ABC, abstractmethod
from asyncio.queues import Queue
from typing import Any
from async_pipeline import tasks
import asyncio


class PipelineStage(ABC):
    """
    Every pipeline stage
      - takes its jobs from an input queue
      - deliver them to N target queues
    """

    target_qs: list
    input_q: Queue
    task_id: str
    _operation: str

    def __init__(self, input_q: Queue, target_qs: list) -> None:
        self.input_q = input_q
        self.target_qs = target_qs

    async def send_objects_to_target_queues(self, outp):
        """
        Send processed data to the stage's target queues from
        """
        for target_q in self.target_qs or []:
            print(f"{self.task_id}: sending {outp}")
            await target_q.put(outp)
            self.input_q.task_done()

    async def __call__(self, param: Any) -> Any:
        """
        Pipeline stage execution
        """
        print(f"{self.task_id}: Initialised with param: {param}")

        while True:
            inpt = await self.input_q.get()
            print(
                f"{self.task_id}: Creating task with {self.task_id}_inner, input {inpt}."
            )
            operation = getattr(self, self._operation)
            tasks.append(asyncio.create_task(operation(inpt)))
