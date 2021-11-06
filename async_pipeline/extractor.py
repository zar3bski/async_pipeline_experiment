from abc import ABC
import asyncio
import aiofiles
from async_pipeline.stage import PipelineStage
from asyncio.queues import Queue
from async_pipeline import tasks


class Extractor(PipelineStage):
    task_id: str = "extractor"

    async def _func_inner(self, input_q, target_qs, inpt):
        print(f"{self.task_id}: Recieved input: {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        async with aiofiles.open(inpt, mode="r") as f:
            outp: str = await f.read()
            for target_q in target_qs or []:
                print(f"{self.task_id}: extract sending {outp}")
                await target_q.put(outp)
            input_q.task_done()

    async def extract(self, param):
        """
        Lecture des Fichiers
        """
        print(f"{self.task_id}: Initialised with param: {param}")

        while True:
            inpt = await self.input_q.get()
            print(
                f"{self.task_id}: Creating task with {self.task_id}_inner, input {inpt}."
            )
            tasks.append(
                asyncio.create_task(
                    self._func_inner(self.input_q, self.target_qs, inpt)
                )
            )
