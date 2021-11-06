from async_pipeline import tasks
import asyncio

from async_pipeline.stage import PipelineStage


class Loader(PipelineStage):
    task_id: str = "load"

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


async def load(input_q, target_qs, task_id, param):
    print(f"{task_id}: Initialised with param: {param}")

    async def func_inner(input_q, target_qs, inpt):
        print(f"{task_id}: Recieved input: {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        for target_q in target_qs or []:
            print(f"{task_id}: load sending {outp}")
            await target_q.put(outp)
        input_q.task_done()

    while True:
        inpt = await input_q.get()
        print(f"{task_id}: Creating task with {task_id}_inner, input {inpt}.")
        tasks.append(asyncio.create_task(func_inner(input_q, target_qs, inpt)))
