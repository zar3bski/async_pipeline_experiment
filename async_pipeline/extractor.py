import asyncio
import aiofiles
from asyncio.queues import Queue
from async_pipeline import tasks

async def extract(input_q:Queue, target_qs:list, task_id, param):
    """
    Lecture des Fichiers
    """
    print(f"{task_id}: Initialised with param: {param}")

    async def func_inner(input_q, target_qs, inpt):
        print(f"{task_id}: Recieved input: {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        async with aiofiles.open(inpt, mode='r') as f:
            outp:str = await f.read() 
            for target_q in target_qs or []:
                print(f"{task_id}: extract sending {outp}")
                await target_q.put(outp)
            input_q.task_done()

    while True:
        inpt = await input_q.get()
        print(f'{task_id}: Creating task with {task_id}_inner, input {inpt}.')
        tasks.append(asyncio.create_task(func_inner(input_q, target_qs, inpt)))

