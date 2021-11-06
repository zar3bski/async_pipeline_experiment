import asyncio
from asyncio.queues import Queue
from async_pipeline import tasks

async def transform(input_q:Queue, target_qs:list, task_id, param):
    """
    Transformation des chaines
    """
    print(f"{task_id}: Initialised with param: {param}")

    async def func_inner(input_q, target_qs, inpt:str):
        print(f"{task_id}: Recieved input: {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt.replace("i", "a")
        for target_q in target_qs or []:
            print(f"{task_id}: transform sending {outp}")
            await target_q.put(outp)
        input_q.task_done()

    while True:
        inpt = await input_q.get()
        print(f'{task_id}: Creating task with {task_id}_inner, input {inpt}.')
        tasks.append(asyncio.create_task(func_inner(input_q, target_qs, inpt)))