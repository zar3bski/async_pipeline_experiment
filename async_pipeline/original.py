"""
Example program for basic asyncio pipeline.
Program takes string as input and converts it to upper case.
For sake of simplicity missing some "features", most notably error handling is absent.
Errors will silenty prevent program completion in many cases.
"""
import asyncio
import random
from dataclasses import dataclass


@dataclass()
class DataAB:
    letter: str


@dataclass()
class DataBC:
    letter: str
    upper: str


result = ""


async def do_stepA(queue_out, input):
    for letter in input:
        print(f"A, sending {letter}")
        await asyncio.sleep(random.randint(1, 2))  # simulated IO delay
        await queue_out.put(DataAB(letter))


async def do_stepB(queue_in, queue_out):
    while True:
        data: DataAB = await queue_in.get()

        # perform actual step
        letter = data.letter
        upper = letter.upper()
        await asyncio.sleep(random.randint(1, 2))  # simulated IO delay
        print(f"B, processed {upper}")

        await queue_out.put(DataBC(letter, upper))

        queue_in.task_done()


async def do_stepC(queue_in):
    global result
    while True:
        data: DataBC = await queue_in.get()

        # perform actual step
        letter = data.letter
        upper = data.upper
        print(f"C, {letter} changed to {upper}")

        result += upper
        print(f"intermediate result : {result}")

        queue_in.task_done()


async def main():
    tasks = []
    pipeline_in = "hello world"

    print(f"converting to upper case: {pipeline_in}")

    queue_AB = asyncio.Queue()
    queue_BC = asyncio.Queue()

    stepA = asyncio.create_task(do_stepA(queue_AB, pipeline_in))

    tasks.extend(
        [
            asyncio.create_task(do_stepB(queue_AB, queue_BC)),
            asyncio.create_task(do_stepC(queue_BC)),
        ]
    )

    await stepA
    
    print('step A done')

    await queue_AB.join()
    print('queue A - B done')
    

    await queue_BC.join()

    await asyncio.gather(*tasks)

    print(f"main complete, result: {result}")


asyncio.run(main())
print("program complete")
