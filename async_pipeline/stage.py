from asyncio.queues import Queue


class PipelineStage:
    """
    Every pipeline stage
      - takes its jobs from an input queue
      - deliver them to N target queues
    """

    def __init__(self, input_q: Queue, target_qs: list) -> None:
        self.input_q = input_q
        self.target_qs = target_qs
