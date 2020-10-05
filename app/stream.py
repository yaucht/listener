from asyncio import Queue
from typing import List

import nsq


class NSQMessagesStream:
    def __init__(self, nsqlookupd_addresses: List, topic: str, channel: str):
        self.reader = nsq.Reader(topic,
                                 channel,
                                 message_handler=self._handle_message,
                                 lookupd_http_addresses=nsqlookupd_addresses)
        self.mediator = Queue()

    def _handle_message(self, message: nsq.Message):
        self.mediator.put_nowait(message.body)
        message.finish()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.mediator.get()
