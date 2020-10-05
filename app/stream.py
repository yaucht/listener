from asyncio import Queue
from typing import List

import nsq
from tornado.ioloop import IOLoop


class NSQMessagesStream:
    def __init__(self, nsqlookupd_addresses: List, topic: str, channel: str):
        self.reader = nsq.Reader(topic,
                                 channel,
                                 message_handler=self._handle_message,
                                 lookupd_poll_interval=5,
                                 lookupd_http_addresses=nsqlookupd_addresses)
        self.mediator = Queue()

        # Or client will have to wait `lookupd_pool_interval`
        # seconds after the service start.
        IOLoop.current().add_callback(self.reader.query_lookupd)

    def _handle_message(self, message: nsq.Message):
        self.mediator.put_nowait(message.body)
        message.finish()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.mediator.get()
