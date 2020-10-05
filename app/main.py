import asyncio
from os import environ
from typing import List

import socketio
from fastapi import FastAPI

from .util import authorize

from .stream import NSQMessagesStream


def get_nsqlookupd_adresses() -> List[str]:
    lookupd_addresses = []
    for key, value in environ.items():
        if key.startswith('NSQLOOKUPD_ADDR'):
            lookupd_addresses.append(value)
    return lookupd_addresses


new_messages = NSQMessagesStream(get_nsqlookupd_adresses(),
                                 topic='messages',
                                 channel='listener')

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')


@sio.event
async def connect(sid, wsgi_environ):
    if not authorize(wsgi_environ):
        return False


async def publisher():
    # TODO: Why `async for` doesn't work?
    it = await new_messages.__aiter__()
    while 1:
        text = (await it.__anext__()).decode('utf-8')
        asyncio.create_task(sio.emit('message', text))


app = FastAPI()


@app.on_event('startup')
async def startup():
    asyncio.create_task(publisher())


app.mount('/', socketio.ASGIApp(sio, other_asgi_app=app))
