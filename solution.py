import sys
import asyncio
from multiprocessing.connection import Client

AWAIT_TIME = 0.1


async def msg2server(msg, conn):
    while True:
        conn.send(('send', msg))
        res = conn.recv()
        if res[0] == 'succ':
            break
        if res[0] == 'fail':
            await asyncio.sleep(AWAIT_TIME)


async def file2server(fs, conn):
    for line in fs:
        await msg2server(line, conn)
    await msg2server(None, conn)


async def server2file(ft, conn):
    while True:
        conn.send(('get',))
        res = conn.recv()
        if res[0] == 'succ':
            if res[1] is None:
                break
            ft.write(res[1])
        if res[0] == 'fail':
            await asyncio.sleep(AWAIT_TIME)


if __name__ == '__main__':
    address = ('localhost', 6000)
    source, target = sys.argv[1], sys.argv[2]

    with open(source, 'r') as fs, \
            open(target, 'w') as ft, \
            Client(address) as conn:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            asyncio.gather(file2server(fs, conn),
                           server2file(ft, conn))
        )
        loop.close()

        conn.send(('close',))