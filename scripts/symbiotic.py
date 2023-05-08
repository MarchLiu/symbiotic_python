# prototype server

import sys
import asyncpg
import asyncpg_listen
import asyncio

port = int(sys.argv[1])
database_name = sys.argv[2]
channel = sys.argv[3]

flag = True


def create_handle(conn):
    async def handle_message(message: asyncpg_listen.NotificationOrTimeout) -> None:
        global flag
        if isinstance(message, asyncpg_listen.Timeout):
            return

        await conn.execute("insert into symbiotic.log(content) values($1)", message.payload)
        if message.payload == "shutdown":
            flag = False

    return handle_message


async def runner():
    conn = await asyncpg.connect(database=database_name, host='127.0.0.1', port=port)
    listener = asyncpg_listen.NotificationListener(
        asyncpg_listen.connect_func(database=database_name, host='127.0.0.1', port=port))
    listener_task = asyncio.create_task(
        listener.run(
            {channel: create_handle(conn)},
            policy=asyncpg_listen.ListenPolicy.LAST,
            notification_timeout=5
        )
    )

    while flag:
        await asyncio.sleep(1)

    listener_task.cancel()
    await conn.close()


def main():
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(runner())
    loop.close()


if __name__ == "__main__":
    main()
