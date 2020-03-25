import asyncio
import asynctnt


def get_push_iterator(connection):
    fut = connection.call("infinite_push_loop", push_subscribe=True)
    return fut, asynctnt.PushIterator(fut)


async def main():
    async with asynctnt.Connection(port=3301) as conn:

        fut, it = get_push_iterator(conn)
        transport_id = id(conn._transport)

        while True:
            current_transport_id = id(conn._transport)
            if current_transport_id != transport_id:
                transport_id = current_transport_id
                fut, it = get_push_iterator(conn)

            try:
                result = await it.__anext__()
                # result = await asyncio.wait_for(it.__anext__(), timeout=10)
                print(result)
            except asyncio.TimeoutError:
                print('timeout')
                pass
            except Exception as e:
                # res = await fut
                # print(res)
                print(e)
                return


if __name__ == "__main__":
    asyncio.run(main())
