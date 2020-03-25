import asyncio

cimport cython

import asynctnt

cdef class PushIterator:
    def __init__(self, fut: asyncio.Future):
        """
            Creates PushIterator object. In order to receive push notifications
            this iterator must be created.

            Example:

            .. code-block:: pycon

                # tarantool function:
                # function f()
                #     box.session.push('hello')
                #     return 'finished'
                # end

                >>> fut = conn.call('async_function', push_subscribe=True)
                >>> async for item in PushIterator(fut):
                ...     print(item)


            :param fut: Future object returned from call_async, eval_sync
                        functions
            :type fut: asyncio.Future
        """
        cdef:
            Request request
        if not hasattr(fut, '_req'):
            raise ValueError('Future is invalid. Make sure to call with '
                             'a future returned from a method with '
                             'push_subscribe=True flag')

        request = <Request>fut._req

        if not request.push_subscribe:
            raise ValueError('Future is invalid. Make sure to call with '
                             'a future returned from a method with '
                             'push_subscribe=True flag')

        self._fut = fut
        self._request = request
        self._response = request.response

    def __iter__(self):
        raise RuntimeError('Cannot use iter with PushIterator - use aiter')

    def __next__(self):
        raise RuntimeError('Cannot use next with PushIterator - use anext')

    def __aiter__(self):
        return self

    @cython.iterable_coroutine
    async def __anext__(self):
        cdef Response response
        response = self._response

        if response.push_len() == 0 and response._code >= 0:
            # no more data left
            raise StopAsyncIteration

        if response.push_len() > 0:
            return response.pop_push()

        ev = response._push_event
        await ev.wait()

        exc = response.get_exception()
        if exc is not None:
            # someone needs to await the underlying future
            # so we do it here, and most probably (like 99%) the exception
            # that happened is already in the self._fut. So, await-ing it
            # would cause an exception to be thrown.
            # But if it doesnt't throw we still await the future and throw
            # the exception ourselves.
            await self._fut
            raise exc

        if response.push_len() > 0:
            return response.pop_push()

        if response._code >= 0:
            ev.clear()
            raise StopAsyncIteration

        assert False, 'Impossible condition happened. ' \
                      'Please file a bug to ' \
                      'https://github.com/igorcoding/asynctnt'

    @property
    def response(self):
        """
            Return current Response object. Might be handy to know if the
            request is finished already while iterating over the PushIterator

            :rtype: asynctnt.Response
        """
        return self._response
