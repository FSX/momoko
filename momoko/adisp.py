# -*- coding:utf-8 -*-
'''
Adisp is a library that allows structuring code with asynchronous calls and
callbacks without defining callbacks as separate functions. The code then
becomes sequential and easy to read. The library is not a framework by itself
and can be used in other environments that provides asynchronous working model
(see an example with Tornado server in proxy_example.py).

Usage:

## Organizing calling code

All the magic is done with Python 2.5 decorators that allow for control flow to
leave a function, do sometihing else for some time and then return into the
calling function with a result. So the function that makes asynchronous calls
should look like this:

    @process
    def my_handler():
        response = yield some_async_func()
        data = parse_response(response)
        result = yield some_other_async_func(data)
        store_result(result)

Each `yield` is where the function returns and lets the framework around it to
do its job. And the code after `yield` is what usually goes in a callback.

The @process decorator is needed around such a function. It makes it callable
as an ordinary function and takes care of dispatching callback calls back into
it.

## Writing asynchronous function

In the example above functions "some_async_func" and "some_other_async_func"
are those that actually run an asynchronous process. They should follow two
conditions:

- accept a "callback" parameter with a callback function that they should call
  after an asynchronous process is finished
- a callback should be called with one parameter -- the result
- be wrapped in the @async decorator

The @async decorator makes a function call lazy allowing the @process that
calls it to provide a callback to call.

Using async with @-syntax is most convenient when you write your own
asynchronous function (and can make your callback parameter to be named
"callback"). But when you want to call some library function you can wrap it in
async in place.

    # call http.fetch(url, callback=callback)
    result = yield async(http.fetch)

    # call http.fetch(url, cb=safewrap(callback))
    result = yield async(http.fetch, cbname='cb', cbwrapper=safewrap)(url)

Here you can use two optional parameters for async:

- `cbname`: a name of a parameter in which the function expects callbacks
- `cbwrapper`: a wrapper for the callback iself that will be applied before
  calling it

## Chain calls

@async function can also be @process'es allowing to effectively chain
asynchronous calls as it can be done with normal functions. In this case the
@async decorator shuold be the outer one:

    @async
    @process
    def async_calling_other_asyncs(arg, callback):
        # ....

## Multiple asynchronous calls

The library also allows to call multiple asynchronous functions in parallel and
get all their result for processing at once:

    @async
    def async_http_get(url, callback):
        # get url asynchronously
        # call callback(response) at the end

    @process
    def get_stat():
        urls = ['http://.../', 'http://.../', ... ]
        responses = yield map(async_http_get, urls)

After *all* the asynchronous calls will complete `responses` will be a list of
responses corresponding to given urls.
'''
from functools import partial
from tornado.ioloop import IOLoop

class CallbackDispatcher(object):
    def __init__(self, generator):
        self.io_loop = IOLoop.instance()
        self.g = generator
        try:
            self.call(self.g.next())
        except StopIteration:
            pass

    def _queue_send_result(self, result, single):
        self.io_loop.add_callback(partial(self._send_result, result, single))

    def _send_result(self, results, single):
        try:
            result = results[0] if single else results
            if isinstance(result, Exception):
                self.call(self.g.throw(result))
            else:
                self.call(self.g.send(result))
        except StopIteration:
            pass

    def call(self, callers):
        single = not hasattr(callers, '__iter__')
        if single:
            callers = [callers]
        self.call_count = len(list(callers))
        results = [None] * self.call_count
        if self.call_count == 0:
            self._queue_send_result(results, single)
        else:
            for count, caller in enumerate(callers):
                caller(callback=partial(self.callback, results, count, single))

    def callback(self, results, index, single, arg):
        self.call_count -= 1
        results[index] = arg
        if self.call_count > 0:
            return
        self._queue_send_result(results, single)

def process(func):
    def wrapper(*args, **kwargs):
        CallbackDispatcher(func(*args, **kwargs))
    return wrapper

def async(func, cbname='callback', cbwrapper=lambda x: x):
    def wrapper(*args, **kwargs):
        def caller(callback):
            kwargs[cbname] = cbwrapper(callback)
            return func(*args, **kwargs)
        return caller
    return wrapper
