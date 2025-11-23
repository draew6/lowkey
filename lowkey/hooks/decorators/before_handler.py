import functools
import inspect
from typing import Callable


def before_handler(func: Callable) -> Callable:
    """
    Turns a 'before' function into a decorator (or decorator factory).

    - If you write:

        @before_handler
        def log(context): ...

      you can then use `@log` on a handler.

    - If you write:

        @before_handler
        def validate(schema, context): ...

      you can then use `@validate(schema)` on a handler.
    """
    is_func_async = inspect.iscoroutinefunction(func)

    def decorator_factory(*bound_args, **bound_kwargs):
        # Support bare usage: @log   (i.e. called with the handler directly)
        if (
            bound_args
            and callable(bound_args[0])
            and len(bound_args) == 1
            and not bound_kwargs
        ):
            handler = bound_args[0]
            is_handler_async = inspect.iscoroutinefunction(handler)

            @functools.wraps(handler)
            def wrap_sync(*h_args, **h_kwargs):
                # Expect context as first arg, but also support kw 'context'
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                # bare form expects only `context`
                func(context) if not is_func_async else None  # cannot await in sync
                # If the before hook is async but handler is sync, run it to completion via a runtime if needed.
                if is_func_async:
                    raise RuntimeError(
                        "Async 'before' function used with sync handler. "
                        "Make your handler async or provide a sync 'before' hook."
                    )
                return handler(*h_args, **h_kwargs)

            @functools.wraps(handler)
            async def wrap_async(*h_args, **h_kwargs):
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                if is_func_async:
                    await func(context)
                else:
                    func(context)
                return await handler(*h_args, **h_kwargs)

            return wrap_async if is_handler_async else wrap_sync

        # Parameterized usage: @validate(schema, ...)
        def decorator(handler: Callable):
            is_handler_async = inspect.iscoroutinefunction(handler)

            @functools.wraps(handler)
            def wrap_sync(*h_args, **h_kwargs):
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                if is_func_async:
                    raise RuntimeError(
                        "Async 'before' function used with sync handler. "
                        "Make your handler async or provide a sync 'before' hook."
                    )
                # Call func with provided bound args, plus context
                try:
                    func(*bound_args, **bound_kwargs, context=context)
                except TypeError:
                    # If the 'before' function expects context positionally
                    func(*bound_args, context, **bound_kwargs)
                return handler(*h_args, **h_kwargs)

            @functools.wraps(handler)
            async def wrap_async(*h_args, **h_kwargs):
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                if is_func_async:
                    try:
                        await func(*bound_args, **bound_kwargs, context=context)
                    except TypeError:
                        await func(*bound_args, context, **bound_kwargs)
                else:
                    try:
                        func(*bound_args, **bound_kwargs, context=context)
                    except TypeError:
                        func(*bound_args, context, **bound_kwargs)
                return await handler(*h_args, **h_kwargs)

            return wrap_async if is_handler_async else wrap_sync

        return decorator

    return decorator_factory
