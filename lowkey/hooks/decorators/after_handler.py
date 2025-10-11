import functools
import inspect
from typing import Callable


def after_handler(func: Callable) -> Callable:
    """
    Turns an 'after' function into a decorator (or decorator factory).

    - If you write:

        @after_handler
        def log(context): ...

      you can then use `@log` on a handler.

    - If you write:

        @after_handler
        def save_raw_html(storage, context): ...

      you can then use `@save_raw_html(storage)` on a handler.
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
                result = handler(*h_args, **h_kwargs)
                # Expect context as first arg, but also support kw 'context'
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                func(context)  # bare form expects only `context`
                return result

            @functools.wraps(handler)
            async def wrap_async(*h_args, **h_kwargs):
                result = await handler(*h_args, **h_kwargs)
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                if is_func_async:
                    await func(context)
                else:
                    func(context)
                return result

            return wrap_async if is_handler_async else wrap_sync

        # Parameterized usage: @save_raw_html(storage, ...)
        def decorator(handler: Callable):
            is_handler_async = inspect.iscoroutinefunction(handler)

            @functools.wraps(handler)
            def wrap_sync(*h_args, **h_kwargs):
                result = handler(*h_args, **h_kwargs)
                context = h_kwargs.get("context", h_args[0] if h_args else None)
                if context is None:
                    raise TypeError(
                        "Handler must receive 'context' as first positional or 'context=' kwarg."
                    )
                # Call func with provided bound args, plus context
                try:
                    func(*bound_args, **bound_kwargs, context=context)
                except TypeError:
                    # If the 'after' function expects context positionally
                    func(*bound_args, context, **bound_kwargs)
                return result

            @functools.wraps(handler)
            async def wrap_async(*h_args, **h_kwargs):
                result = await handler(*h_args, **h_kwargs)
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
                return result

            return wrap_async if is_handler_async else wrap_sync

        return decorator

    return decorator_factory
