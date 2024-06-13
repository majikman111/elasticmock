# -*- coding: utf-8 -*-
import typing as t
import functools
from elastic_transport import ApiResponseMeta, ObjectApiResponse


def for_all_methods(decorators, apply_on_public_only=True):
    def decorate(cls):
        for attr in cls.__dict__:

            if apply_on_public_only:
                if attr.startswith('_'):
                    continue

            if callable(getattr(cls, attr)):
                for decorator in decorators:
                    setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate


def wrap_object_api_response(func):
    @functools.wraps(func)
    def wrapper(*args: t.Any, **kwargs: t.Any):
        return ObjectApiResponse(
            body=func(*args, **kwargs),
            meta=ApiResponseMeta(200, "HTTP/1.1", {}, 1, None),
        )
    return wrapper
