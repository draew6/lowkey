from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Sequence, get_args, get_origin, Annotated, Union
from types import UnionType
from uuid import UUID

import pandas as pd
from pydantic import BaseModel


def _strip_annotated(tp):
    """If type is Annotated[T, ...], return T."""
    if get_origin(tp) is Annotated:
        args = get_args(tp)
        if args:
            return args[0]
    return tp


def _strip_optional(tp):
    """
    If type is Optional[T] or T | None (i.e. Union[T, NoneType]), return T.
    Otherwise return tp unchanged.
    """
    origin = get_origin(tp)

    # Handles both typing.Union[...] and the new types.UnionType for `|` unions
    if origin in (Union, UnionType):
        args = [a for a in get_args(tp) if a is not type(None)]
        if len(args) == 1:
            return args[0]

    return tp


def _python_type_to_pandas_dtype(tp) -> str | None:
    """
    Map a Python / Pydantic type annotation to a pandas dtype string.
    Uses *nullable* pandas dtypes where possible.
    """
    tp = _strip_annotated(tp)
    tp = _strip_optional(tp)

    # basic scalars
    if tp is int:
        return "Int64"  # nullable integer
    if tp in (float, Decimal):
        return "float64"  # float64 + NaN handles nullability
    if tp is bool:
        return "boolean"  # pandas nullable boolean dtype
    if tp is str:
        return "string"  # pandas string dtype

    # dates / datetimes
    if tp in (datetime, date):
        return "datetime64[ns]"

    # other common scalars
    if tp is UUID:
        return "string"  # store UUID as string

    # fallback: let pandas keep 'object'
    return None


def models_to_dataframe(models: Sequence[BaseModel]) -> pd.DataFrame:
    """
    Convert a sequence of Pydantic v2 model instances to a pandas DataFrame,
    using the model *schema* (field annotations) to set column dtypes.

    - Works with nullable fields (T | None / Optional[T])
    - Uses pandas nullable dtypes where possible
    """
    if not models:
        raise ValueError("models_to_dataframe() expects at least one model instance")

    model_cls = type(models[0])

    # Pydantic v2 field definitions
    fields = getattr(model_cls, "model_fields", None)
    if fields is None:
        raise TypeError("Provided instances must be Pydantic v2 BaseModel objects")

    # Build dtype map purely from annotations
    dtype_map: dict[str, str] = {}
    for name, field in fields.items():
        annotation = field.annotation
        pd_dtype = _python_type_to_pandas_dtype(annotation)
        if pd_dtype is not None:
            dtype_map[name] = pd_dtype

    # Dump models to python-native values (not JSON strings)
    records = [m.model_dump(mode="python") for m in models]

    # Build DataFrame — pandas will infer initially, but we override.
    df = pd.DataFrame.from_records(records)

    # Enforce dtypes from schema
    for col, pd_dtype in dtype_map.items():
        if col not in df.columns:
            continue
        if pd_dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(pd_dtype)

    return df
