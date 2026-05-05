"""
assembler.common.cache_utils
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cache_key_fn helpers for Prefect tasks.

Any @task that receives a Path argument (e.g. regimens_full) can opt into
content-addressed caching by passing:

    @task(
        name="my-task",
        cache_key_fn=file_hash_cache_key,
        cache_expiration=timedelta(hours=24),
    )
    def my_task(cfg: AssemblerConfig, input_path: Path): ...

The key is deterministic: same file content + same task version = cache hit.
Config objects are stringified via their model_dump() if they are Pydantic models,
otherwise via str().

Design note
-----------
Prefect's built-in ``task_input_hash`` hashes ALL parameters including cfg objects,
which may contain mutable paths that change between runs even when content is identical.
``file_hash_cache_key`` is content-addressed: only file bytes + task version matter.
"""
from __future__ import annotations

import hashlib
from pathlib import Path


def file_hash_cache_key(context, parameters: dict) -> str:
    """
    Cache key = SHA-256 of:
      - task name + task version
      - bytes of every Path argument that exists on disk (content-addressed)
      - str() of all other arguments (AssemblerConfig, flags, etc.)

    Parameters
    ----------
    context    : TaskRunContext supplied by Prefect
    parameters : dict of task call arguments

    Returns
    -------
    str — hex digest used as Prefect cache key
    """
    hasher = hashlib.sha256()

    # task identity
    hasher.update(context.task.name.encode())
    hasher.update((context.task.version or "0").encode())

    for name, val in sorted(parameters.items()):
        if isinstance(val, Path):
            if val.exists() and val.is_file():
                hasher.update(val.read_bytes())
            else:
                hasher.update(str(val).encode())
        elif hasattr(val, "model_dump"):
            # Pydantic v2 model (AssemblerConfig, FieldSchema, …)
            hasher.update(str(val.model_dump()).encode())
        else:
            hasher.update(str(val).encode())

    return hasher.hexdigest()
