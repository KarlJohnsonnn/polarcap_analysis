"""Compatibility wrapper for chunking helpers.

Canonical implementation lives in `utilities.compute_fabric`.
"""

from utilities.compute_fabric import auto_chunk_dataset, describe_chunk_plan, recommend_target_chunk_mb

__all__ = ["recommend_target_chunk_mb", "auto_chunk_dataset", "describe_chunk_plan"]
