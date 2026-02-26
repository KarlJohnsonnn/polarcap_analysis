"""Compatibility wrapper for runtime environment helpers.

Canonical implementation lives in `utilities.compute_fabric`.
"""

from utilities.compute_fabric import in_slurm_allocation, is_server

__all__ = ["is_server", "in_slurm_allocation"]

