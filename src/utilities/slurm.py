"""Compatibility wrapper for SLURM helpers.

Canonical implementation lives in `utilities.compute_fabric`.
"""

from utilities.compute_fabric import allocate_resources, calculate_optimal_scaling

__all__ = ["allocate_resources", "calculate_optimal_scaling"]