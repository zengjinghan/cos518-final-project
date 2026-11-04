"""Pure selection policies; execution and publication belong to the engine."""

from .plan import CompactionPlan, select_compaction

__all__ = ["CompactionPlan", "select_compaction"]
