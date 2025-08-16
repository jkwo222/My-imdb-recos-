# FILE: engine/util/__init__.py
"""
Utility package exports.

Re-exports DiskCache so both:
    from engine.util import DiskCache
and:
    from engine.util.cache import DiskCache
work.
"""
from .cache import DiskCache

__all__ = ["DiskCache"]