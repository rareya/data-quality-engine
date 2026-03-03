"""
dq_engine
---------
Data Quality Engine package.
"""

from backend.dq_engine.pipeline import DataQualityPipeline, DataQualityPipelineFromDataFrame
from backend.dq_engine.loader import DataLoader
from backend.dq_engine.sql_loader import SQLiteLoader
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.rule_factory import RuleFactory
from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.eda import EDAAnalyzer
from backend.dq_engine.recommendations import RecommendationEngine

__all__ = [
    "DataQualityPipeline",
    "DataQualityPipelineFromDataFrame",
    "DataLoader",
    "SQLiteLoader",
    "DataProfiler",
    "SchemaDetector",
    "RuleFactory",
    "QualityScorer",
    "EDAAnalyzer",
    "RecommendationEngine",
]