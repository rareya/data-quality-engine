"""
pipeline.py
-----------
Main orchestration pipeline for the Data Quality Engine.

Flow:
1. Load data (CSV / Excel / JSON / SQL)
2. Profile data (missing values, distributions, stats)
3. Infer schema (data types, semantic types)
4. Generate rules dynamically based on schema + profile
5. Evaluate rules → pass/fail results
6. Score dataset (0-100 weighted score)
7. Run EDA (correlations, distributions, outliers, insights)
8. Generate recommendations (actionable fixes)
9. Compile final report
"""

from backend.dq_engine.smart_loader import SmartLoader
from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.rule_factory import RuleFactory
from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.report import QualityReport
from backend.dq_engine.eda import EDAAnalyzer
from backend.dq_engine.recommendations import RecommendationEngine, generate_natural_language_summary


class DataQualityPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def run(self) -> dict:
        # 1. Load data
        loader = SmartLoader(self.file_path)
        df, parse_report = loader.load()

        # 2. Profile data
        profiler = DataProfiler(df)
        profile = profiler.profile()

        # 3. Infer schema
        schema = SchemaDetector(df).infer_schema()

        # 4. Generate rules dynamically
        factory = RuleFactory(schema=schema, profile=profile)
        rules = factory.generate_rules()

        # 5. Evaluate rules
        rule_results = [rule.evaluate(profile) for rule in rules]

        # 6. Score dataset (0-100)
        scorer = QualityScorer(rule_results)
        score_report = scorer.evaluate()

        # 7. Run EDA
        eda = EDAAnalyzer(df)
        eda_results = eda.analyze()

        # 8. Generate recommendations
        rec_engine = RecommendationEngine(profile, schema, rule_results)
        recommendations = rec_engine.generate()

        # 9. Natural language summary
        nl_summary = generate_natural_language_summary(
            profile, score_report, recommendations
        )

        # 10. Compile final report
        reporter = QualityReport(profile, rule_results, score_report)
        report = reporter.generate()
        report["parse_report"] = parse_report

        report["eda"] = eda_results
        report["recommendations"] = recommendations
        report["summary"] = nl_summary

        return report


class DataQualityPipelineFromDataFrame:
    """
    Alternative pipeline that accepts a pandas DataFrame directly.
    Used when data comes from SQL loader or in-memory sources.
    """

    def __init__(self, df):
        self.df = df

    def run(self) -> dict:
        df = self.df

        profiler = DataProfiler(df)
        profile = profiler.profile()

        schema = SchemaDetector(df).infer_schema()

        factory = RuleFactory(schema=schema, profile=profile)
        rules = factory.generate_rules()
        rule_results = [rule.evaluate(profile) for rule in rules]

        scorer = QualityScorer(rule_results)
        score_report = scorer.evaluate()

        eda = EDAAnalyzer(df)
        eda_results = eda.analyze()

        rec_engine = RecommendationEngine(profile, schema, rule_results)
        recommendations = rec_engine.generate()

        nl_summary = generate_natural_language_summary(
            profile, score_report, recommendations
        )

        reporter = QualityReport(profile, rule_results, score_report)
        report = reporter.generate()

        report["eda"] = eda_results
        report["recommendations"] = recommendations
        report["summary"] = nl_summary

        return report