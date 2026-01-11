from backend.dq_engine.loader import DataLoader
from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.rules import MissingValueRule
from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.report import QualityReport


class DataQualityPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def run(self):
        loader = DataLoader(self.file_path)
        df = loader.load()

        profiler = DataProfiler(df)
        profile = profiler.profile()

        schema = SchemaDetector(df).infer_schema()

        rules = []

        for col, meta in profile["columns"].items():
            rules.append(
                MissingValueRule(
                    column=col,
                    max_missing_pct=20   # configurable threshold
                )
            )


        rule_results = [rule.evaluate(profile) for rule in rules]

        scorer = QualityScorer(rule_results)
        score_report = scorer.evaluate()

        reporter = QualityReport(profile, rule_results, score_report)
        return reporter.generate()
