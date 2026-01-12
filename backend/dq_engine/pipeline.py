from backend.dq_engine.loader import DataLoader
from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.rule_factory import RuleFactory
from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.report import QualityReport


class DataQualityPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def run(self):
        # 1. Load data
        loader = DataLoader(self.file_path)
        df = loader.load()

        # 2. Profile data
        profiler = DataProfiler(df)
        profile = profiler.profile()

        # 3. Infer schema
        schema = SchemaDetector(df).infer_schema()

        # 4. Generate rules dynamically (DATA-AGNOSTIC)
        factory = RuleFactory(schema=schema, profile=profile)
        rules = factory.generate_rules()

        # 5. Evaluate rules
        rule_results = [rule.evaluate(profile) for rule in rules]

        # 6. Score dataset (0–100)
        scorer = QualityScorer(rule_results)
        score_report = scorer.evaluate()

        # 7. Generate final report
        reporter = QualityReport(profile, rule_results, score_report)
        return reporter.generate()
