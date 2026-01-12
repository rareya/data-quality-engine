from backend.dq_engine.loader import DataLoader
from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.rules import MissingValueRule
from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.report import QualityReport
from backend.dq_engine.rule_factory import RuleFactory


class DataQualityPipeline:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def run(self):
        loader = DataLoader(self.file_path)
        df = loader.load()

        profiler = DataProfiler(df)
        profile = profiler.profile()

        schema = SchemaDetector(df).infer_schema()

        # 4. Generate rules dynamically (DATA-AGNOSTIC)
        factory = RuleFactory(schema=schema, profile=profile)
        rules = factory.generate_rules()



        rule_results = [rule.evaluate(profile) for rule in rules]

        scorer = QualityScorer(rule_results)
        score_report = scorer.evaluate()

        reporter = QualityReport(profile, rule_results, score_report)
        return reporter.generate()
