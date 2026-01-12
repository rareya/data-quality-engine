from backend.dq_engine.rules import MissingValueRule

class RuleFactory:
    def __init__(self, schema: dict, profile: dict):
        self.schema = schema
        self.profile = profile

    def generate_rules(self):
        rules = []

        for col, meta in self.profile["columns"].items():
            # Base weight logic
            weight = 20

            if self.schema[col]["semantic_type"] == "id-like":
                weight = 30
            elif self.schema[col]["semantic_type"] == "categorical":
                weight = 15

            rules.append(
                MissingValueRule(
                    column=col,
                    max_missing_pct=20,
                    weight=weight
                )
            )

        return rules
