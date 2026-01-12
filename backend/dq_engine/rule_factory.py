from backend.dq_engine.rules import MissingValueRule

class RuleFactory:
    def __init__(self, schema: dict, profile: dict):
        self.schema = schema
        self.profile = profile

    def generate_rules(self):
        rules = []

        for col, col_profile in self.profile["columns"].items():
            semantic = self.schema[col]["semantic_type"]

            # relative importance (not final score)
            if semantic == "id-like":
                weight = 3
            elif semantic == "categorical":
                weight = 2
            else:
                weight = 1

            rules.append(
                MissingValueRule(
                    column=col,
                    max_missing_pct=20,
                    weight=weight
                )
            )

        return rules
