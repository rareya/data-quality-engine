from backend.dq_engine.rules import (
    MissingValueRule,
    ConstantColumnRule,
    DuplicateRowRule,
    ErrorRateRule,
    TrafficVolumeRule
)


class RuleFactory:
    def __init__(self, schema: dict, profile: dict):
        self.schema = schema
        self.profile = profile

    def generate_rules(self):
        rules = []

        # ---------------- Column-level rules ----------------
        for col, col_profile in self.profile["columns"].items():

            semantic = self.schema[col]["semantic_type"]

            # Default weights
            missing_weight = 20
            constant_weight = 10

            # Increase importance based on semantic meaning
            if semantic == "id-like":
                missing_weight = 30
                constant_weight = 20
            elif semantic == "categorical":
                missing_weight = 15

            rules.append(
                MissingValueRule(
                    column=col,
                    max_missing_pct=20,
                    weight=missing_weight
                )
            )

            rules.append(
                ConstantColumnRule(
                    column=col,
                    weight=constant_weight
                )
            )

        # ---------------- Dataset-level rules ----------------
        rules.append(
            DuplicateRowRule(
                max_duplicate_pct=5.0,
                weight=15
            )
        )

        # ---------------- Log-specific rules (optional) ----------------
        if "log_metrics" in self.profile:
            rules.append(
                ErrorRateRule(
                    max_error_rate_pct=2.0,
                    weight=10
                )
            )

            rules.append(
                TrafficVolumeRule(
                    min_rows=100,
                    weight=5
                )
            )

        return rules
