from backend.dq_engine.rules import (
    MissingValueRule,
    ConstantColumnRule,
    DuplicateRowRule,
    ErrorRateRule,
    TrafficVolumeRule
)

# Semantics where constant values are VALID
CONSTANT_ALLOWED_SEMANTICS = {
    "unit",
    "units",
    "currency",
    "country-code",
    "system-metadata",
    "status",
    "http_status",    # HTTP status being constant (all 200s) = healthy server, not a data issue
    "http_method",    # HTTP method being constant is valid in filtered log datasets
    "boolean",        # Boolean columns with one value are valid in some contexts
}


class RuleFactory:
    def __init__(self, schema: dict, profile: dict):
        self.schema = schema
        self.profile = profile

    def generate_rules(self):
        rules = []

        # ---------------- Column-level rules ----------------
        for col, col_profile in self.profile["columns"].items():

            # Normalize semantic
            semantic = (
                self.schema.get(col, {})
                .get("semantic_type", "unknown")
                .strip()
                .lower()
            )

            # Default weights
            missing_weight = 20
            constant_weight = 10

            if semantic == "id-like":
                missing_weight = 30
                constant_weight = 20
            elif semantic == "categorical":
                missing_weight = 15

            # Missing values rule ALWAYS applies
            rules.append(
                MissingValueRule(
                    column=col,
                    max_missing_pct=20,
                    weight=missing_weight
                )
            )

            # ✅ Apply ConstantColumnRule ONLY when column is expected to vary
            if semantic not in CONSTANT_ALLOWED_SEMANTICS:
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

        # ---------------- Log-specific rules ----------------
        log_metrics = self.profile.get("log_metrics")

        if (
            isinstance(log_metrics, dict)
            and log_metrics.get("error_rate_pct") is not None
        ):
            rules.append(
                ErrorRateRule(
                    max_error_rate_pct=2.0,
                    weight=10
                )
            )

        if self.profile.get("row_count", 0) >= 100:
            rules.append(
                TrafficVolumeRule(
                    min_rows=100,
                    weight=5
                )
            )

        return rules