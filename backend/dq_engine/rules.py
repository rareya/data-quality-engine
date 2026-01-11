class RuleResult:
    def __init__(self, rule_name: str, passed: bool, message: str, weight: float = 1.0):
        self.rule_name = rule_name
        self.passed = passed
        self.message = message
        self.weight = weight


class BaseRule:
    def __init__(self, weight: float = 1.0):
        self.weight = weight

    def evaluate(self, profile: dict) -> RuleResult:
        raise NotImplementedError("Rule must implement evaluate()")


# ---------------- Column-level rules ----------------

class MissingValueRule(BaseRule):
    def __init__(self, column: str, max_missing_pct: float, weight: float = 1.0):
        super().__init__(weight)
        self.column = column
        self.max_missing_pct = max_missing_pct

    def evaluate(self, profile: dict) -> RuleResult:
        col_profile = profile["columns"].get(self.column)

        if not col_profile:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"Column '{self.column}' not found",
                weight=self.weight
            )

        missing_pct = col_profile.get("missing_pct", 0)

        if missing_pct > self.max_missing_pct:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"{self.column}: {missing_pct}% missing exceeds {self.max_missing_pct}%",
                weight=self.weight
            )

        return RuleResult(
            rule_name=self.__class__.__name__,
            passed=True,
            message=f"{self.column}: missing within limit",
            weight=self.weight
        )


# ---------------- Log-specific rules ----------------

class ErrorRateRule(BaseRule):
    def __init__(self, max_error_rate_pct: float, weight: float = 2.0):
        super().__init__(weight)
        self.max_error_rate_pct = max_error_rate_pct

    def evaluate(self, profile: dict) -> RuleResult:
        error_rate = profile.get("log_metrics", {}).get("error_rate_pct")

        if error_rate is None:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message="Error rate metric missing",
                weight=self.weight
            )

        if error_rate > self.max_error_rate_pct:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"Error rate {error_rate}% exceeds {self.max_error_rate_pct}%",
                weight=self.weight
            )

        return RuleResult(
            rule_name=self.__class__.__name__,
            passed=True,
            message="Error rate within acceptable range",
            weight=self.weight
        )


class TrafficVolumeRule(BaseRule):
    def __init__(self, min_rows: int, weight: float = 1.5):
        super().__init__(weight)
        self.min_rows = min_rows

    def evaluate(self, profile: dict) -> RuleResult:
        rows = profile.get("row_count", 0)

        if rows < self.min_rows:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"Too few log entries: {rows}",
                weight=self.weight
            )

        return RuleResult(
            rule_name=self.__class__.__name__,
            passed=True,
            message="Sufficient log volume",
            weight=self.weight
        )
