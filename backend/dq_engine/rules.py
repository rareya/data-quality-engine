class RuleResult:
    def __init__(self, passed: bool, message: str):
        self.passed = passed
        self.message = message


class BaseRule:
    def __init__(self, column: str):
        self.column = column

    def evaluate(self, profile: dict) -> RuleResult:
        """
        Every rule must implement this method.
        """
        raise NotImplementedError("Rule must implement evaluate()")


class MissingValueRule(BaseRule):
    def __init__(self, column: str, max_missing_pct: float):
        super().__init__(column)
        self.max_missing_pct = max_missing_pct

    def evaluate(self, profile: dict) -> RuleResult:
        columns = profile.get("columns", {})
        col_profile = columns.get(self.column)

        if not col_profile:
            return RuleResult(
                False,
                f"Column '{self.column}' not found in dataset"
            )

        missing_pct = col_profile.get("missing_pct", 0)

        if missing_pct > self.max_missing_pct:
            return RuleResult(
            False,
            f"Missing {missing_pct}% exceeds allowed {self.max_missing_pct}%"
            )

        return RuleResult(
        True,
        f"Missing {missing_pct}% within allowed limit"
    )

