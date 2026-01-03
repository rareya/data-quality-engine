class RuleResult:
    def __init__(self, rule_name: str, passed: bool, message: str):
        self.rule_name = rule_name
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

    def check(self, profile):
        col_profile = profile["columns"].get(self.column)

        if not col_profile:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"Column '{self.column}' not found"
            )

        missing_pct = col_profile["missing_pct"]

        if missing_pct > self.max_missing_pct:
            return RuleResult(
                rule_name=self.__class__.__name__,
                passed=False,
                message=f"Missing {missing_pct}% exceeds allowed {self.max_missing_pct}%"
            )

        return RuleResult(
            rule_name=self.__class__.__name__,
            passed=True,
            message="Rule passed"
        )


