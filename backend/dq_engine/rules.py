class RuleResult:
    def __init__(self, rule_name: str, passed: bool, message: str, weight: float):
        self.rule_name = rule_name
        self.passed = passed
        self.message = message
        self.weight = weight


class BaseRule:
    def __init__(self, weight: float):
        self.weight = weight

    def evaluate(self, profile: dict) -> RuleResult:
        raise NotImplementedError
        

# ---------------- Column-level rules ----------------

class MissingValueRule(BaseRule):
    def __init__(self, column: str, max_missing_pct: float, weight: float = 20):
        super().__init__(weight)
        self.column = column
        self.max_missing_pct = max_missing_pct

    def evaluate(self, profile: dict) -> RuleResult:
        col_profile = profile["columns"].get(self.column)

        if not col_profile:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"Column '{self.column}' not found",
                self.weight
            )

        missing_pct = col_profile["missing_pct"]

        if missing_pct > self.max_missing_pct:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"{self.column}: {missing_pct}% missing",
                self.weight
            )

        return RuleResult(
            self.__class__.__name__,
            True,
            f"{self.column}: missing within limit",
            self.weight
        )


class DuplicateRowRule(BaseRule):
    def __init__(self, max_duplicate_pct: float = 5.0, weight: float = 15):
        super().__init__(weight)
        self.max_duplicate_pct = max_duplicate_pct

    def evaluate(self, profile: dict) -> RuleResult:
        total = profile["row_count"]
        duplicates = profile["duplicate_rows"]
        pct = (duplicates / total) * 100 if total else 0

        if pct > self.max_duplicate_pct:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"{pct:.2f}% duplicate rows",
                self.weight
            )

        return RuleResult(
            self.__class__.__name__,
            True,
            "Duplicate rows within limit",
            self.weight
        )


# ---------------- Log-specific rules ----------------

class ErrorRateRule(BaseRule):
    def __init__(self, max_error_rate_pct: float, weight: float = 10):
        super().__init__(weight)
        self.max_error_rate_pct = max_error_rate_pct

    def evaluate(self, profile: dict) -> RuleResult:
        error_rate = profile.get("log_metrics", {}).get("error_rate_pct")

        if error_rate is None:
            return RuleResult(
                self.__class__.__name__,
                False,
                "Error rate missing",
                self.weight
            )

        if error_rate > self.max_error_rate_pct:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"Error rate {error_rate}%",
                self.weight
            )

        return RuleResult(
            self.__class__.__name__,
            True,
            "Error rate acceptable",
            self.weight
        )


class TrafficVolumeRule(BaseRule):
    def __init__(self, min_rows: int, weight: float = 5):
        super().__init__(weight)
        self.min_rows = min_rows

    def evaluate(self, profile: dict) -> RuleResult:
        rows = profile["row_count"]

        if rows < self.min_rows:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"Only {rows} rows",
                self.weight
            )

        return RuleResult(
            self.__class__.__name__,
            True,
            "Sufficient data volume",
            self.weight
        )


class ConstantColumnRule(BaseRule):
    def __init__(self, column: str, weight: float = 10):
        super().__init__(weight)
        self.column = column

    def evaluate(self, profile: dict) -> RuleResult:
        col_profile = profile["columns"].get(self.column)

        if not col_profile:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"Column '{self.column}' not found",
                self.weight
            )

        unique_count = col_profile.get("unique_count", 0)

        if unique_count <= 1:
            return RuleResult(
                self.__class__.__name__,
                False,
                f"{self.column} is constant",
                self.weight
            )

        return RuleResult(
            self.__class__.__name__,
            True,
            f"{self.column} varies",
            self.weight
        )
