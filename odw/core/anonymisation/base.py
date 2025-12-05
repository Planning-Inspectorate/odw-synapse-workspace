from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Sequence, Set
import random as _rand

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.column import Column


STAFF_SEED_COL_CANDIDATES: Sequence[str] = (
    "Staff Number",
    "PersNo",
    "PersNo.",
    "Personnel Number",
    "Employee ID",
    "EmployeeID",
)


class BaseStrategy(ABC):
    """Interface for anonymisation strategies applied to a single column."""

    @property
    @abstractmethod
    def classification_names(self) -> Set[str]:
        """Classification names that should trigger this strategy."""

    @abstractmethod
    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        """Return a new DataFrame with the transformation applied to the given column."""

    @staticmethod
    def seed_col(df: DataFrame) -> Column:
        """Return a deterministic seed column.
        Picks the first available identifier in STAFF_SEED_COL_CANDIDATES; if none exist, derive a per-row hash from all columns.
        """
        cols = [c for c in STAFF_SEED_COL_CANDIDATES if c in df.columns]
        if cols:
            return F.coalesce(*[F.col(c).cast("string") for c in cols])
        # Fallback: deterministic per-row hash using all columns (string-cast, null-safe)
        if not df.columns:
            # Extremely defensive: empty schema; maintain previous behavior
            return F.lit("seed")
        concatenated = F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in sorted(df.columns)],
        )
        return F.sha2(concatenated, 256)

    @staticmethod
    def mask_keep_first_last(col: Column) -> Column:
        """Mask a string, keeping only the first and last character.
        Nulls are preserved; intermediate characters are replaced with '*'.
        """
        return F.when(col.isNull(), None).otherwise(F.regexp_replace(col.cast("string"), r"(?<=.).(?=.$)", "*"))

    @staticmethod
    def random_int_from_seed(seed: Column, min_value: int, max_value: int) -> Column:
        """Deterministically generate an integer in [min_value, max_value].
        The value is derived from the hash of the provided seed column.
        """
        return (F.abs(F.hash(seed)) % (max_value - min_value + 1)) + F.lit(min_value)

    @staticmethod
    def random_date_from_seed(seed: Column, start: str = "1955-01-01", end: str = "2005-12-31") -> Column:
        start_date = F.to_date(F.lit(start))
        end_date = F.to_date(F.lit(end))
        days = F.datediff(end_date, start_date)
        offset = F.abs(F.hash(seed)) % days
        return F.date_add(start_date, offset.cast("int"))


class NINumberStrategy(BaseStrategy):
    classification_names = {"NI Number"}

    def generate_random_ni_number(_: str | None) -> str:
        letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
        first = _rand.choice(letters)
        second = _rand.choice(letters)
        digits = "".join(str(_rand.randint(0, 9)) for _ in range(6))
        last = _rand.choice("ABCD")
        return f"{first}{second}{digits}{last}"

    # Public UDF
    generate_random_ni_number_udf = F.udf(generate_random_ni_number, T.StringType())

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, NINumberStrategy.generate_random_ni_number_udf(F.col(column).cast("string")))


class EmailMaskStrategy(BaseStrategy):
    classification_names = {"MICROSOFT.PERSONAL.EMAIL", "Email Address", "Email Address Column Name"}

    def _mask_email_preserve_domain(email: str | None) -> str | None:
        """Mask email local part (keep first and last char) and preserve the original domain (no '#').
        If not a valid email, mask the whole string similarly.
        """
        try:
            if email is None:
                return None
            s = str(email)
            parts = s.split("@", 1)
            if len(parts) != 2:
                local = s
                if len(local) <= 2:
                    masked_local = local
                else:
                    masked_local = local[0] + ("*" * (len(local) - 2)) + local[-1]
                return masked_local
            local, domain = parts[0], parts[1]
            if len(local) <= 2:
                masked_local = local
            else:
                masked_local = local[0] + ("*" * (len(local) - 2)) + local[-1]
            return f"{masked_local}@{domain}"
        except Exception:
            return None

    # UDF (public)
    mask_email_preserve_domain_udf = F.udf(_mask_email_preserve_domain, T.StringType())

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        # Mask local part and preserve domain (no '#', no EmployeeID override)
        return df.withColumn(column, EmailMaskStrategy.mask_email_preserve_domain_udf(F.col(column)))


class NameMaskStrategy(BaseStrategy):
    classification_names = {"MICROSOFT.PERSONAL.NAME", "First Name", "Last Name", "Names Column Name"}

    def _mask_fullname_initial_lastletter(v: str | None) -> str | None:
        """Mask full name keeping only:
        - first letter of the first name
        - last letter of the last name
        All other letters are replaced by '*'.
        Example: "John Doe" -> "J*** **e".
        """
        if v is None:
            return None
        tokens = [t for t in str(v).split() if t]
        if not tokens:
            return None
        if len(tokens) == 1:
            t = tokens[0]
            if len(t) == 1:
                return t
            return t[0] + ("*" * max(0, len(t) - 2)) + t[-1]

        first = tokens[0]
        last = tokens[-1]
        middle = tokens[1:-1] if len(tokens) > 2 else []

        def mask_first(t: str) -> str:
            return t[0] + ("*" * (len(t) - 1)) if len(t) >= 1 else ""

        def mask_middle(t: str) -> str:
            return "*" * len(t)

        def mask_last(t: str) -> str:
            return ("*" * (len(t) - 1)) + t[-1] if len(t) >= 1 else ""

        out = [mask_first(first)]
        out.extend(mask_middle(m) for m in middle)
        out.append(mask_last(last))
        return " ".join(out)

    def _mask_name_first_only(v: str | None) -> str | None:
        """Mask single-part name: keep first letter, mask all remaining letters.
        Example: 'John' -> 'J***'.
        """
        if v is None:
            return None
        s = str(v)
        if len(s) <= 1:
            return s
        return s[0] + ("*" * (len(s) - 1))

    # UDFs (public)
    mask_fullname_initial_lastletter_udf = F.udf(_mask_fullname_initial_lastletter, T.StringType())
    mask_name_first_only_udf = F.udf(_mask_name_first_only, T.StringType())

    # Backward-compat private aliases
    _mask_fullname_initial_lastletter_udf = mask_fullname_initial_lastletter_udf
    _mask_name_first_only_udf = mask_name_first_only_udf

    @staticmethod
    def mask_keep_first_last_col_expr(col: Column) -> Column:
        return BaseStrategy.mask_keep_first_last(col)

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        # Apply by classification: if value looks like full name (contains whitespace),
        # keep first letter of first name and last letter of surname; otherwise keep only first letter.
        classes = set(context.get("classifications") or [])
        if classes.intersection(self.classification_names):
            # Use Column.rlike to avoid Spark SQL parsing the regex as an identifier
            return df.withColumn(
                column,
                F.when(
                    F.col(column).cast("string").rlike(r"\s+"),
                    NameMaskStrategy.mask_fullname_initial_lastletter_udf(F.col(column)),
                ).otherwise(NameMaskStrategy.mask_name_first_only_udf(F.col(column))),
            )

        # Fallback legacy heuristic based on column name
        cname = column.lower()
        if "name" in cname and "first" not in cname and "last" not in cname:
            return df.withColumn(column, NameMaskStrategy.mask_fullname_initial_lastletter_udf(F.col(column)))
        return df.withColumn(column, NameMaskStrategy.mask_keep_first_last_col_expr(F.col(column)))


class BirthDateStrategy(BaseStrategy):
    classification_names = {"Birth Date", "Date of Birth"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, BaseStrategy.random_date_from_seed(seed))


class AgeStrategy(BaseStrategy):
    classification_names = {"Person's Age", "Employee Age"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, BaseStrategy.random_int_from_seed(seed, 18, 70).cast("int"))


class SalaryStrategy(BaseStrategy):
    classification_names = {"Annual Salary"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, BaseStrategy.random_int_from_seed(seed, 20000, 100000).cast("int"))


def default_strategies() -> List[BaseStrategy]:
    return [
        NINumberStrategy(),
        EmailMaskStrategy(),
        NameMaskStrategy(),
        BirthDateStrategy(),
        AgeStrategy(),
        SalaryStrategy(),
    ]
