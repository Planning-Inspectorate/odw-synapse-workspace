from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Sequence, Set

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.column import Column


STAFF_SEED_COL_CANDIDATES: Sequence[str] = (
    "Staff Number",
    "PersNo",
    "PersNo.",
    "Personnel Number",
    "Employee ID",
    "EmployeeID",
)

_NI_LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"  # 24 characters (no I or O)
_NI_LAST_LETTERS = "ABCD"


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
    def seed_col(df: DataFrame, seed_column: Optional[str] = None) -> Column:
        """Return a deterministic seed column.

        If seed_column is given and present in the DataFrame it is used directly.
        Otherwise falls back to known identifier column candidates, then a full-row hash.
        The full-row hash fallback is not idempotent across schema changes; prefer
        providing an explicit seed_column via the anonymisation policy config.
        """
        if seed_column and seed_column in df.columns:
            return F.col(seed_column).cast("string")
        cols = [c for c in STAFF_SEED_COL_CANDIDATES if c in df.columns]
        if cols:
            return F.coalesce(*[F.col(c).cast("string") for c in cols])
        # Last resort: deterministic per-row hash using all columns
        if not df.columns:
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
        return F.when(col.isNull(), None).otherwise(F.regexp_replace(col.cast("string"), r"(?<=.).(?=.)", "*"))

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

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        """Generate a deterministic fake NI number using Spark column expressions.

        Format: <letter><letter><6 digits><A-D>
        Each component is derived from a salted hash of the seed column so that
        different positions produce different values from the same seed.
        """
        letters_arr = F.array([F.lit(c) for c in _NI_LETTERS])
        last_arr = F.array([F.lit(c) for c in _NI_LAST_LETTERS])
        first = F.element_at(letters_arr, (F.abs(F.hash(F.concat(seed, F.lit("_ni1")))) % 24 + 1).cast("int"))
        second = F.element_at(letters_arr, (F.abs(F.hash(F.concat(seed, F.lit("_ni2")))) % 24 + 1).cast("int"))
        digits = F.lpad((F.abs(F.hash(F.concat(seed, F.lit("_nid")))) % 1000000).cast("string"), 6, "0")
        last = F.element_at(last_arr, (F.abs(F.hash(F.concat(seed, F.lit("_nil")))) % 4 + 1).cast("int"))
        ni_num = F.concat(first, second, digits, last)
        return df.withColumn(column, F.when(F.col(column).isNull(), None).otherwise(ni_num))


class EmailMaskStrategy(BaseStrategy):
    classification_names = {"MICROSOFT.PERSONAL.EMAIL", "Email Address", "Email Address Column Name"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        """Hash the email address using SHA-256 of the lowercased value.

        Normalising to lowercase before hashing ensures the same email stored in
        different cases across tables produces the same hash, enabling reliable
        joins on the anonymised column. Nulls are preserved.
        """
        result = F.when(F.col(column).isNull(), None).otherwise(F.sha2(F.lower(F.col(column).cast("string")), 256))
        return df.withColumn(column, result)


class NameMaskStrategy(BaseStrategy):
    classification_names = {"MICROSOFT.PERSONAL.NAME", "First Name", "Last Name", "Names Column Name", "All Full Names"}

    @staticmethod
    def _mask_first_only_expr(col: Column) -> Column:
        """Keep the first character, replace the rest with '*'.
        Uses a lookbehind so any character that has a preceding character is replaced.
        """
        return F.regexp_replace(col, r"(?<=.).", "*")

    @staticmethod
    def _mask_last_only_expr(col: Column) -> Column:
        """Keep the last character, replace the rest with '*'.
        Uses a lookahead so any character that has a following character is replaced.
        """
        return F.regexp_replace(col, r".(?=.)", "*")

    @staticmethod
    def _mask_fullname_expr(col: Column) -> Column:
        """Mask a full name: keep first char of first token and last char of last token.
        Single-token names fall back to mask_keep_first_last (first and last char).
        """
        tokens = F.split(col, r"\s+")
        n = F.size(tokens)
        first_token = tokens.getItem(0)
        last_token = tokens.getItem(n - 1)
        masked_first = NameMaskStrategy._mask_first_only_expr(first_token)
        masked_last = NameMaskStrategy._mask_last_only_expr(last_token)
        middle_masked = F.transform(
            F.slice(tokens, 2, n - 2),
            lambda t: F.regexp_replace(t, ".", "*"),
        )
        return F.when(n == 1, BaseStrategy.mask_keep_first_last(col)).otherwise(
            F.array_join(
                F.concat(F.array(masked_first), middle_masked, F.array(masked_last)),
                " ",
            )
        )

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        col_expr = F.col(column).cast("string")
        classes = set(context.get("classifications") or [])
        if classes.intersection(self.classification_names):
            result = F.when(F.col(column).isNull(), None).otherwise(
                F.when(col_expr.rlike(r"\s+"), self._mask_fullname_expr(col_expr)).otherwise(self._mask_first_only_expr(col_expr))
            )
        else:
            cname = column.lower()
            if "name" in cname and "first" not in cname and "last" not in cname:
                result = F.when(F.col(column).isNull(), None).otherwise(self._mask_fullname_expr(col_expr))
            else:
                result = BaseStrategy.mask_keep_first_last(F.col(column))
        return df.withColumn(column, result)


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
