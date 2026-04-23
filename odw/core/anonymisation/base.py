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

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, F.when(F.col(column).isNull(), None).otherwise(F.lit("REDACTED")))


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


class PostcodeStrategy(BaseStrategy):
    classification_names = {"UK Postcode"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        col_expr = F.col(column).cast("string")
        result = F.when(F.col(column).isNull(), None).otherwise(F.split(col_expr, " ").getItem(0))
        return df.withColumn(column, result)


class AddressStrategy(BaseStrategy):
    classification_names = {
        "Address Line 1",
        "Address Line 2",
        "Street Address",
        "Postcode",
        "All Physical Addresses",
        "MICROSOFT.PERSONAL.PHYSICALADDRESS",  # Microsoft Purview classification
    }

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        """Redact address fields in non-production environments.

        Handles both simple string columns and struct (nested) columns.
        For struct columns (e.g. 'address' with fields like addressLine1, addressLine2),
        all nested fields are redacted.
        """
        from pyspark.sql.types import StructType

        col_type = dict(df.dtypes).get(column)

        if col_type and col_type.startswith("struct"):
            schema = df.schema[column].dataType
            if isinstance(schema, StructType):
                redacted_fields = []
                for field in schema.fields:
                    if field.name.lower() == "postcode":
                        pc = F.col(f"{column}.{field.name}")
                        redacted_fields.append(F.when(pc.isNull(), None).otherwise(F.split(pc.cast("string"), " ").getItem(0)).alias(field.name))
                    else:
                        redacted_fields.append(F.lit("REDACTED").alias(field.name))
                redacted_struct = F.struct(*redacted_fields)
                return df.withColumn(column, F.when(F.col(column).isNotNull(), redacted_struct).otherwise(None))

        return df.withColumn(column, F.when(F.col(column).isNotNull(), F.lit("REDACTED")).otherwise(None))


def default_strategies() -> List[BaseStrategy]:
    return [
        NINumberStrategy(),
        EmailMaskStrategy(),
        NameMaskStrategy(),
        PostcodeStrategy(),
        BirthDateStrategy(),
        AgeStrategy(),
        SalaryStrategy(),
        AddressStrategy(),
    ]
