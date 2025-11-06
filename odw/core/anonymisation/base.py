from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List, Sequence, Set

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


def _seed_col(df: DataFrame) -> Column:
    cols = [c for c in STAFF_SEED_COL_CANDIDATES if c in df.columns]
    return F.coalesce(*[F.col(c).cast("string") for c in cols]) if cols else F.lit("seed")


def mask_keep_first_last_col(col: Column) -> Column:
    return F.when(col.isNull(), None).otherwise(F.regexp_replace(col.cast("string"), r"(?<=.).(?=.$)", "*"))


@F.udf("string")
def mask_fullname_udf(v: str | None) -> str | None:
    if v is None:
        return None
    parts = [p for p in str(v).split() if p]

    def m(p: str) -> str:
        p = str(p)
        if len(p) <= 2:
            return p
        return p[0] + ("*" * (len(p) - 2)) + p[-1]

    return " ".join(m(p) for p in parts)


def random_int_from_seed(seed: Column, min_value: int, max_value: int) -> Column:
    return (F.abs(F.hash(seed)) % (max_value - min_value + 1)) + F.lit(min_value)


def random_date_from_seed(seed: Column, start: str = "1955-01-01", end: str = "2005-12-31") -> Column:
    start_date = F.to_date(F.lit(start))
    end_date = F.to_date(F.lit(end))
    days = F.datediff(end_date, start_date)
    offset = F.abs(F.hash(seed)) % days
    return F.date_add(start_date, offset.cast("int"))


@F.udf("string")
def mask_email_udf(email: str | None) -> str | None:
    try:
        if email is None:
            return None
        s = str(email)
        local = s.split("@")[0]
        if len(local) <= 2:
            masked_local = local
        else:
            masked_local = local[0] + ("*" * (len(local) - 2)) + local[-1]
        return f"{masked_local}@#PINS.com"
    except Exception:
        return None


@F.udf("string")
def generate_random_ni_number_udf(_: str | None) -> str:
    import random as _rand  # local import to keep worker safe

    letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    first = _rand.choice(letters)
    second = _rand.choice(letters)
    digits = "".join(str(_rand.randint(0, 9)) for _ in range(6))
    last = _rand.choice("ABCD")
    return f"{first}{second}{digits}{last}"


class Strategy(ABC):
    """Interface for anonymisation strategies applied to a single column."""

    @property
    @abstractmethod
    def classification_names(self) -> Set[str]:
        """Classification names that should trigger this strategy."""

    @abstractmethod
    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        """Return a new DataFrame with the transformation applied to the given column."""


class NINumberStrategy(Strategy):
    classification_names = {"NI Number", "PotentialID", "Potential ID"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, generate_random_ni_number_udf(F.col(column).cast("string")))


class EmailMaskStrategy(Strategy):
    classification_names = {"MICROSOFT.PERSONAL.EMAIL", "Email Address"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, mask_email_udf(F.col(column)))


class NameMaskStrategy(Strategy):
    classification_names = {"MICROSOFT.PERSONAL.NAME", "First Name", "Last Name"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        cname = column.lower()
        if "name" in cname and "first" not in cname and "last" not in cname:
            return df.withColumn(column, mask_fullname_udf(F.col(column)))
        return df.withColumn(column, mask_keep_first_last_col(F.col(column)))


class BirthDateStrategy(Strategy):
    classification_names = {"Birth Date", "Date of Birth"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, random_date_from_seed(seed))


class AgeStrategy(Strategy):
    classification_names = {"Person's Age", "Employee Age"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, random_int_from_seed(seed, 18, 70).cast("int"))


class SalaryStrategy(Strategy):
    classification_names = {"Annual Salary"}

    def apply(self, df: DataFrame, column: str, seed: Column, context: dict) -> DataFrame:
        return df.withColumn(column, random_int_from_seed(seed, 20000, 100000).cast("int"))


def default_strategies() -> List[Strategy]:
    return [
        NINumberStrategy(),
        EmailMaskStrategy(),
        NameMaskStrategy(),
        BirthDateStrategy(),
        AgeStrategy(),
        SalaryStrategy(),
    ]
