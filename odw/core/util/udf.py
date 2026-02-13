
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf
def absolute(col: int):
    """
    Return the absolute value of an integer. e.g: absolute(-42) -> 42 , absolute(42) -> 42
    """
    if not isinstance(col, int):
        return None
    if col < 0:
        return 0 - col
    return col
