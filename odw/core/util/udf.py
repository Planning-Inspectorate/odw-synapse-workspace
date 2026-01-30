
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf
def absolute(col: int):
    if not isinstance(col, int):
        return None
    if col < 0:
        return 0 - col
    return col
