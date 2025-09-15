from dataclasses import dataclass
from typing import Optional, Iterable, Dict, List, Literal, Annotated, Union
from datetime import datetime, date, time, timezone
from delta.tables import DeltaTable
import re
# import json, os, time as _time

# from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, functions as F

from beartype import beartype
from beartype.vale import Is

try:
    from pyspark.sql.session import SparkSession as ClassicSpark
except ImportError:
    ClassicSpark = None

try:
    from pyspark.sql.connect.session import SparkSession as ConnectSpark
except ImportError:
    ConnectSpark = None

# --------- Utilities ---------

def _is_rfc3339(s: str) -> bool:
    """
    Strict RFC3339 UTC ('Z') validator allowing only 0, 3, 6, or 9 fractional digits.
    """
    # Shape: yyyy-mm-ddThh:mm:ss(.ddd|.dddddd|.ddddddddd)?Z
    m = re.match(
        r"^\d{4}-\d{2}-\d{2}T"
        r"\d{2}:\d{2}:\d{2}"
        r"(?:\.(\d{3}|\d{6}|\d{9}))?Z$",
        s,
    )
    if not m:
        return False

    # Final sanity parse (normalize to <=6 fractional digits for Python)
    try:
        if "." in s:
            head, fracZ = s.split(".", 1)
            frac = fracZ[:-1]  # drop trailing 'Z'
            if len(frac) == 3:      # ms -> μs
                frac = frac + "000"
            elif len(frac) == 9:    # ns -> μs (truncate)
                frac = frac[:6]
            # len==6 -> μs, keep as is
            s = f"{head}.{frac}Z"
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

# DEFAULT_WATERMARK = datetime(2000, 1, 1, tzinfo=timezone.utc)
DEFAULT_TIME_ZONE = "America/New_York"
RFC3339ZStr = Annotated[str, Is[_is_rfc3339]]
SparkSessionType = Union[*(t for t in (ClassicSpark, ConnectSpark) if t is not None)]

def _parse_rfc3339(ts: RFC3339ZStr) -> datetime:
    # Accept...Z or +00:00
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _to_rfc3339(dt_obj: datetime) -> RFC3339ZStr:
    return dt_obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

@beartype
class DeltaWatermarkStore:
    def __init__(self, spark: SparkSessionType, table: str, default_tzone: str = DEFAULT_TIME_ZONE):
        self.spark, self.table = spark, table
        self.default_tzone = default_tzone

        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    # def get(self, key: str) -> Optional[datetime]:
    #     row = self.spark.sql(f"SELECT value_ts FROM {self.table} WHERE key = '{key}'").first()
    #     return row.value_ts if row else _to_rfc3339(DEFAULT_WATERMARK)
    
    def get_watermark(self, key: str) -> Optional[RFC3339ZStr]:
        """
        Return the stored RFC3339Z string for reuse in the Forms filter.
        Assumes unique key in the table.
        """
        row = (self.spark.table(self.table)
              .where(F.col("key") == F.lit(key))
              .select("ts_rfc3339")
              .first())
        return row.ts_rfc3339 if row else None

    @beartype
    def set_watermark(self, key: str, rfc_ts: RFC3339ZStr) -> None:

        src = (self.spark.createDataFrame([(key, rfc_ts)], "key STRING, rfc STRING")
               .select(
                   "key",
                   F.col("rfc").alias("new_ts_rfc3339"),
                   F.to_timestamp("rfc").alias("new_ts_utc"),
                   F.from_utc_timestamp(F.to_timestamp("rfc"), self.default_tzone).alias("new_ts_et"),
               ))

        dt = DeltaTable.forName(self.spark, self.table)
        (dt.alias("t")
           .merge(src.alias("s"), "t.key = s.key")
           .whenMatchedUpdate(set={
               "ts": "s.new_ts_utc",
               "ts_rfc3339": "s.new_ts_rfc3339",
               "ts_est": "s.new_ts_et",
               "updated_at": F.current_timestamp().cast("timestamp"),
               "updated_by": F.current_user(),
           })
           .whenNotMatchedInsert(values={
               "key": "s.key",
               "ts": "s.new_ts_utc",
               "ts_rfc3339": "s.new_ts_rfc3339",
               "ts_est": "s.new_ts_et",
               "updated_at": F.current_timestamp().cast("timestamp"),
               "updated_by": F.current_user(),
           })
           .execute())