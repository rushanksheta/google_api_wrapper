from dataclasses import dataclass
from typing import Optional, Iterable, Dict, List, Literal, Annotated, Union
from datetime import datetime, date, time, timezone
from delta.tables import DeltaTable
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

def _parse_rfc3339(ts: str) -> datetime:
    # Accept...Z or +00:00
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _to_rfc3339(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _is_rfc3339(s: str) -> bool:
    try:
        # tolerate trailing 'Z'
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False
    

# DEFAULT_WATERMARK = datetime(2000, 1, 1, tzinfo=timezone.utc)
DEFAULT_TIME_ZONE = "America/New_York"
RFC3339ZStr = Annotated[str, Is[_is_rfc3339]]
SparkSessionType = Union[
    *(t for t in (ClassicSpark, ConnectSpark) if t is not None)
]


# @dataclass
# class FileWatermarkStore:
#     """Simple JSON watermark: {'last_submit_ts': '<RFC3339Z>'}"""
#     path: str
#     DEFAULT_WATERMARK: datetime = datetime(2000, 1, 1, tzinfo=timezone.utc)

#     def read(self) -> datetime:
#         try:
#             with open(self.path, "r", encoding="utf-8") as f:
#                 payload = json.load(f)
#             ts = payload.get("last_submit_ts")
#             return _parse_rfc3339(ts) if ts else self.DEFAULT_WATERMARK
#         except FileNotFoundError:
#             self.write(self.DEFAULT_WATERMARK)
#             return self.DEFAULT_WATERMARK
#         except Exception:
#             # If corrupt, start from epoch but do NOT overwrite automatically
#             return self.DEFAULT_WATERMARK

#     def write(self, dt_obj: datetime) -> None:
#         os.makedirs(os.path.dirname(self.path), exist_ok=True)
#         tmp = self.path + ".tmp"
#         with open(tmp, "w", encoding="utf-8") as f:
#             json.dump({"last_submit_ts": _to_rfc3339_z(dt_obj)}, f)
#         os.replace(tmp, self.path)

@beartype
class DeltaWatermarkStore:
    def __init__(self, spark: SparkSessionType, table: str, default_tzone: str = DEFAULT_TIME_ZONE):
        self.spark, self.table = spark, table
        self.default_tzone = default_tzone

        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    # def get(self, key: str) -> Optional[datetime]:
    #     row = self.spark.sql(f"SELECT value_ts FROM {self.table} WHERE key = '{key}'").first()
    #     return row.value_ts if row else _to_rfc3339(DEFAULT_WATERMARK)
    
    def get_rfc3339(self, key: str) -> Optional[str]:
        """
        Return the stored RFC3339Z string for reuse in the Forms filter.
        """
        df = (self.spark.table(self.table)
              .where(F.col("key") == F.lit(key))
              .select("ts_rfc3339")
              .limit(1))
        row = df.first()
        return row.ts_rfc3339 if row else None
    
    def set_if_newer(self, key: str, rfc_ts: RFC3339ZStr) -> None:
        # watermark = _to_rfc3339(ts) #ts.isoformat().replace("+00:00", "Z")
        self.spark.sql(f"""
          MERGE INTO {self.table} t
          USING (SELECT '{key}' AS key, to_timestamp('{rfc_ts}') AS new_ts) s
          ON t.key = s.key
          WHEN MATCHED AND s.new_ts > t.value_ts
            THEN UPDATE SET value_ts = s.new_ts, updated_at = current_timestamp(), updated_by = current_user()
          WHEN NOT MATCHED
            THEN INSERT (key, value_ts, updated_at, updated_by)
                 VALUES (s.key, s.new_ts, current_timestamp(), current_user())
        """)

    @beartype
    def set_kv(self, key: str, rfc_ts: RFC3339ZStr) -> None:

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