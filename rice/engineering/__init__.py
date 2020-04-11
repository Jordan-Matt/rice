from enum import Enum
from .db_access import query_sql_pandas


class Field(Enum):
    DROP = 1
    KEEP = 2
