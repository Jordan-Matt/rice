from enum import Enum
from .operations import df_subset, df_drop, df_rename, df_apply, df_astype, df_map, df_prepare


class Field(Enum):
    DROP = 1
    KEEP = 2

