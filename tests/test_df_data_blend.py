import pytest
import pandas as pd
from rice.data_blend.operations import df_astype, df_rename, df_apply

# constants
DATA_TEST_1 = {"var": ["1", "2", "3"], "var2": [1, 2, 3]}
CONFIG_RENAME = {"var": "VAR", "var2": "VAR_2"}
CONFIG_ASTYPE = {"var": "int64", "var2": "str"}  # can use str or object
df = pd.DataFrame(DATA_TEST_1)


def test_data_blend_rename():
    df_renamed = df_rename(df, CONFIG_RENAME)
    assert list(df_renamed.columns) == list(CONFIG_RENAME.values())


def test_data_blend_astype():
    df_typechange = df_astype(df, CONFIG_ASTYPE)
    final_types = [str(df_typechange[col].dtype) for col in df_typechange.columns]
    df_expected_types = ["int64", "object"]
    assert list(final_types) == df_expected_types


def test_data_blend_apply():
    def func(row):
        return row ** 2

    astype = {"var": "int64", "var2": "float"}
    df_new = df_apply(df_astype(df, astype), func)
    df_example = pd.DataFrame({"var": [1.0, 4.0, 9.0], "var2": [1.0, 4.0, 9.0]})
    assert df_new.equals(df_example)
