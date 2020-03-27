import pytest
import pandas as pd
from rice.utilities import df_utils

# Constants
a_vals = [1, 2, 3, 4, 5]
b_vals = [6, 7, 8, 9, 10]
vals = {"a": a_vals, "b": b_vals}
df_test = pd.DataFrame(vals)


def test_describe_stats():

    df_described = df_utils.describe_stats(df_test)
    df_expected_index = [
        "count",
        "mean",
        "std",
        "cv",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "iqr",
    ]

    assert isinstance(df_described, pd.DataFrame)
    assert list(df_described.index) == df_expected_index
