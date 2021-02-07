import pandas as pd
import numpy as np
from numpy.lib.stride_tricks import as_strided as stride


def roll_np(df: pd.DataFrame, apply_func: callable, window: int, return_col_num: int, **kwargs) -> np.ndarray:
    """
    rolling with multiple columns on 2 dim pd.Dataframe
    * the result can apply the function which can return pd.Series with multiple columns

    call apply function with numpy ndarray
    :param return_col_num: 返回的列数
    :param apply_func:
    :param df:
    :param window
    :param kwargs:
    :return:
    """

    # move index to values
    v = df.reset_index().values

    dim0, dim1 = v.shape
    stride0, stride1 = v.strides

    stride_values = stride(v, (dim0 - (window - 1), window, dim1), (stride0, stride0, stride1))

    result_values = np.full((dim0, return_col_num), np.nan)

    for idx, values in enumerate(stride_values, window - 1):
        # values : col 1 is index, other is value
        result_values[idx,] = apply_func(values, **kwargs)

    return result_values


def roll_func(arr: np.ndarray, **kwargs):
    idx_col = arr[:, 0]
    col1 = arr[:, 1]
    col2 = arr[:, 2]

    # calc col1
    col1_rel = np.mean(col1)
    col2_rel = np.std(col2)

    return col1_rel, col2_rel


if __name__ == '__main__':
    dates1 = pd.date_range('20130101', periods=10, freq='H')
    df1 = pd.DataFrame(
        {'col1': [1.4, 0.4, 2, 43, 59, 1, 5.6, 9.2, 1.0, 23.7], 'col2': [5, 6, 7, 8, 1, 0, 12, 45, 8, 2]},
        index=dates1)
    df1.index.name = 'datetime'

    print(df1)

    rel: np.ndarray = roll_np(df1, roll_func, 3, 2) # 3 是 lookback window 2 是返回的结果数量，任意数值都行

    df1["col1_rel"] = rel[:, 0]
    df1["col2_rel"] = rel[:, 1]

    print(df1)
