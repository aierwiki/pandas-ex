import numpy as np
import pandas as pd
from pandas_ex import DataFrameHelper


def main():
    df = pd.DataFrame({'a':[1, 2, 3, 4, 5, 6, 7, 8]})
    df_helper = DataFrameHelper(df)
    df_new = df_helper.parallel_map('b', 'a', lambda x : x + 10, verbose=1)
    print(df_new)

if __name__ == "__main__":
    main()
