import numpy as np
import pandas as pd
from pandas_ex import DataFrameHelper

def map_func(x):
    return x + 10

def main():
    df = pd.DataFrame({'a':[1, 2, 3, 4, 5, 6]})
    df_helper = DataFrameHelper(df)
    df_new = df_helper.parallel_map('b', 'a', map_func, n_threads=2, n_tasks_per_thread=3, verbose=1)
    print(df_new)

if __name__ == "__main__":
    main()
