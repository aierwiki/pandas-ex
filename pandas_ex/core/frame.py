# @Author: Tang Yubin <tangyubin>
# @Date:   2019-05-26T12:04:11+08:00
# @Email:  tang-yu-bin@qq.com
# @Last modified by:   tangyubin
# @Last modified time: 2019-05-26T16:24:35+08:00
import numpy as np
import pandas as pd
import gevent
from gevent import monkey, pool, Timeout
from multiprocessing import Pool, Manager, cpu_count
import joblib
import math
from ..parallel import ParallelExecutor


def parallel_executor_map_func_for_map(task_resource):
    task_id = task_resource[0]
    df = task_resource[1]
    map_func = task_resource[2]
    verbose = task_resource[3]
    if verbose > 0:
        print("start task {} ...".format(task_id))
    df[self._col_dst] = df[self._col_src].map(lambda x: map_func(x))
    if verbose > 0:
        print("task {} finished !".format(task_id))
    return df

class DataFrameHelper():
    """
    该类的提供多线程读取文件和多线程进行map操作的功能函数
    """
    def __init__(self, df):
        self._df = df
        self._n_threads = None
        self._n_task_per_thread = None
        self._n_routines = None
        self._map_func = None
        self._col_dst = None
        self._col_src = None
        self._verbose = 0
        self._parallel_executor = ParallelExecutor()

    def parallel_read_csv(self, csv_file_list, n_jobs = None):
        pass

    def _log(self, info):
        if self._verbose > 0:
            print(info)


    def _parallel_executor_reduce_func_for_map(self, result_list):
        self._log("start reduce results ...")
        if len(result_list) == 0:
            self._log("results is empty!")
            return self._df
        result_list = sorted(result_list, key=lambda x : x[0])
        df_list = [result[1] for result in result_list]
        df = pd.concat(df_list, axis=0)
        self._log("reduce process finished!")
        return df

    def parallel_map(self, col_dst, col_src, map_func, n_threads=None, n_task_per_thread=2, n_routines=None, verbose=0):
        if n_threads is None:
            self._n_threads = cpu_count()
            self._log("n_threads set {}".format(self._n_threads))
        else:
            self._n_threads = n_threads
        self._n_task_per_thread = n_task_per_thread
        self._n_routines = n_routines
        self._map_func = map_func
        self._col_dst = col_dst
        self._col_src = col_src
        self._verbose = verbose

        # 构造task_resources
        total_tasks_num = self._n_threads * self._n_task_per_thread
        dataframe_len = self._df.shape[0]
        num_per_task = math.ceil(dataframe_len / total_tasks_num)
        task_resources_list = []
        task_cnt = 0
        for i in range(self._n_threads):
            task_resources = []
            for j in range(self._n_task_per_thread):
                start_pos = task_cnt * num_per_task
                if start_pos >= dataframe_len:
                    continue
                end_pos = (task_cnt + 1) * num_per_task
                if (i == self._n_threads - 1) and (j == self._n_task_per_thread - 1):
                    task_resources.append((task_cnt, self._df.iloc[start_pos:], map_func, verbose))
                else:
                    task_resources.append((task_cnt, self._df.iloc[start_pos:end_pos], map_func, verbose))
            task_resources_list.append(task_resources)

        result = self._parallel_executor.run(task_resources_list, parallel_executor_map_func_for_map,
                                                reduce_func=self._parallel_executor_reduce_func_for_map,
                                                n_routines=n_routines)

        return result



