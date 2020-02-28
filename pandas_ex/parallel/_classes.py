# @Author: Tang Yubin <tangyubin>
# @Date:   2020-02-27T12:04:11+08:00
# @Email:  tang-yu-bin@qq.com
# @Last modified by:   tangyubin
# @Last modified time: 2020-02-27T16:24:35+08:00

import gevent
from gevent import monkey, pool, Timeout
from multiprocessing import Pool, Manager, cpu_count
import joblib
import pandas as pd

def job_func(task):
    return 10

def event_pool(task_resources, result_list, pool_size):
    #print("gevent_pool task num {}".format(len(task_resources)))
    #print("gevent_pool pool_size {}".format(pool_size))
    print('hello')
    p = pool.Pool(pool_size)
    jobs = []
    for task_resource in task_resources:
        job = p.spawn(job_func, task_resource)
        jobs.append(job)
    try:
        gevent.joinall(jobs)
    except Exception as e:
        print(e)
    for job in jobs:
        result_list.append(job.get())

class ParallelExecutor():
    """
    该工具类提供了通用的并发执行框架，能够实现线程+协程级别的并发计算。
    """
    def __init__(self):
        self._n_jobs = None
        self._task_resources_list = None
        self._map_func = None
        self._n_routines = None

    def _multiprocess_execute(self):
        self._n_jobs = len(self._task_resources_list)
        print("n_jobs = {}".format(self._n_jobs))
        pool = Pool(processes=self._n_jobs)
        result_list = Manager().list()

        for task_resources in self._task_resources_list:
            print("task resources {}".format(len(task_resources)))
            pool.apply_async(event_pool, (task_resources, result_list, self._n_routines))
        pool.close()
        pool.join()
        return result_list

    def run(self, task_resources_list, map_func, reduce_func=None, n_routines=2):
        """
        执行并发运算
        :param task_resources_list: 需要并发执行的操作所需参数列表，二维列表。
        [
            [args for routine1 of thread1, args for routine2 of thread1, ...],
            [args for routine1 of thread2, args for routine2 of thread2, ...],
            ...
        ]
        列表中的args在框架执行时会作为参数传递给map_func
        列表的长度会作为框架启动线程的数量
        :param map_func: 并发操作执行函数，该函数需要接受task_resource_list中的值作为参数
        :param reduce_func: 用来合并map_func返回结果的函数，该函数需要接收map_func执行结果列表作为参数，可选参数，默认为None
        :param n_routines: 一个线程内协程的数量
        :return: 如果reduce_func为None，则将reduce_func在多个协程内执行后返回的结果组成一个list返回，
            否则，将结果list传给reduce_func，然后将reduce_func的结果返回
        """
        self._task_resources_list = task_resources_list
        self._map_func = map_func
        self._n_routines = n_routines

        results = self._multiprocess_execute()
        print(results)

        #if reduce_func is None:
        #    return results
        #else:
        #    return reduce_func(results)
