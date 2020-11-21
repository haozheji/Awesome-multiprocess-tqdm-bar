from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import time
import random

def func_single_argument(n):
    
    time.sleep(0.5)
    
    return n

def func_multiple_argument(n, m, *args, **kwargs):
    
    time.sleep(0.5)

    return n, m

def run_imap_multiprocessing(func, argument_list, num_processes):

    pool = Pool(processes=num_processes)

    result_list_tqdm = []
    for result in tqdm(pool.imap(func=func, iterable=argument_list), total=len(argument_list)):
        result_list_tqdm.append(result)

    return result_list_tqdm

def run_imap_unordered_multiprocessing(func, argument_list, num_processes):

    pool = Pool(processes=num_processes)

    result_list_tqdm = []
    for result in tqdm(pool.imap_unordered(func=func, iterable=argument_list), total=len(argument_list)):
        result_list_tqdm.append(result)

    return result_list_tqdm

def run_apply_async_multiprocessing(func, argument_list, num_processes):

    pool = Pool(processes=num_processes)

    jobs = [pool.apply_async(func=func, args=(*argument,)) if isinstance(argument, tuple) else pool.apply_async(func=func, args=(argument,)) for argument in argument_list]
    pool.close()
    result_list_tqdm = []
    for job in tqdm(jobs):
        result_list_tqdm.append(job.get())

    return result_list_tqdm

def main():

    num_processes = 10
    num_jobs = 100
    random_seed = 0
    random.seed(random_seed) 

    # imap, imap_unordered
    # It only support functions with one dynamic argument
    func = func_single_argument
    argument_list = [random.randint(0, 100) for _ in range(num_jobs)]
    print("Running imap multiprocessing for single-argument functions ...")
    result_list = run_imap_multiprocessing(func=func, argument_list=argument_list, num_processes=num_processes)
    assert result_list == argument_list
    print("Running imap_unordered multiprocessing for single-argument functions ...")
    result_list = run_imap_unordered_multiprocessing(func=func, argument_list=argument_list, num_processes=num_processes)
    # partial functions (one dynamic argument, one or more than one fixed arguments)
    partial_func = partial(func_multiple_argument, m=10)
    print("Running imap multiprocessing for single-argument partial functions ...")
    result_list = run_imap_multiprocessing(func=partial_func, argument_list=argument_list, num_processes=num_processes)
    print("Running imap_unordered multiprocessing for single-argument partial functions ...")
    result_list = run_imap_unordered_multiprocessing(func=partial_func, argument_list=argument_list, num_processes=num_processes)
    # Since it is unordered, this assertion might not be valid
    # assert result_list == argument_list

    # apply_async
    # One dynamic argument
    func = func_single_argument
    argument_list = [random.randint(0, 100) for _ in range(num_jobs)]
    print("Running apply_async multiprocessing for single-argument functions ...")
    result_list = run_apply_async_multiprocessing(func=func, argument_list=argument_list, num_processes=num_processes)
    assert result_list == argument_list
    # More than one dynamic arguments
    func = func_multiple_argument
    argument_list = [(random.randint(0, 100), random.randint(0, 100)) for _ in range(num_jobs)]
    print("Running apply_async multiprocessing for multi-argument functions ...")
    result_list = run_apply_async_multiprocessing(func=func, argument_list=argument_list, num_processes=num_processes)
    assert result_list == argument_list
    # partial functions (multiple dynamic arguments, one or more than one fixed arguments)
    partial_func = partial(func_multiple_argument, x=1, y=2, z=3) # Giving some arguments for kwargs
    print("Running apply_async multiprocessing for multi-argument partial functions ...")
    result_list = run_apply_async_multiprocessing(func=partial_func, argument_list=argument_list, num_processes=num_processes)
    assert result_list == argument_list

if __name__ == "__main__":

    main()
