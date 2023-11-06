import daft
import pyarrow as pa
import polars
from ctypes import *
import ctypes
import os
import sys
import argparse
import boto3
import time

ROW_GROUPS_IN_FILE = 10

class Vector(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_size_t)),
                ("size", ctypes.c_size_t)]


EMPTY = 18446744073709551615

def row_group_search(filenames, row_groups, query, limit, batch_size = 100):

    row_group_keys = {}
    transformed_row_groups = []
    for r in row_groups:
        if r // ROW_GROUPS_IN_FILE not in row_group_keys:
            row_group_keys[r // ROW_GROUPS_IN_FILE] = []
        row_group_keys[r // ROW_GROUPS_IN_FILE].append(r % ROW_GROUPS_IN_FILE)

    transformed_row_groups = [[] for i in range(len(filenames))]
    results = []

    for key in row_group_keys:
        for i in row_group_keys[key]:
            transformed_row_groups[key].append(i)

    result = polars.DataFrame()
    for i in range(0, len(transformed_row_groups), 10):
        z = daft.daft.read_parquet_into_pyarrow_bulk(filenames[i:i+10], row_groups = transformed_row_groups[i:i+10])

        # handle the timestamp some other way!, basically make sure log is indeed [2][0], it may be [2][1] if tehre is ts

        logs = [k[2][0] for k in z if k[2] != [[],[]]]
        logs = [item.cast(pa.large_string()) for sublist in logs for item in sublist]
        if len(logs) == 0:
            continue
        logs_array = pa.concat_arrays(logs)

        result.vstack(polars.DataFrame([polars.from_arrow(logs_array)]).filter(polars.col("column_0").str.contains(query)).unique(), in_place = True)
        if len(result) > limit:
            break
        # result = result.rename({"column_0" : "log"})
    return result


def brute_force_search(filenames, query, limit):

    reversed_filenames = filenames[::-1]
    results = []
    # you should search in reverse order in batches of 10
    for start in range(0, len(reversed_filenames), 10):
        batch = reversed_filenames[start:start+10]
        a = daft.daft.read_parquet_into_pyarrow_bulk(batch)
        df = polars.DataFrame([polars.from_arrow(pa.concat_arrays([pa.concat_arrays(k[2][0]) for k in a]))])
        result = df.filter(polars.col("column_0").str.contains(query))
        if len(result) > 0:
            results.append(result)
            if sum([len(r) for r in results]) > limit:
                break
    
    if len(results) > 0:
        return polars.concat(results)
    else:
        return None

def count_folders_in_prefix(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    folder_count = 0
    seen_folders = set()

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        for folder in page.get('CommonPrefixes', []):
            folder_name = folder['Prefix']
            if folder_name not in seen_folders:
                seen_folders.add(folder_name)
                folder_count += 1

    return folder_count

def search(index_path, query, limit):

    if index_path[:5] == "s3://":
        bucket = index_path[5:].split("/")[0]
        index_name = "/".join(index_path[5:].split("/")[1:]).rstrip("/") + "/"
        num_splits = count_folders_in_prefix(bucket, index_name + "parquets/")
    elif index_path[:5] == "gs://" or index_path[:5] == "az://":
        raise NotImplementedError("GCS and Azure not supported yet")
    else:
        num_splits = len(os.listdir(index_path + "/parquets/"))

    print(num_splits)
    lib = PyDLL(os.path.dirname(__file__) + "/libindex.cpython-3{}-x86_64-linux-gnu.so".format(sys.version_info.minor))

    index_path = index_path.rstrip("/")
    split_prefixes = ["split_" + str(i) for i in range(num_splits)]

    # read the splits in reverse order 

    all_dfs = []

    index_time = 0

    for split_prefix in split_prefixes[::-1]:
        number_of_files = len(daft.daft.io_glob("{}/parquets/{}/**".format(index_path, split_prefix)))
        filenames = ["{}/parquets/{}/{}.parquet".format(index_path, split_prefix,i) for i in range(number_of_files)]

        lib.search_python.argtypes = [c_char_p, c_char_p, c_size_t]
        lib.search_python.restype = Vector

        split_index_prefix = index_path + "/indices/" + split_prefix

        start = time.time()

        # the c bindings expect s3://bucket/index_name/split_i as the argument or path/split_i as the argument
        result = lib.search_python(split_index_prefix.encode('utf-8'), query.encode('utf-8'), limit)

        index_time += time.time() - start

        row_groups = [result.data[i] for i in range(result.size)]
        row_groups = sorted(list(set(row_groups)))

        if row_groups != [EMPTY]:
            result = row_group_search(filenames, row_groups, query, limit)
        else:
            result = brute_force_search(filenames, query, limit)
        
        if result is not None:
            all_dfs.append(result)

        if sum([len(i) for i in all_dfs]) > limit:
            break

    print("INDEX TIME: {}".format(index_time))
    
    if len(all_dfs) == 0 or sum([len(i) for i in all_dfs]) == 0:
        return None 
    else:
        result = polars.concat(all_dfs)
        return result

def main():

    parser = argparse.ArgumentParser(description='Search the index')
    parser.add_argument('--index_path', required = True, type=str, help='path to the index')
    parser.add_argument('--query', required = True, type=str, help='query')
    parser.add_argument('--limit', required = True, type=int, help='limit')
    parser.add_argument('--output', required = False, default='search_result.parquet', type=str, help='output path, e.g. test.parquet')

    index_path, query, limit, output_path = parser.parse_args().index_path, parser.parse_args().query, parser.parse_args().limit, parser.parse_args().output

    result = search(index_path, query, limit)

    if result is None:
        print("No results found")
    else:
        print("Found {} hits, wrote output to {}".format(len(result), output_path))
        result.write_parquet(output_path)

if __name__ == "__main__":
    main()