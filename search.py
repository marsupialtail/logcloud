import daft
import pyarrow as pa
import polars
from ctypes import *
import ctypes
import sys
import boto3

ROW_GROUPS_IN_FILE = 10

class Vector(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_size_t)),
                ("size", ctypes.c_size_t)]


EMPTY = 18446744073709551615

def row_group_search(filenames, row_groups, query, batch_size = 100):
    
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

    z = daft.daft.read_parquet_into_pyarrow_bulk(filenames, row_groups = transformed_row_groups)
    logs = [k[2][1] for k in z if k[2] != [[],[]]]
    if len(logs) == 0:
        return None
    ts = [k[2][0] for k in z if k[2] != [[],[]]]
    logs_array = pa.concat_arrays([item.cast(pa.large_string()) for sublist in logs for item in sublist])
    ts_array = pa.concat_arrays([item for sublist in ts for item in sublist])

    result = polars.DataFrame([polars.from_arrow(logs_array), polars.from_arrow(ts_array)]).filter(polars.col("column_0").str.contains(query))
    result = result.unique()
    result = result.rename({"column_0" : "log", "column_1" : "ts"})
    return result

def brute_force_search(filenames, query, limit):

    results = []
    # you should search in reverse order
    for file in filenames[::-1]:
        df = polars.read_parquet(file)
        result = df.filter(df["log"].str.contains(query))
        if len(result) > 0:
            results.append(result)
            if sum([len(r) for r in results]) > limit:
                break
    
    return polars.concat(results)

index_name = sys.argv[1]
num_splits = int(sys.argv[2]) # in the future this will be read from a metadata file.
query = sys.argv[3]
limit = int(sys.argv[4])

lib = PyDLL('libindex.so')

if index_name[:5] == "s3://":

    index_name = index_name[5:].rstrip("/")
    bucket = index_name.split("/")[0]

    split_prefixes = ["split_" + str(i) for i in range(num_splits)]

    # read the splits in reverse order 
    for split in split_prefixes[::-1]:
        filenames = ["s3://{}{}{}.parquet".format(index_name, i) for i in range(len(daft.daft.io_glob(FILEPATH)))]

        lib.search_python.argtypes = [c_char_p, c_char_p, c_size_t]
        lib.search_python.restype = Vector
        result = lib.search_python(index_name.encode('utf-8'), query.encode('utf-8'), limit)

    # row_groups = [result.data[i] for i in range(result.size)]

    # if row_groups != [EMPTY]:
    #     result = row_group_search(filenames, row_groups, query)
    # else:
    #     result = brute_force_search(filenames, query, limit)

    # result.write_parquet("search_result.parquet")
