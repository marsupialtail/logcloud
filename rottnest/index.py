import os
import sys
import argparse
import time
import logging
import subprocess
from ctypes import *
import ctypes

logging.basicConfig(level=logging.INFO)

def to_c_string_array(py_strings):
    array = (ctypes.c_char_p * len(py_strings))()
    array[:] = [s.encode('utf-8') for s in py_strings]
    return array

def compress_files(current_files, index_name, group, prefix_bytes, prefix_format, parquet_dir):
    logging.info("./rex " + " ".join(current_files) + " " + index_name + " " + str(group) + " " + str(prefix_bytes) + " '" + prefix_format + "' " + parquet_dir)

    char_files = to_c_string_array(current_files)
    rex = PyDLL(os.path.dirname(__file__) + "/librex.cpython-3{}-x86_64-linux-gnu.so".format(sys.version_info.minor))
    rex.rex_python.argtypes = [ctypes.c_size_t, ctypes.POINTER(ctypes.c_char_p), ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
    rex.rex_python.restype = None

    rex.rex_python(len(char_files), char_files, index_name.encode('utf-8'), group, str(prefix_bytes).encode('utf-8'), prefix_format.encode('utf-8'), parquet_dir.encode('utf-8'))

def compact_indices(index_name, num_groups, split):
    logging.info("./index index " + index_name + " " + str(num_groups))

    index = PyDLL(os.path.dirname(__file__) + "/libindex.cpython-3{}-x86_64-linux-gnu.so".format(sys.version_info.minor))
    index.index_python.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
    index.index_python.restype = None
    index.index_python(index_name.encode('utf-8'),num_groups)

    # this will produce index_name.kauai, index_name.oahu, index_name.hawaii in the current directory, move them to the split directory
    [os.rename(index_name + k, index_name + "/indices/split_{}{}".format(split, k)) for k in [".kauai", ".oahu", ".hawaii"]]

def main():
    parser = argparse.ArgumentParser(description='Index with live tail or on-demand.')
    parser.add_argument('--mode', type=str, default='tail', help='tail or batch')
    parser.add_argument('--dir', type=str, default='.', help='directory to index')
    # specify the format, which can be text, json or parquet, currently only support text. 
    parser.add_argument('--format', type=str, default='text', help='format to index')
    parser.add_argument('--index_interval', type=int, default=1024, help='indexing interval in MBs')
    parser.add_argument('--compaction_interval', type=int, default=1024 * 20, help='compaction interval in MBs')
    parser.add_argument('--index_name', type=str, default='index', help='index name')
    parser.add_argument('--prefix_bytes', type=int, default=0, help='timestamp prefix characters')
    parser.add_argument('--prefix_format', type=str, default='', help='timestamp prefix format in linux strptime format')
    parser.add_argument('--tail-interval', type=int, default=10, help='tail interval in seconds')

    args = parser.parse_args()
    mode, dir, index_interval, compaction_interval, index_name, prefix_bytes, prefix_format = \
        args.mode, args.dir, args.index_interval, args.compaction_interval, args.index_name, args.prefix_bytes, args.prefix_format
    if not os.path.exists(dir):
        logging.ERROR("Directory does not exist.")
        exit(1)

    # check if the index_name directory exists, if so remove it

    if os.path.exists(index_name):
        logging.warning("Folder with index name already exists, removing it...")
        os.system("rm -rf {}".format(index_name))
    os.system("mkdir {}".format(index_name))

    """
    Eventually we will produce the following directory structure

    index_name/split_0/parquets/
                split_0/index_name.hawaii
                split_0/index_name.oahu
                split_0/index_name.kauai
                split_1/
                split_2/
                ....
    """
    # this is a temporary directory to write uncompacted but indexed chunks.
    if os.path.exists("compressed"):
        os.system("rm -rf compressed")
    os.system("mkdir compressed")

    group = 0
    split = 0

    os.mkdir(index_name + "/indices")
    os.mkdir(index_name + "/parquets")
    os.mkdir(index_name + "/parquets/split_" + str(split))


    if mode == "batch":

        files = os.listdir(dir)
        # remove files that has bad prefix meaning hidden files
        files = sorted([dir + f for f in files if not (f.startswith('_') or f.startswith('.'))])
        logging.info("Found {} files".format(len(files)))

        current_size = 0
        current_size_compaction = 0

        current_files = []
        for i in range(0, len(files)):
            current_size += os.path.getsize(files[i])
            current_size_compaction += os.path.getsize(files[i])
            current_files.append(files[i])

            if current_size >= index_interval * 1024 * 1024 or i == len(files) - 1:

                # run the indexer
                parquet_dir = index_name + "/parquets/" + "split_{}/".format(split)
                
                compress_files(current_files, index_name, group, prefix_bytes, prefix_format, parquet_dir)

                group += 1
                current_size = 0
                current_files = []

                if current_size_compaction >= compaction_interval * 1024 * 1024 or i == len(files) - 1:
                    # run the compactor
                    
                    compact_indices(index_name, group, split)
                    
                    current_size_compaction = 0
                    os.system("rm -rf compressed")
                    split += 1
                    group = 0
                    if i != len(files) - 1:
                        os.system("mkdir compressed")
                        os.mkdir(index_name + "/parquets/split_" + str(split))

    elif mode == "tail":

        split_size = 0

        while True:
            time.sleep(args.tail_interval)
            logging.info("Checking for new files...")
            files = os.listdir(dir)
            files = sorted([dir+ f for f in files if not (f.startswith('_') or f.startswith('.'))])

            # figure out the name of the last accessed file
            last_file = ""
            last_file_time = 0
            for f in files:
                if os.path.getmtime(dir + f) > last_file_time:
                    last_file_time = os.path.getmtime(dir + f)
                    last_file = f
            
            logging.info("Last file is {}, not indexing it".format(last_file))
            files.remove(last_file)
            
            # get the total size of these files
            total_size = 0
            for f in files:
                total_size += os.path.getsize(f)
            
            if total_size >= index_interval * 1024 * 1024:

                parquet_dir = index_name + "/parquets/" + "split{}/".format(split)
                command = "./rex " + " ".join(files) + " " + index_name + " " + str(group) + " " + str(prefix_bytes) + " '" + prefix_format + "' " + parquet_dir
                
                logging.info(command)
                subprocess.call(["./rex", *files, index_name, str(group), str(prefix_bytes), prefix_format, parquet_dir])

                group += 1
                split_size += total_size

                # now go remove the files you indexed
                for f in files:
                    os.remove(f)
            
            if split_size >= compaction_interval * 1024 * 1024:

                # run the compactor
                command = "./index index " + index_name + " " + str(group)
                logging.info(command)
                subprocess.call(["./index", "index", index_name, str(group)])

                # this will produce index_name.kauai, index_name.oahu, index_name.hawaii in the current directory, move them to the split directory
                [os.rename(index_name + k, index_name + "/indices/split_{}{}".format(split, k)) for k in [".kauai", ".oahu", ".hawaii"]]
                
                split_size = 0
                split += 1
                group = 0
                os.system("rm -rf compressed")
                os.system("mkdir compressed")
                os.mkdir(index_name + "/parquets/split_" + str(split))


if __name__ == "__main__":
    main()