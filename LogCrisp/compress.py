import os
import shutil
import subprocess
import polars
from tqdm import tqdm

# Check if the file was provided as an argument
if len(os.sys.argv) < 3:
    print(f"Usage: {os.sys.argv[0]} <filenames> <templatename>")
    os.sys.exit(1)

CHUNK_SIZE = 64 * 1024 * 1024  # 64MB in bytes
ROW_GROUP_SIZE = 100000
ROW_GROUPS_PER_FILE = 100
TIMESTAMP_BYTES = 24
TOTAL_SAMPLE_LINES = 100000
SAMPLE_PER_FILE = TOTAL_SAMPLE_LINES // len(os.sys.argv[1:-1])
TIMESTAMP_PREFIX = "2018"
SPLIT_PREFIX = "chunks/chunk"

# Remove and create the 'chunks' directory
if os.path.exists("chunks"):
    shutil.rmtree("chunks")
os.makedirs("chunks")

# get the list of files, sort by filenames directly, this is sufficient for dates in filenames
files = sorted(os.sys.argv[1:-1])

# split each file, the numeric suffix for file i starts at the number of chunks already created
counter = 0
if os.path.exists("parquets"):
    shutil.rmtree("parquets")
os.makedirs("parquets")
all_logs = None

if os.path.exists("__sample__"):
    os.remove("__sample__")
sample_file = open("__sample__", "w")

import time

for file in tqdm(files):

    # first run the rexer on the file
    start = time.time()
    my_env = os.environ.copy()
    my_env["LD_LIBRARY_PATH"] = "/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow/"
    subprocess.run(["./rex", file, "__timestamp__", "__rexed__", "{}".format(TIMESTAMP_BYTES), TIMESTAMP_PREFIX], env=my_env)
    print("Rexing took {} seconds".format(time.time() - start))
    
    start = time.time()
    subprocess.run(["shuf", "-n", str(SAMPLE_PER_FILE), "__rexed__"], stdout=sample_file)
    print("Sampling took {} seconds".format(time.time() - start))

    start = time.time()
    logs = polars.from_dict({"timestamp": open("__timestamp__","r").readlines(), "logs":open("__rexed__", "r").readlines()})
    logs = logs.with_columns([polars.col("timestamp").str.strip(), polars.col("logs").str.strip()])
    print("Reading took {} seconds".format(time.time() - start))

    if all_logs is None:
        all_logs = logs
    else:
        all_logs = all_logs.vstack(logs)
    
    if len(all_logs) > ROW_GROUP_SIZE * ROW_GROUPS_PER_FILE:
        write_lines = len(all_logs) // (ROW_GROUP_SIZE * ROW_GROUPS_PER_FILE) * (ROW_GROUP_SIZE * ROW_GROUPS_PER_FILE)
        all_logs[:write_lines].write_parquet("parquets/{}.parquet".format(counter), row_group_size=ROW_GROUP_SIZE)
        all_logs = all_logs[write_lines:]
        counter += 1

    start = time.time()

    current_offset = sum(1 for entry in os.scandir("chunks") if entry.is_file())
    subprocess.run(
        [
            "split", "-a", "4", "-C", str(CHUNK_SIZE), "--numeric-suffixes={}".format(current_offset), "--additional-suffix=.txt",
            "__rexed__", SPLIT_PREFIX
        ]
    )
    print("Splitting took {} seconds".format(time.time() - start))

all_logs.write_parquet("parquets/{}.parquet".format(counter), row_group_size=ROW_GROUP_SIZE)
# remove the rexed files
os.remove("__timestamp__")
# you are gonna train on the last file
subprocess.run(["./LogCrisp_trainer_var/Trainer", "-I", "__sample__", "-O", os.sys.argv[-1]])
os.remove("__rexed__")
os.remove("__sample__")

# Manage directories
if os.path.exists("compressed"):
    shutil.rmtree("compressed")
os.makedirs("compressed")

counter = 0
THREADS = 8
ps = {}
for chunk in sorted(os.listdir("chunks")):
    chunk_path = os.path.join("chunks", chunk)
    if os.path.exists(f"variable_{counter}"):
        shutil.rmtree(f"variable_{counter}")
    os.makedirs(f"variable_{counter}")
    if os.path.exists("variable_tag_{counter}.txt"):
        shutil.rmtree("variable_tag_{counter}.txt")

    subprocess.run([
            "./LogCrisp_compression_var/Compressor", "-I", chunk_path,
            "-O", f"compressed/{os.path.splitext(chunk)[0]}", "-T", os.sys.argv[-1], "-P", str(counter)
        ])
    
    
    shutil.move(f"variable_{counter}", f"compressed/variable_{counter}")
    shutil.move(f"variable_{counter}_tag.txt", f"compressed/variable_{counter}_tag.txt")    
    counter += 1


# with open('./compressed/variable_tags', 'w') as outfile:
#     files = [os.path.join("./compressed", f) for f in os.listdir("./compressed") if f.startswith("variable_tag_")]
#     content = []
#     for file in files:
#         with open(file, 'r') as infile:
#             content.extend(infile.readlines())
#     content = sorted(set(content))
#     outfile.writelines(content)

# # Remove individual variable tag files
# for file in files:
#     os.remove(file)
