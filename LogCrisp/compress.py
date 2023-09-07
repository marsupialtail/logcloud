import os
import shutil
import subprocess
import polars
from tqdm import tqdm

# Check if the file was provided as an argument
if len(os.sys.argv) < 3:
    print(f"Usage: {os.sys.argv[0]} <filenames> <templatename>")
    os.sys.exit(1)

TIMESTAMP_BYTES = 24
TIMESTAMP_PREFIX = "2018"

my_env = os.environ.copy()
my_env["LD_LIBRARY_PATH"] = "/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow/"
subprocess.run(["./rex"] + os.sys.argv[1:-1] + ["chunk", "test", str(TIMESTAMP_BYTES), TIMESTAMP_PREFIX], env=my_env)

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
