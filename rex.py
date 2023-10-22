import os
import sys

files = os.listdir(sys.argv[1])
group_size = int(sys.argv[2])
index_name = sys.argv[3]
prefix_bytes = int(sys.argv[4])
prefix = sys.argv[5]
# remove files that has bad prefix
files = sorted([sys.argv[1] + f for f in files if not (f.startswith('_') or f.startswith('.'))])

# check if the compressed/ directory exists, if so remove it

if os.path.exists("compressed"):
    os.system("rm -rf compressed")
os.system("mkdir compressed")
if os.path.exists("parquets"):
    os.system("rm -rf parquets")
os.system("mkdir parquets")

counter = 0
for i in range(0, len(files), group_size):
    group_files = files[i:i+group_size]
    command = "./rex " + " ".join(group_files) + " " + index_name + " " + str(counter) + " " + str(prefix_bytes) + " '" + prefix + "'"
    counter += 1
    print(command)
    os.system(command)
