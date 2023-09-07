# first we are going to read all the variable_tag_*.txt and create a dictionary
# of variable name to types

import sys
import os

CHUNKS = 28

def variable_str_to_tup(variable):
    return tuple(int(k) for k in variable.replace("V","")[1:].split("_"))

def variable_to_paths(variable, chunks):
    result = []
    for i in range(CHUNKS):
        if os.path.exists("compressed/variable_{}/E".format(i) + str(variable[0]) + "_V" + str(variable[1])):
            result.append("compressed/variable_{}/E".format(i) + str(variable[0]) + "_V" + str(variable[1]))
        if len(result) == chunks:
            break
    return result
    
def sample_variable(variable, chunks = 2):
    paths = variable_to_paths(variable, chunks)
    lines = {}
    counter = 0
    for path in paths:
        lines[counter] = [l.replace("\n","") for l in open(path,"r").readlines()]
        counter += 1
    return lines


variable_to_type = {}
chunk_variables = {chunk: [] for chunk in range(CHUNKS)}
for chunk in range(CHUNKS):
    variable_tag_file = "compressed/variable_{}_tag.txt".format(chunk)
    lines = open(variable_tag_file).readlines()
    for line in lines:
        variable, tag = line.replace("\n","").split(" ")
        variable = variable_str_to_tup(variable)
        tag = int(tag)
        variable_to_type[variable] = tag
        chunk_variables[chunk].append(variable)
        

print(variable_to_type)
variables = list(variable_to_type.keys())

# we want to find items that are evenly distributed across space, many times
# these items are dictionary items, so we will find them and compress them
# these items will not appear in the other compacted_type files, the searcher will search the dictionary items first
# if the searcher finds that a dictionary item matches a query, it will just give up searching for 
# other matches in the other compacted_type files since there is already too many results

DICT_NUM_THRESHOLD = 100 # this item must appear at least this number times
DICT_SAMPLE_CHUNKS = 5
DICT_CHUNK_RATIO_THRESHOLD = 0.6 # at least this percent of sampled chunks must have this item
ROW_GROUP_SIZE = 100000
dictionary_items = set()
from collections import Counter

for variable in variables:
    lines = sample_variable(variable, DICT_SAMPLE_CHUNKS)
    counters = [Counter(lines[k]) for k in lines]
    # get all the items
    items = set()
    for counter in counters:
        items = items.union(set(counter.keys()))
    
    for item in items:
        # get the number of chunks that have this item
        num_chunks = 0
        for counter in counters:
            if item in counter:
                num_chunks += 1
        # get the number of times this item appears
        num_times = sum([counter[item] for counter in counters if item in counter])
        if num_times > DICT_NUM_THRESHOLD and num_chunks / DICT_SAMPLE_CHUNKS > DICT_CHUNK_RATIO_THRESHOLD:
            dictionary_items.add(item)

print(sorted(dictionary_items))
print(len(dictionary_items))
# eid to variables
touched_types = {0}
eid_to_variables = {}
for variable in variables:
    eid = variable[0]
    if eid not in eid_to_variables:
        eid_to_variables[eid] = []
    eid_to_variables[eid].append(variable)
    touched_types.add(variable_to_type[variable])

print(eid_to_variables)

# now we are just going to process the real variables in all the chunks

COMPACTION_WINDOW = 1000000 # in practice the window size will be larger than this because compaction is done only once per chunk at the end.
DEBUG = False
variable_files = {}

if DEBUG:
    type_files = {t: open("compressed/type_" + str(t),"w") for t in touched_types}
    type_lineno_files = {t: open("compressed/type_" + str(t) + "_lineno","w") for t in touched_types}

current_line_number = -1

compacted_type_files = {t: None for t in touched_types}
compacted_lineno_files = {t: None for t in touched_types}
outlier_file = open("compressed/outlier","w")
outlier_lineno_file = open("compressed/outlier_lineno","w")
OUTLIER_THRESHOLD = 1000

expanded_items = {t: [] for t in touched_types}
expanded_lineno = {t: [] for t in touched_types}

for chunk in range(CHUNKS):

    variable_files = {variable: open("compressed/variable_{}/E{}_V{}".format(chunk, variable[0], variable[1]), "r") for variable in variables if variable in chunk_variables[chunk]}

    chunk_file = "compressed/chunk{:04}.eid".format(chunk)
    print("processing chunk file: " + chunk_file)
    chunk_lines = open(chunk_file).readlines()
    chunk_eids = [int(k) for k in chunk_lines]

    for eid in chunk_eids:
        current_line_number += 1
        if eid not in eid_to_variables:
            continue
        this_variables = eid_to_variables[eid]
        type_vars = {}
        for variable in this_variables:

            item = variable_files[variable].readline().replace("\n","")
            if item in dictionary_items:
                t = 0
            else:
                t = variable_to_type[variable]
            
            if t in type_vars:
                type_vars[t].append(item)
            else:
                type_vars[t] = [item]

        for t in type_vars:
            expanded_items[t].extend(type_vars[t])
            expanded_lineno[t].extend([current_line_number for _ in range(len(type_vars[t]))])

        if DEBUG:
            for t in type_vars:
                type_files[t].write(" ".join(type_vars[t]) + "\n")
                type_lineno_files[t].write(str(current_line_number) + "\n")

    for t in touched_types: 
        if (len(expanded_items[t]) > COMPACTION_WINDOW or chunk == CHUNKS - 1) and len(expanded_items[t]) > 0:
            # sort expanded_items and expanded_lineno by expanded_items
            this_expanded_items, this_expanded_lineno = zip(*sorted(zip(expanded_items[t], expanded_lineno[t])))

            compacted_items = []
            compacted_lineno = []
            last_item = None

            for item, lineno in zip(this_expanded_items, this_expanded_lineno):
                if item != last_item:
                    compacted_items.append(item)
                    compacted_lineno.append([lineno])
                    last_item = item
                else:
                    compacted_lineno[-1].append(lineno)

            # sort compacted_items and compacted_lineno by the first element of compacted lineno
            compacted_items, compacted_lineno = zip(*sorted(zip(compacted_items, compacted_lineno), key=lambda x: x[1][0]))

            if len(compacted_items) > OUTLIER_THRESHOLD:
                if compacted_type_files[t] is None:
                    compacted_type_files[t] = open("compressed/compacted_type_" + str(t),"w")
                    compacted_lineno_files[t] = open("compressed/compacted_type_" + str(t) + "_lineno","w")
                for item, lineno in zip(compacted_items, compacted_lineno):
                    compacted_type_files[t].write(item + "\n")
                    compacted_lineno_files[t].write(" ".join([str(k) for k in lineno]) + " \n")
            else:
                for item, lineno in zip(compacted_items, compacted_lineno):
                    outlier_file.write(item + "\n")
                    outlier_lineno_file.write(" ".join([str(k) for k in lineno]) + " \n")
        
            expanded_items[t] = []
            expanded_lineno[t] = []
    
    for variable in variable_files:
        variable_files[variable].close()

for t in touched_types:
    if compacted_type_files[t] is not None:
        compacted_type_files[t].close()
        compacted_lineno_files[t].close()

outlier_file.close()
outlier_lineno_file.close()

if DEBUG:
    for t in touched_types:
        type_files[t].close()
        type_lineno_files[t].close()