#!/bin/bash

# Check if the file was provided as an argument
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <filename> <templatename>"
    exit 1
fi

FILENAME=$1
rm -r chunks
mkdir chunks
# 64MB in bytes (64*1024*1024)
CHUNK_SIZE=67108864
SPLIT_PREFIX="chunks/chunk"

# Use split command to break the file on line boundaries
split -a 4 -C $CHUNK_SIZE -d --additional-suffix=.txt "$FILENAME" $SPLIT_PREFIX

echo "Splitting completed."

./LogCrisp_trainer_var/Trainer -I $FILENAME -O $2

rm -r variable
mkdir variable
rm -r compressed
mkdir compressed
counter=0
for chunk in chunks/*; do
        ./LogCrisp_compression_var/Compressor -I $chunk -O compressed/$(basename "${chunk%.txt}")  -T $2
        mv variable compressed/variable_$counter
	mv variable_tag.txt compressed/variable_tag_$counter.txt
        counter=$((counter + 1))
        mkdir variable
done

rm -r variable
