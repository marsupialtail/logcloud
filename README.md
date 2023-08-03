# fts

**This is strictly confidential.** Please do not describe this algorithm to other people. 

In here contains a method to perform fast full text search on log files in S3. No hot indices. Performance comparable to Elasticsearch with disk/memory indices. How is it possible?

I considered alternatives like storing the inverted indices on S3 directly or brute forcing things like CLP. However they are all too complicated, brittle and most importantly not performant for important use cases.

Here is my solution:

- We will have separate buckets for separate types of logs. This is similar to different tables in a database.
- Inside a bucket, we are going to have different chunks that are chronologically ordered. Chunks are ideally organized by size, not constant time duration. 
- Each chunk correspond to logs in a fixed time interval, it is created from the raw logs in that time interval after the time interval has passed and pushed to S3 thereafter.
- How is the chunk created from the raw logs? It is a complicated process.

    - Each chunk has three separate files. They are designed to be independent and modular. Let's assume logs have length N.

        - The first file is the raw logs in Parquet format. The row group size is small, like 512 or 1024. This allows efficient random access. The typical compression ratio here is 10x or 20x using Apache Arrow's methods.
        
        - The second file is the Wavelet Tree with the bitvectors. This is the most complicated index, but also the smallest one. The file structure looks like 

        bit_vector_chunk | bit_vector_chunk | .... | bit_vector_chunk | chunk_offsets | level_offsets | C | 32 bytes

        The final 32 bytes contain the byte_offset from the beginning of the file to the chunk_offsets, level_offsets and C data structures. It also store N as the last 8 bytes. Typically you will read the last 32 bytes first and then read the metadata page with the three structures altogether. Each chunk and offset structure is compressed indepdendently. The final 32 bytes are not compressed.

        - The third file is the largest. It contains the suffic array. Once the FM index has returned to us a start and an end, we will need to figure out which original logs they correspond to! Hence we need to store for each index in the suffix array, which log file that suffix came from. These numbers are not guaranteed to be strictly increasing. (indeed they shouldn't be)

        We are going to store this list with block compression. The block compression algorithm is default ZSTD.

        The uncompressed size of the information contained in this file is 8N. Typically we can have a 50x compression ratio here. So the compressed size is 0.16N.
