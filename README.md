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

        - The first file is a compressed version of the original logs. Two requirements:
            - It must contain enough information to losslessly reconstruct each log.
            - It must allow efficient random access (and then decompression) to each log.
          The solution I have right now is to find a list of substrings to replace in the original file, and create new tokens in their place. Of course a more efficient compression algorithm can be used here, but it must satisfy the two requirements.

          The uncompressed size of the information contained in this file is N. Assuming a 2:1 compression ratio we can get to 0.5N.

        - The second file is a B+ tree of the FM index of the logs.
            - We will first perform BWT on the logs and construct the FM matrix and the C array.
            - The FM matrix is represented as a sparse matrix, where the nonzeros correspond to where the values change.
            - The key operation then is to be able to look up OCC[C][i], which is the row index of the nnz in the sparse matrix right before i. This means the sparse matrix nnzs need to be arranged in a format that supports efficient binary search.
            - The nnzs of each FM matrix column will thus be stored as a B+ tree with configurable block size B.
            - We call each leaf in the B+ tree a page. Then FM matrix is then represented as a sequence of pages.
            - The layout for a page is a sequence of tuples (val, child). child is the id of another page if this is not a leaf node, otherwise it's -1. 
            - The page itself is compressed using Snappy or Daniel Lemire's sorted integer compression algorithm.
            - We need to keep track of where each page lives. Each page might have different size due to compression. If we assume that B = 10k and N = 1B, then we have 100k pages. That is 100k sorted integers. We can compress this too and store it at the beginning of the file. This would be 200KB assuming a 2:1 compression ratio. 
        
           The uncompressed size of the information contained in this file is 8N, assuming 4 bytes for each nnz and 4 bytes for each page index. Assuming a 4:1 compression ratio (very optimistic), we can get to 2N.

           Upon search, we are going to first look at the offsets of each page with a fairly big byte range request. Then we are going to perform the FM substring search algorithm and iteratively look at OCC[C][s] and OCC[C][t]. Each access to OCC amounts to two or three page accesses assuming a page size of 10k elements. 

           As a result, assuming you are querying a pattern of length P, you will be fetching 200KB + 6 * P * page_size bytes from S3. Assuming 4:1 compression ratio and B = 10k, page size is 20KB. So it's 200KB + 120KB * P. For length 10 pattern, this is around 1.5 MB. 1.5 MB index read from 1GB raw logs for a pattern match, not bad at all. However most likely you are going to be latency bound. You are going to be making on the order of 4P - 6P requests to S3, assuming each request has a latency of 50ms that is 2.5s for a length 10 pattern. 

           Of course the search is trivially parallelizable across chunks.

        - The third file contains a mapping from the suffix array index to the log index. Once the FM index has returned to us a start and an end, we will need to figure out which original logs they correspond to! Hence we need to store for each index in the suffix array, which log file that suffix came from. These numbers are not guaranteed to be strictly increasing. (indeed they shouldn't be)

        We are going to store this list with block compression. The block compression algorithm can be one of Daniel Lemire's or Snappy.

        The uncompressed size of the information contained in this file is 4N. Assuming a 2:1 compression ratio we can get to 2N.

        Overall we need to store 4.5N data. This is still not ideal. We should try to cut this number further. A lot of this depends on the compression ratios. We should really try to optimize for the best compression algorithms at each stage. 