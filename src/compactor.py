import os

def merge_files(input_filenames, input_filenames_linenos, output_filename, output_filename_linenos):
    input_files = [open(filename, 'r') for filename in input_filenames]
    input_files_linenos = [open(filename, 'r') for filename in input_filenames_linenos]
    
    current_lines = []
    current_linenos = []
    output_file = open(output_filename, 'w')
    output_file_linenos = open(output_filename_linenos, 'w')

    # read the first line from each file
    for f in input_files:
        current_lines.append(f.readline().replace('\n', ''))
    for f in input_files_linenos:
        current_linenos.append([int(i) for i in f.readline().replace(' \n', '').split(" ")])
    
    while any(current_lines):
        it = min([k for k in current_lines if k is not None])
        it_linenos = []
        where_is_it = [i for i, x in enumerate(current_lines) if x == it]
        for loc in where_is_it:
            it_linenos.extend(current_linenos[loc])
        
        # write it and it_lineos to output file
        output_file.write(it + '\n')
        output_file_linenos.write(' '.join([str(i) for i in it_linenos]) + '\n')

        for loc in where_is_it:
            attempt = input_files[loc].readline()
            if attempt == '':
                current_lines[loc] = None
                input_files[loc].close()
                input_files_linenos[loc].close()
            else:
                current_lines[loc] = attempt.replace('\n', '')
                current_linenos[loc] = [int(i) for i in input_files_linenos[loc].readline().replace(' \n', '').split(" ")]
    
    output_file.close()
    output_file_linenos.close()

merge_files([f"compressed/{i}/compacted_type_1" for i in range(17)], [f"compressed/{i}/compacted_type_1_lineno" for i in range(17)], 
            "compressed/compacted_type_1", "compressed/compacted_type_1_lineno")

