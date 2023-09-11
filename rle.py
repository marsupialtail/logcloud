import numpy as np
from tqdm import tqdm

numbers = np.load("logidx.npy")

total_bits = 0

curr_num = numbers[0]
total_bits += 9

for i in tqdm(range(1, len(numbers))):
    if numbers[i] == curr_num:
        continue
    else:
        if numbers[i + 1] != numbers[i]:
            total_bits += 5
        else:
            curr_num = numbers[i]
            total_bits += 9

print(total_bits)