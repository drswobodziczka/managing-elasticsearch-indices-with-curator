from typing import List


def chunks(list: List, slice_size: int):
  for i in range(0, len(list), slice_size):
    yield list[i:i + slice_size]


for chunk in chunks([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2):
  print(chunk)
