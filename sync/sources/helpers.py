import csv

from os.path import exists
from typing import List


def csv_to_list(filename) -> List:
    """
    Read CSV data from a temporary file defined in that `filename`
    and return it as a list.
    """

    if not exists(filename):
        return None

    with open(filename, "r") as file:
        try:
            reader = csv.reader(file)
        except Exception:
            reader = None

        return list(reader) if reader else []
