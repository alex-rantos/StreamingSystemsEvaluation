import csv
import re
import ntpath
import settings
from typing import List


class MyDB():

    EXTENSION = "_"

    valid_query_names = settings.VALID_QUERIES_NAMES

    def __init__(self, extension=""):
        if extension != "": self.EXTENSION += extension

    def store(self, identifier: str, data: List[str], mode="a") -> None:
        """
        Store to the corresponding csv file (based on @identifier) the @data
        If you pass an empty List @data and set mode = "w+", deletes all content of file.
        """
        if identifier not in self.valid_query_names:
            print("--------ERROR: WRONG IDENTIFIER:" + identifier +
                  " @ myDB::add_file_contents")
            return

        full_file_path = settings.PATH_TO_DB + identifier + self.EXTENSION + ".csv"

        with open(full_file_path, mode) as fp:
            if data:
                writer = csv.writer(fp)
                writer.writerow(data)
                print("storing to csv " + full_file_path)

    def add_file_contents(self, args: List[str], file_path: str, mode="a"):
        if args[0] not in self.valid_query_names:
            print("--------ERROR: WRONG IDENTIFIER:" + args[0] +
                  " @ myDB::add_file_contents")
        with open(file_path, 'r') as fp:
            # Every log file from ds2 has only 1 line
            line = fp.read()
            line = line.strip()  # remove /n from the end
            results = line.split(',')            
            nums = re.findall('\d+', ntpath.basename(file_path))
            data = [results[0], str(nums[1])] + args[1:] + results[1:]

            print('DATA INSERTED: ' + str(data) + " @ file: " + file_path)
            # Data format: Query1 [specific metrics...] [log results]
            self.store(args[0], data, mode)


if __name__ == "__main__":
    d = MyDB()
    # clear query_2.csv
    d.store("Query2", [], "w+")
    # add data to 3s
    d.store("Query3s", ["2d", "2da"], "w+")
