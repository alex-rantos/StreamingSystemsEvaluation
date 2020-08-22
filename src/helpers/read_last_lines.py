import os, sys


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Need 2 exactly arguments (file_path and number of lines). Exiting "
            + sys.argv[0]
        )

    file_path = sys.argv[1]
    nolines = int(sys.argv[2])

    with open(file_path, 'r') as fp:
        lines = fp.readlines()
        print(len(lines))
        for l in lines[-nolines:]:
            print(l, end='')
