import in_place

"""
Search a certain pattern in a file and replace it on the same file.
"""

if __name__ == "__main__":
    VALID_QUERIES_NAMES = set({
        # "Query1",
        # "Query2",
        # "Query3",
        # "Query3Stateful",
        "Query5",
        #"Query8",
        "Query11"
    })
    for q in VALID_QUERIES_NAMES:
        with in_place.InPlace("./db/" + q + '_equalv1.csv') as file:
            for line in file:
                line = line.replace(',>', '->')
                #line = line.replace('.log', '')
                file.write(line)