import bz2

bz2_file = "1.207324924.bz2"

# Open the bz2 file for reading
with bz2.BZ2File(bz2_file, "r") as bz2_file:
    # Read the contents of the bz2 file line by line
    line = bz2_file.readline()
    while line:
        print(line)
        line = bz2_file.readline()
