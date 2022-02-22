import sys
import random

vals = 10000
if(len(sys.argv) > 1):
    vals = sys.argv[1]
else:
    print(f"Specify arg for custom data size. Using {vals} as default.")

vals = int(vals)

people = open("output/PEOPLE.csv", "w")
i_small = open("output/INFECTED-small.csv", "w")
i_large = open("output/INFECTED-large.csv", "w")

people.write("ID,X,Y\n")
i_small.write("ID,X,Y\n")
i_large.write("ID,X,Y\n")

for i in range(vals):
    write_str = f"{i}," + ",".join([str(random.randint(1, 10000)) for j in range(2)]) + "\n"
    people.write(write_str)
    if i % 1000 == 0:
        i_large.write(write_str)
        if i < vals/4:
            i_small.write(write_str)

people.close()
i_small.close()
i_large.close()
