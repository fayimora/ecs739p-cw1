lines = open("supportout/part-r-00000").readlines()

data = []
for line in lines:
    line = line.split()
    data.append((line[0], int(line[1])))

sorted_data = sorted(data, key=lambda tup: tup[1])

for t in sorted_data:
    print t[0], t[1]
