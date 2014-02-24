import matplotlib.pyplot as plt
import numpy as np

lines = map(lambda l: l.split(), open("timeout/part-r-00000").readlines())
dates, counts = [], []
for l in lines:
    dates.append(l[0])
    counts.append(int(l[1]))

fig = plt.figure()
plt.title("Time series bar chart showing tweets/day")
plt.xlabel("Dates")
plt.ylabel("Number of tweets")

width = .55
ind = np.arange(len(counts))
plt.bar(ind, counts, color='aqua')
plt.xticks(ind + width / 2, dates)
fig.autofmt_xdate()
plt.show()
