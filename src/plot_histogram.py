import matplotlib.pyplot as plt

lines = map(lambda l: l.split(), open("out/part-r-00000").readlines())
tweet_lengths = map(lambda arr: int(arr[0]), lines)
length_counts = map(lambda arr: int(arr[1]), lines)

avg = sum([x * y for x, y in zip(tweet_lengths, length_counts)]) / sum(length_counts)
print "Mean length of characters is", avg

plt.hist(tweet_lengths, 50, facecolor='green', alpha=0.5, range=(1, 1000))
plt.ylim(0, 1000)
plt.title("Tweet Length Histogram")
plt.xlabel("Tweet Length")
plt.ylabel("Occurence")
plt.show()
