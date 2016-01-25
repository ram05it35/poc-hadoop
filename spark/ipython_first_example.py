def split_words(line):
	return line.split()


def create_pair(word):
	return (word, 1)


text_RDD = sc.textFile("/user/cloudera/data/stars_war.txt")

pairs_RDD = text_RDD.flatMap(split_words).map(create_pair)
