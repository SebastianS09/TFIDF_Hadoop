text_raw = sc.textFile("/user/hadoop/docgen/input/5000-8.txt")
text_raw.take(5)

#Cleaning text and removing punctuation:

text_clean = text_raw.map(lambda x: ''.join([ c for c in x if (c.isalnum() or c==' ')]))
text_clean = text_clean.filter(lambda x: x != '').map(lambda x: x.lower())
words = text_clean.flatMap(lambda x: x.split())
doc_nb = 5
sample_size = 0.001

documents = []
documents = [(words.sample(False,sample_size)) for i in range(doc_nb)]
documents = [sc.parallelize(documents[i].map(lambda x: (i,x)).collect()) for i in range(doc_nb)]
documents = sc.union(documents)
doclist = documents.groupByKey().map(lambda x: list(x[1]))

import math

doc_word_count = documents.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y: x+y)
occ_count = doc_word_count.map(lambda x: (x[0][1],1)).reduceByKey(lambda x,y: x+y)

IDF = occ_count.map(lambda x : (x[0],math.log(doc_nb/x[1])))
#doc_word_count.sortBy(lambda x: x[1],False).collect()
joined = doc_word_count.map(lambda x: (x[0][1],(x[0][0],x[1]))).join(IDF)
TFIDF_scores = joined.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1])))
TFIDF_scores.collect()
