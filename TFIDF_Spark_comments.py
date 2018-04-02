
#Loading the text file into a RDD
text_raw = sc.textFile("/user/hadoop/docgen/input/5000-8.txt")

#Cleaning the text file:
##removing non alphanumerical characters
##removing uppercase letters
text_clean = text_raw.map(lambda x: ''.join([ c for c in x if (c.isalnum() or c==' ')]))
text_clean = text_clean.filter(lambda x: x != '').map(lambda x: x.lower())

#Creating the documents
##Splitting the text into words:
words = text_clean.flatMap(lambda x: x.split())

##Defining Sample and Doc Size
doc_nb = doc
sample_size = samp

##Creating empty list
documents = []

## Sampling the input text as often as required
documents = [(words.sample(False,sample_size)) for i in range(doc_nb)]

##Coalescing the samples into a RDD
documents = [sc.parallelize(documents[i].map(lambda x: (i,x)).collect()) for i in range(doc_nb)]
documents = sc.union(documents)

#TFIDF 
##Counting the number of words per document for TF and joining on documents
doc_word_number = documents.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
documents_w_count = documents.join(doc_word_number)

##Computing TF
doc_word_count = documents_w_count.map(lambda x: ((x[0],x[1][0]),float(1)/x[1][1])).reduceByKey(lambda x,y: x+y)

##Computing occurence (number of documents with a certain word in the corpus for IDF) 
occ_count = doc_word_count.map(lambda x: (x[0][1],1)).reduceByKey(lambda x,y: x+y)

##Computing the IDF score 
IDF = occ_count.map(lambda x : (x[0],math.log(doc_nb/x[1])))

##Computing the final TF-IDF score
joined = doc_word_count.map(lambda x: (x[0][1],(x[0][0],x[1]))).join(IDF)
TFIDF_scores = joined.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1]))
