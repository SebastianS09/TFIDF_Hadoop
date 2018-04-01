import math
import time

docs = [2,5,10,20,50,100]
samples = [0.001,0.002,0.005,0.01,0.05,0.1]
samples_2 = [0.001,0.002,0.01,0.05,0.1]   #because we already did it with 0.005 

def TFIDF(doc,samp):
    t0 = time.time()
    text_raw = sc.textFile("/user/hadoop/docgen/input/5000-8.txt")
    text_clean = text_raw.map(lambda x: ''.join([ c for c in x if (c.isalnum() or c==' ')]))
    text_clean = text_clean.filter(lambda x: x != '').map(lambda x: x.lower())
    words = text_clean.flatMap(lambda x: x.split())
    doc_nb = doc
    sample_size = samp
    documents = []
    documents = [(words.sample(False,sample_size)) for i in range(doc_nb)]
    documents = [sc.parallelize(documents[i].map(lambda x: (i,x)).collect()) for i in range(doc_nb)]
    documents = sc.union(documents)
    doclist = documents.groupByKey().map(lambda x: list(x[1]))
    doc_word_count = documents.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y: x+y)
    occ_count = doc_word_count.map(lambda x: (x[0][1],1)).reduceByKey(lambda x,y: x+y)
    IDF = occ_count.map(lambda x : (x[0],math.log(doc_nb/x[1])))
    joined = doc_word_count.map(lambda x: (x[0][1],(x[0][0],x[1]))).join(IDF)
    TFIDF_scores = joined.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1])))
    filename = str(doc)+'_'+str(samp)+'_'"file.csv"
    TFIDF_scores.saveAsTextFile(filename)
    t1 = time.time()
    print("elasped time for","doc numb: ",doc_nb,"sample_size: ",sample_size,"time: ",t1-t0)
    
for i in docs:
    TFIDF(i,0.005)
        
for j in samples_2:
    TFIDF(10,j)
