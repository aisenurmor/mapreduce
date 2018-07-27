from pyspark import SparkContext, SparkConf
from pyspark import sql
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import re

conf=SparkConf().setMaster("local[2]").setAppName("XmlMapReduce")
sc=SparkContext(conf=conf)

dataFile=sc.textFile("/home/aisenur/Datasets/Postsnew2")
header=dataFile.first()

dataFile=dataFile.filter(lambda x: x!=header)
dataFile=dataFile.map(lambda x: x.replace("<", ""))
dataFile=dataFile.map(lambda x: x.replace(">", ","))
dataFile = dataFile.map(lambda x: x.split(" ", 4))
dataFile=dataFile.map(lambda x: (x[0].split(","),x[1],x[2],x[3]))
dataFile=dataFile.flatMap(lambda x: [(k,x[1],x[2],x[3]) for k in x[0]])
dfPostFav=dataFile.map(lambda x: (x[0],int(x[1]))).combineByKey(lambda x: (x,1),
                                lambda x,y: (x[0] + y, x[1] + 1),
                                lambda x,y: (x[0] + y[0], x[1] + y[1])).collect()
dfPostComment=dataFile.map(lambda x: (x[0],int(x[2]))).combineByKey(lambda x: (x,1),
                                lambda x,y: (x[0] + y, x[1] + 1),
                                lambda x,y: (x[0] + y[0], x[1] + y[1])).collect()
dfPostView=dataFile.map(lambda x: (x[0],int(x[3]))).combineByKey(lambda x: (x,1),
                                lambda x,y: (x[0] + y, x[1] + 1),
                                lambda x,y: (x[0] + y[0], x[1] + y[1])).collect()

#for value in dfPostFav:
#    print(value)
#print("--------")
#output=dataFile.collect()
#for value in dfPostComment:
#    print(value)
#print("---------")
#output=dataFile.collect()
#for value in dfPostView:
#    print(value)
#print("-----------")


dataFile2=sc.textFile("/home/aisenur/Datasets/Tagsnew")
header2=dataFile2.first()

dataFile2=dataFile2.filter(lambda x: x!=header2)
dataFile2=dataFile2.map(lambda x: x.split(" "))

sqlContext1 = sql.SQLContext(sc)
dfPostFav = sqlContext1.createDataFrame(dfPostFav, ["postTag1", "FavoriteCount"])
#dfPostFav.show()
tf=dfPostFav.alias('tf')

sqlContext1 = sql.SQLContext(sc)
dfPostComment = sqlContext1.createDataFrame(dfPostComment, ["postTag2", "CommentCount"])
#dfPostComment.show()
tc=dfPostComment.alias('tc')

sqlContext1 = sql.SQLContext(sc)
dfPostView = sqlContext1.createDataFrame(dfPostView, ["postTag3", "ViewCount"])
#dfPostView.show()
tv=dfPostView.alias('tv')

join_post=tf.join(tc, tf.postTag1 == tc.postTag2, how='left').select([col('tc.'+xx) for xx in tc.columns]
                                                                     + [col('tf.FavoriteCount')])
tj=join_post.alias('tj')
#tj.show()
join_post1=tj.join(tv, tj.postTag2 == tv.postTag3, how='left').select([col('tv.'+xx) for xx in tv.columns]
                                                                      + [col('tj.CommentCount'),col('tj.FavoriteCount')])
tg=join_post1.alias('tg')
#join_post1.show()

sqlContext2 = sql.SQLContext(sc)
dfTag = sqlContext2.createDataFrame(dataFile2, ["tagId", "Tag", "Count"])
#dfTag.show()
tb=dfTag.alias('tb')

inner_join = tb.join(tg, tb.Tag == tg.postTag3, how='Right').select([col('tg.'+xx) for xx in tg.columns]
                                                                    + [col('tb.tagId'),col('tb.Count')])
inner_join=inner_join.sort(col("Count"))

print("-------------------------------    SONUC    -----------------------------")
print("\n\n")

inner_join.show(inner_join.count())


#inner_join=inner_join.rdd
#inner_join.coalesce(1, True).saveAsTextFile("FilePostTag1")

#inner_join.coalesce(1).write.option("header", "true").csv("fileshg.csv")
