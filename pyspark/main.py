from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("./data/input/SogouQ.txt")
    split_rdd = file_rdd.map(lambda x:x.split("\t"))

    split_rdd.persist(StorageLevel.DISK_ONLY)

    # 需求1
    context_rdd = split_rdd.map(lambda x:x[2])
    words_rdd = context_rdd.flatMap()
