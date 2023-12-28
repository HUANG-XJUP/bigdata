# coding:utf8
# 将rdd的数据写入文本文件中，支持本地、hdfs系统

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)

    rdd.saveAsTextFile("hdfs://node1:8020/output/out1")
