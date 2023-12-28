import json
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile(r"D:\Code\Project\bigdata\data\input\order.txt")
    
    # 将数据按"|"分开得到json文件
    json_rdd = file_rdd.flatMap(lambda x: x.split('|'))

    # 将json数据转字典
    dict_rdd = json_rdd.map(lambda json_str: json.loads(json_str))
    # print(dict_rdd.collect())

    # 过滤areaname=北京的数据
    beijing_rdd = dict_rdd.filter(lambda d: d['areaName']=='北京')

    # 去重
    category_rdd = beijing_rdd.map(lambda x:(x['areaName'], x['category']))

    result_rdd = category_rdd.distinct()

    print(result_rdd.collect())

# [('北京', '手机'), ('北京', '电脑'), ('北京', '家具'), ('北京', '食品'), ('北京', '服饰')
