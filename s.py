from pyspark import SparkContext
import sys
assert sys.version_info >= (3,5)
import pyspark as spark


def main(inputs, output):
    rdd = sc.textFile(inputs)
    # 1 READ
    splits = rdd.map(lambda line: line.split(" "))
    # 2 CAST
    splits = splits.map(lambda a: [ *a[:3], *[int(a[3])]] )
    date = splits.collect()[0][0]
    # 3 FILTER
    splits = splits.filter(lambda a: a[1] == 'en')
    splits = splits.filter(lambda a: a[2] != 'Main_Page')
    splits = splits.filter(lambda a: a[2][:8] != 'Special:')
    # 4 KEY-VAL PAIR 
    tuples = splits.map(lambda a: (a[0],a[3]))
    # 5 reduce by key
    red = tuples.reduceByKey(lambda a, b: max(a,b))
    sortedred = red.sortByKey( False )

    sortedred.map(tab_separated).saveAsTextFile(output)

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    print("BEGIN")
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


