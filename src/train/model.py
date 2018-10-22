import sys
from pyspark import SparkContext, SQLContext
import pulp  # this import is just to check that deps were distributed correctly


def process(sc, args):
    input_data = args if args else [1, 2, 3, 4, 5]
    distr_data = sc.parallelize(input_data)
    result = distr_data.collect()
    return result

def main(args):
    app_name = 'pyspak-boilerplate'

    print('initialize context')
    sc = SparkContext(appName=app_name)
    print(sum(process(sc, args)))
    sc.stop()


if __name__ == '__main__':
    sys.exit(main(sys.argv))