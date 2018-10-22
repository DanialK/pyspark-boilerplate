python setup.py bdist_spark
spark-submit --py-files spark_dist/test_spark_submit-0.1-deps.zip,spark_dist/test_spark_submit-0.1.zip src/driver.py src.train.model