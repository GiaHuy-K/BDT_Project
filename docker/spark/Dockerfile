# Spark image + numpy: pyspark.ml (used by spark_jobs/forecast.py) imports it and apache/spark does not ship it.
FROM apache/spark:3.5.3
USER root
RUN python3 -m pip install --no-cache-dir numpy==1.24.4
USER spark
