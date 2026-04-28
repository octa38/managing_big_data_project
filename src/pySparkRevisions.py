from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, split,regexp_replace, when,concat_ws


spark = SparkSession.builder.appName("example").getOrCreate()
singleRevision = spark.read.json("/user/s2539829/SHARED_MBD/rev_data")
singleRevision = singleRevision.withColumn("text", when(col("text").isNotNull(), regexp_replace(col("text"), r'[^\w\s.,!?]', ' ')))
newTable = singleRevision.select("date", "text", "to_id")

newTable = newTable.withColumn("text_length", length(col("text")))
newTable = newTable.withColumn("date_to_id", concat_ws("_", col("to_id"), col("date")))
newTable = newTable.groupBy("date_to_id").sum("text_length")

newTable = newTable.withColumn("to_id", split(col("date_to_id"), "_")[0])
newTable = newTable.withColumn("date", split(col("date_to_id"), "_")[1])
newTable = newTable.drop("date_to_id")

newTable.write.mode("overwrite").csv("/user/s2539829/SHARED_MBD/rev_data_csv")





