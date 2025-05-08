from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, collect_set, udf
from pyspark.sql.types import BinaryType
from datasketches import update_theta_sketch

def generate_sketches(spark):
    input_path = "new_input_file.csv"
    output_path = "pyspark_output"

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    dimensions = [("brand", "brand_id"), ("l1_category", "l1_category_id")]

    expanded_df = None
    for dimension_name, column_name in dimensions:
        temp_df = df.select(
            lit(dimension_name).alias("dimension_name"),
            col(column_name).cast("string").alias("dimension_value"),
            col("user_id").cast("string")
        )
        expanded_df = temp_df if expanded_df is None else expanded_df.union(temp_df)

    grouped_df = expanded_df.groupBy("dimension_name", "dimension_value").agg(
        collect_set("user_id").alias("user_ids")
    )

    def build_sketch(user_ids):
        if user_ids:
            sketch = update_theta_sketch()
            for uid in user_ids:
                if uid:
                    sketch.update(uid.encode("utf-8")) 
            return sketch.compact().serialize()
        return b'' 

    build_sketch_udf = udf(build_sketch, BinaryType())

    final_df = grouped_df.withColumn("theta_sketch", build_sketch_udf(col("user_ids"))) \
                          .select("dimension_name", "dimension_value", "theta_sketch")

    final_df.write.format("avro").mode("overwrite").save(output_path)

    print(f"Sketches written as Avro to: {output_path}")

if __name__ == "__main__":
    # Create the SparkSession with the Avro dependency
    spark = SparkSession.builder \
        .appName("theta-sketch-builder") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.2") \
        .getOrCreate()

    generate_sketches(spark)
