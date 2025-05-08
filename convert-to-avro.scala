import org.apache.pinot.connector.spark.common.Logging
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.datasketches.theta.UpdateSketch

object ExamplePinotSketchBuilder extends Logging {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("theta-sketch-builder")
      .master("local[*]")
      .getOrCreate()

    generateSketches()
  }

  def generateSketches()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val inputPath = "new_input_file.csv"
    val outputPath = "/tmp/pinot_upload/sketches_data"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val dimensions = Seq(
      ("brand", "brand_id"),
      ("l1_category", "l1_category_id")
    )

    val expandedDF = dimensions.map { case (dimensionName, columnName) =>
      df.select(
        lit(dimensionName).alias("dimension_name"),
        col(columnName).cast("string").alias("dimension_value"),
        col("user_id").cast("string")
      )
    }.reduce(_ union _)

    val groupedDF = expandedDF
      .groupBy("dimension_name", "dimension_value")
      .agg(collect_set("user_id").alias("user_ids"))

    val buildSketchUDF = udf { (userIds: Seq[String]) =>
      val sketch = UpdateSketch.builder().build()
      if (userIds != null) {
        userIds.foreach { uid =>
          if (uid != null) sketch.update(uid)
        }
      }
      sketch.compact().toByteArray
    }

    val finalDF = groupedDF.withColumn("theta_sketch", buildSketchUDF(col("user_ids")))
      .select("dimension_name", "dimension_value", "theta_sketch")

    finalDF.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(outputPath)

    println(s"Sketches written as Avro to: $outputPath")
  }
}
