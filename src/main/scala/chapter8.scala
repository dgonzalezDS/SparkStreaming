import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.source.image
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger

object chapter8 {
  def ejercicio1()(implicit spark: SparkSession): Unit = {

    val lines = spark
      .readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.select(split(col("value"), "\\s").as("word"))
    val counts = words.groupBy("word").count()

    val streamingQuery = counts.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation","/tmp/checkpoint")
      .start()
    streamingQuery.awaitTermination()
  }

  def ejercicio2()(implicit spark: SparkSession): Unit = {
    /*
    PDTE DE ARREGLAR
     */

    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "events")
      .load()

    inputDF.show()
  }

  def ejercicio3()(implicit spark: SparkSession): Unit = {
    /*
    Cargamos el dataset de departures delays y lo troceamos en 3 trozos similares
     */

    val schema = StructType(Array(
      StructField("date", StringType, true),  //
      StructField("delay", IntegerType, true),
      StructField("distance", IntegerType, true),
      StructField("origin", StringType, true),
      StructField("destination", StringType, true)
    ))

    val csvFile = "data/departuredelays.csv"
    val df = spark.read.format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .schema(schema)
      .load(csvFile)

    val partitions = df.randomSplit(Array(0.33, 0.33, 0.34))
    partitions(0).coalesce(1).write.option("header", "true").csv("data//dataset_part1")
    partitions(1).coalesce(1).write.option("header", "true").csv("data//dataset_part2")
    partitions(2).coalesce(1).write.option("header", "true").csv("data//dataset_part3")
  }

  def ejercicio4(): Unit = {

    // Crear sesión de Spark con Delta Lake
    val spark = SparkSession.builder()
      .appName("Spark Streaming ETL")
      .master("local[*]")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    // Ruta donde se almacenan los archivos en streaming
    val inputPath = "file:/C:/Users/david.gternero/IdeaProjects/SparkStreaming/data/dataset_part"

    val schema = StructType(Array(
      StructField("date", StringType, true),  //
      StructField("delay", IntegerType, true),
      StructField("distance", IntegerType, true),
      StructField("origin", StringType, true),
      StructField("destination", StringType, true)
    ))

    val streamingDF = spark.readStream
      .option("header", "true")
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .load(inputPath)

    // Añadir columna timestamp con fecha y hora actuales
    val dfWithTimestamp = streamingDF.withColumn("ingestion_time", current_timestamp())

    // Transformación: Agrupar por categoría y contar elementos
    val transformedDF = dfWithTimestamp
      .groupBy("origin")
      .agg(count("*").alias("total_registros"))

    // Mostrar en consola
    val queryConsole = transformedDF.writeStream
      .outputMode("complete") // Como usamos groupBy, el modo debe ser "complete"
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "file:///tmp/checkpoints_streaming")//
      .start()

    queryConsole.awaitTermination()
    /*
    // Escribir en formato Delta
    val outputPath = "data"

    val queryDelta = transformedDF.writeStream
      .outputMode("complete")
      .format("delta")
      .option("checkpointLocation", "file:///tmp/checkpoints")
      .start(outputPath)

    queryConsole.awaitTermination()
    queryDelta.awaitTermination()
    */

    spark.stop()
  }
}