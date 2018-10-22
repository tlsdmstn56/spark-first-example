import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{udf, unix_timestamp, date_format}

object GreenbuttonETL {
  def main(args: Array[String]) {
    if (args.length != 2) {
        println("ERROR need file path to convert and output file name")

    } else {
      val csvFile = args(0) // Should be some file on your system
      val spark = SparkSession.builder.appName("GreenButton ETL").getOrCreate()
      import spark.implicits._
      /* Loading CSV file */
      val rddOrigin = spark.read
        .format("csv")
        .option("header","false")
        .load(csvFile).toDF()

      val colNames = Seq("usage_point_id","reading_type","start_time", "interval","value")
      val rddOriginWithColNames = rddOrigin.toDF(colNames: _*)

      val rddWithTimeStamp = rddOriginWithColNames.withColumn("unix_timestsamp",unix_timestamp($"start_time"))


      val rddCast = rddWithTimeStamp.select(
          rddWithTimeStamp.columns.map {
          case start_time @ "start_time" => rddWithTimeStamp(start_time)
          case unix_timestsamp @ "unix_timestsamp" => rddWithTimeStamp(unix_timestsamp)
          case other => rddWithTimeStamp(other).cast(IntegerType).as(other)
        }: _*
      )
      def fixTimeZone: Int => Int = _ - 3600*8
      val udfFixTimeZone = udf(fixTimeZone)
      import org.apache.spark.sql.types.{TimestampType, DateType}
      val rddTZPDT = rddCast.withColumn("unix_timestsamp", udfFixTimeZone($"unix_timestsamp").cast(TimestampType))
      val rddNewStartDate = rddTZPDT.withColumn("date", rddTZPDT("unix_timestsamp").cast(DateType))
      val rdd203 = rddNewStartDate.filter($"reading_type" === 203)
      val rdd203DropDup = rdd203.dropDuplicates("usage_point_id","unix_timestsamp")
      val rdd203AddTime = rdd203DropDup.withColumn("time", date_format($"unix_timestsamp","HH:mm"))
      val reshapedRdd = rdd203AddTime.groupBy($"usage_point_id",$"date").pivot("time").max("value")
      val rddAddDateAndDOW = reshapedRdd
          .withColumn("month", date_format($"date","M"))
          .withColumn("dayofweek", date_format($"date","E"))
      val columnSorted = rddAddDateAndDOW.columns.sorted
      val columnNames = Array("usage_point_id","date","dayofweek","month")++columnSorted.slice(0,96)
      val rddColReordered = rddAddDateAndDOW.select(columnNames.head, columnNames.tail :_*)
      rddColReordered.coalesce(1).write.csv(args(1))
      spark.stop()
    }
    
  }
}



