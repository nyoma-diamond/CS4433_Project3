import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object P2 extends Serializable {
  val pur_schema: StructType = StructType(Array(
    StructField("TransID",StringType),
    StructField("CustID",StringType),
    StructField("TransTotal",FloatType),
    StructField("TransNumItems",IntegerType),
    StructField("TransDesc",StringType)
  ))

  def T1(sc: SparkContext): Unit = {
    val spark = SparkSession.builder.getOrCreate()

    val purchases = spark.read
      .option("delimiter",",")
      .option("header","false")
      .schema(pur_schema)
      .csv("data/output/PURCHASES.csv")

    purchases.createOrReplaceTempView("purchases")

    spark.sql("SELECT * FROM purchases WHERE TransTotal > 600").show()
  }

  def T2(sc: SparkContext): Unit = {
    val spark = SparkSession.builder.getOrCreate()

    val purchases = spark.read
      .option("delimiter",",")
      .option("header","false")
      .schema(pur_schema)
      .csv("data/output/PURCHASES.csv")

    purchases.createOrReplaceTempView("purchases")

    spark.sql("SELECT TransNumItems, min(TransTotal), max(TransTotal) FROM purchases WHERE TransTotal > 600 GROUP BY TransNumItems").show()
    // TODO: Median
  }
}
