import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object P2 extends Serializable {
  var path: String = ""

  val pur_schema: StructType = StructType(Array(
    StructField("TransID",IntegerType),
    StructField("CustID",StringType),
    StructField("TransTotal",FloatType),
    StructField("TransNumItems",IntegerType),
    StructField("TransDesc",StringType)
  ))

  val cus_schema: StructType = StructType(Array(
    StructField("ID",IntegerType),
    StructField("Name",StringType),
    StructField("Age",IntegerType),
    StructField("CountryCode",IntegerType),
    StructField("Salary",FloatType)
  ))

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def loadTables(): Unit = {
    spark.read
      .option("delimiter",",")
      .option("header","false")
      .schema(pur_schema)
      .csv(path+"/PURCHASES.csv")
      .createOrReplaceTempView("purchases")

    spark.read
      .option("delimiter",",")
      .option("header","false")
      .schema(cus_schema)
      .csv(path+"/CUSTOMERS.csv")
      .createOrReplaceTempView("customers")
  }

  def T1(): Unit = {
    spark.sql("SELECT * FROM purchases WHERE TransTotal > 600").createOrReplaceTempView("T1")
  }

  def T2(): Unit = {
    spark.sql("SELECT TransNumItems, avg(TransTotal), min(TransTotal), max(TransTotal) FROM T1 GROUP BY TransNumItems").show()
    // TODO: Median
  }

  def T3(): Unit = {
    spark.sql(
      "SELECT ID, Age, SUM(TransNumItems) AS TotalItems, SUM(TransTotal) AS TotalSpent " +
        "FROM T1 JOIN customers " +
        "ON customers.ID = T1.CustID " +
        "AND Age < 25 AND Age > 18 " +
        "GROUP BY ID, Age"
    ).createOrReplaceTempView("T3")
  }


  def T4(): Unit = {
    spark.sql(
      "SELECT  c1.ID AS c1ID, c2.ID AS c2ID, " +
          "c1.Age AS Age1, c2.Age AS Age2, " +
          "c1.TotalItems AS TotalItemCount1, c2.TotalItems AS TotalItemCount2, " +
          "c1.TotalSpent AS TotalAmount1, c2.TotalSpent AS TotalAmount2 " +
        "FROM T3 AS c1, T3 AS c2 " +
        "WHERE c1.ID <> c2.ID AND c1.Age < c2.Age AND c1.TotalSpent > c2.TotalSpent AND c1.TotalItems < c2.TotalItems"
    ).show()
  }

  def init_sc(): SparkSession = {
    val conf = new SparkConf().setAppName("cs4433")
    conf.set("spark.sql.parquet.compression.codec", "uncompressed")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = init_sc()
    val sc = spark.sparkContext
    path = args(0)
    loadTables()
    T1()
    T2()
    T3()
    T4()
    sc.stop()
  }
}
