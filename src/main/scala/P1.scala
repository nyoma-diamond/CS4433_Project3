import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object P1 extends Serializable {
  var path: String = ""

  // https://www.geeksforgeeks.org/find-if-a-point-lies-inside-or-on-circle/
  def isInside(circle_x: Int, circle_y: Int, rad: Int, x: Int, y: Int): Boolean = { // Compare radius of circle with
    // distance of its center from
    // given point
    if ((x - circle_x) * (x - circle_x) + (y - circle_y) * (y - circle_y) <= rad * rad) true
    else false
  }

  def Q1(): Unit = {
    val spark: SparkSession = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val infected = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv(path+"/INFECTED-small.csv")
      .cache()

    val people = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv(path+"/PEOPLE.csv")

    infected.flatMap(row => {
      val x = row.getAs[Int](1)
      val y = row.getAs[Int](2)

      var seq = Seq[(Int, Int)]()

      for (i <- x-6 to x+6) {
        for (j <- y-6 to y+6) {
          if (isInside(x, y, 6, i, j)) {
            seq = seq :+ (i, j)
          }
        }
      }

      seq
    }).toDF("X","Y")
      .join(people, Seq("X","Y"))
      .select("ID")
      .show()
  }

  def Q2(): Unit = {
    val spark: SparkSession = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val infected = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv(path+"/INFECTED-large.csv")

    val people = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv(path+"/PEOPLE.csv")

    infected.flatMap(row => {
      val id = row.getAs[Int](0)
      val x = row.getAs[Int](1)
      val y = row.getAs[Int](2)

      var seq = Seq[(Int, Int, Int)]()

      for (i <- x-6 to x+6) {
        for (j <- y-6 to y+6) {
          if (isInside(x, y, 6, i, j)) {
            seq = seq :+ (id, i, j)
          }
        }
      }

      seq
    }).toDF("ID_inf","X","Y")
      .join(people, Seq("X","Y"))
      .groupBy("ID_inf")
      .count()
      .map(row => (row.getAs[Int](0), row.getAs[Long](1)-1))
      .toDF("infect_i","count-close-contacts-of-infect-i")
      .show()
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
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    val sc = spark.sparkContext
    path = args(0)
    Q1()
    Q2()
    sc.stop()
  }
}
