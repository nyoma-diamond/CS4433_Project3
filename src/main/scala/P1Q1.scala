
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession

//object RunMain extends  Serializable{
//  def main(args: Array[String]): Unit = {
//    val spark = init_sc()
//    val sc = spark.sparkContext
//    P1Q1.run(sc)
//    sc.stop()
//  }
//
//  def init_sc(): SparkSession ={
//    val conf = new SparkConf().setAppName("cs585")
//    conf.set("spark.sql.parquet.compression.codec", "uncompressed")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val spark = SparkSession
//      .builder()
//      .config(conf)
//      .getOrCreate()
//    spark
//  }
//}


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object P1Q1 extends Serializable {
  // https://www.geeksforgeeks.org/find-if-a-point-lies-inside-or-on-circle/
  def isInside(circle_x: Int, circle_y: Int, rad: Int, x: Int, y: Int): Boolean = { // Compare radius of circle with
    // distance of its center from
    // given point
    if ((x - circle_x) * (x - circle_x) + (y - circle_y) * (y - circle_y) <= rad * rad) true
    else false
  }

  def run(sc: SparkContext): Unit = {
    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._

    val data = spark.read
//      .format("csv")
//      .option("sep",",")
      .textFile("data/output/INFECTED-small.csv")
      .cache()

    val result = data.flatMap(row => {
      val values = row.split(",")

      val x = values(1).toInt
      val y = values(2).toInt

      var seq = Seq[String]()

      for (i <- x-6 to x+6) {
        for (j <- y-6 to y+6) {
          if (isInside(x, y, 6, i, j)) {
            seq = seq :+ i.toString + "," + j.toString
          }
        }
      }

      seq
    })

    result.foreach(x => println(x))
  }
}
