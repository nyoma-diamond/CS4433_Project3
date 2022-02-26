import org.apache.spark.sql.SparkSession

object P1 extends Serializable {
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  import spark.implicits._

  var path: String = ""

  // https://www.geeksforgeeks.org/find-if-a-point-lies-inside-or-on-circle/
  def isInside(circle_x: Int, circle_y: Int, rad: Int, x: Int, y: Int): Boolean = { // Compare radius of circle with
    // distance of its center from
    // given point
    if ((x - circle_x) * (x - circle_x) + (y - circle_y) * (y - circle_y) <= rad * rad) true
    else false
  }

  def Q1(): Unit = {
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
      .show(1000)
//      .collect()
//      .map(row => row.getInt(0))
  }

  def Q2(): Unit = {
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
      .show(1000)
  }

  def main(args: Array[String]): Unit = {
    path = args(0)
    Q1()
    Q2()
  }
}
