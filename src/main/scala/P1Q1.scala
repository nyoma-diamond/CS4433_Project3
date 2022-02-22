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

    val columns = Seq("ID","X","Y")

    val infected = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv("data/output/INFECTED-small.csv")
      .cache()

    val people = spark.read
      .option("delimiter",",")
      .option("header","true")
      .option("inferschema","true")
      .csv("data/output/PEOPLE.csv")

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
}
