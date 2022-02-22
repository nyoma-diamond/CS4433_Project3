import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

class Tests extends AnyFunSuite  with Logging with BeforeAndAfterAll with Serializable {
  val sc = new SparkContext(new SparkConf().setAppName("TEST").setMaster("local"))
  override def afterAll() {
    sc.stop()
  }

  test("run") {
    P1Q1.run(sc)
    P1Q2.run(sc)
  }
}
