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
    P1.main(Array("data/output"))
    P2.main(Array("data/output"))
  }
}
