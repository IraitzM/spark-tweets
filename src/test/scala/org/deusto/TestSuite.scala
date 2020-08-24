package org.deusto

// import required spark classes
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

class TestSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll with DataFrameSuiteBase with Matchers {

  // Disable log options
  Logger.getRootLogger.setLevel(Level.OFF)

  @transient var ss: SparkSession = _

   override def beforeAll(): Unit = {

    ss = AppConfig.getLocalSession()
  }

  // Word count
  test("Test word count") {

    /* First test to see if we can get the word count*/
    val inputRDD = ss.sparkContext.parallelize(Array("This is a tweet","This is another tweet"))

    /* Let's evaluate */
    val wc = AppMain.wordcount(inputRDD)

    /* Expected output */
    assert(wc.count() == 5)
    val map = wc.collect().toMap
    assert(map.getOrElse("This", 0) == 2)
    assert(map.getOrElse("a", 0) == 1)
  }

  // Find @s
  test("Test find @s") {

    /* Test to see if we find entities */
    val inputRDD = ss.sparkContext.parallelize(Array("This is a tweet for @Deusto","This is another tweet @Deusto"))

    /* Let's evaluate*/
    val ats = AppMain.findAts(inputRDD)

    /* Expected output */
    assert(ats.count() == 2)
    assert(ats.first() == "Deusto")
  }

  // Find AMERICA GREAT AGAIN
  test("Find the occurrences of AMERICA GREAT AGAIN") {

    /* Test to see if we can find our words of interest */
    val data = Seq(
      Row("AMERICA", 3),
      Row("GREAT", 4),
      Row("AGAIN", 2),
      Row("tweet",1)
    )
    val schema = List(
      StructField("word", StringType, nullable = true),
      StructField("count", IntegerType, nullable = true)
    )
    val inputDF = ss.createDataFrame(
      ss.sparkContext.parallelize(data),
      StructType(schema)
    )

    /* Filter to be applied */
    val list = List("AMERICA","GREAT","AGAIN")

    /* Filter data */
    val df = AppMain.findAGA(inputDF, list)

    /* Check expected output */
    assert(df.count() == 3)
  }

  override def afterAll(): Unit = {
    ss.close()
  }
}