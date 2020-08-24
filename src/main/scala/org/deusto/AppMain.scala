package org.deusto

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

object AppMain {

  val log: org.slf4j.Logger = LoggerFactory.getLogger(getClass)

  /**
    * Function that evaluates word count on a given text
    *
    * @param RDD[String] Collection of strings
    * @return RDD[(String, Int)] Key-value pairs containing word the occurrences
    */
  def wordcount(input: RDD[String]): RDD[(String, Int)] = {

    // TODO

  }

  /**
    * Function that locates the @ symbol precedent to entities
    *
    * @param RDD[String] Collection of strings
    * @return RDD[String] Collection of entities without the @
    */
  def findAts(input: RDD[String]): RDD[String] = {

    // TODO
  }

  /**
    * Function that filters a text finding just requested terms
    *
    * @param DataFrame Dataframe containing at least "word" column
    * @param List[String] List of strings to use as filter
    * @return Filtered Dataframe
    */
  def findAGA(input: DataFrame, words: List[String]): DataFrame = {

    // TODO
  }

  /**
    * Application entry point
    *
    * @param args Path to the document to process
    */
  def main(args: Array[String]): Unit = {

    /* Initiate the session */
    val spark = AppConfig.getSession("SparkTest")
    import spark.implicits._

    /* Get the information to process */
    val text = spark.sparkContext.textFile(args.apply(0))
    val wc = wordcount(text)
    val wc_df = wc.toDF("word", "count")
    wc_df.write.mode(SaveMode.Overwrite).saveAsTable("tweets_trump")

    /* Get different entities within tweets */
    val entities = findAts(text)
    val en_df = entities.distinct().toDF("entities")
    en_df.write.mode(SaveMode.Overwrite).saveAsTable("entities_trump")

    /* Close the session*/
    if (!spark.sparkContext.isStopped) {
      spark.sparkContext.stop()
      spark.close()
    }
  }
}
