package enron

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class EnronEmail(mid: Int = 0, sender: String, date: String = "", messageId: String = "",
                      subject: String, body: String, folder: String = "") {
  /**
    * @return Whether the email body or subject contains `keyword` or not
    * @param keyword Language to look for (e.g. "fraud")
    */
  def containsKeyword(keyword: String): Boolean = {
    body.contains(keyword) || subject.contains(keyword)
  }
}

object EnronAnalysis {

  // See: Exploratory Data Analysis of Enron Emails
  // https://www.stat.berkeley.edu/~aldous/Research/Ugrad/HarishKumarReport.pdf
  // Topic 1 contains a lot of meeting related words, perhaps they are from
  // emails that were sent as meeting notices
  val topic1 = List(
    "message", "origin", "pleas", "email", "thank", "attach", "file", "copi",
    "inform", "receiv")
  // Topic 2 while related to business seems to be more about the process
  // rather than the content of the core business. It has a lot of terms
  // relevant to business legalities.
  val topic2 = List(
    "enron", "deal", "agreement", "chang", "contract", "corp", "fax", "houston",
    "date", "america")
  // Topic 3 contains words that are directly related to the core business of
  // Enron like ”gas”, ”power” etc.
  val topic3 = List(
    "market", "gas", "price", "power", "compani", "energi", "trade", "busi",
    "servic", "manag")
  // Topic 4 also seems to be meeting-related but in a more casual tone and
  // setting.
  val topic4 = List(
    "thank", "call", "time", "meet", "look", "week", "day", "don",
    "vinc", "talk")

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Enron")
      .config("spark.master", "local")
      .getOrCreate()

  val sc = spark.sparkContext

  val enronRdd: RDD[EnronEmail] = EnronData.readEnronEmail(spark)

  /** Returns the number of emails sent by each user
    * Sorted by the number of emails per user in the descending order
   *  Hint1: consider using method `reduceByKey` on RDD[T].
   *  Hint2: consider using method `sortBy`
   */
  def emailsPerUser(rdd: RDD[EnronEmail]): Array[(String, Int)] = {
    ???
  }

  /** Returns the number of emails on which the keyword `keyword` occurs.
    *  Hint1: consider using method `aggregate` on RDD[T].
    *  Hint2: consider using method `containsKeyword` on `EnronEmail`
    */
  def occurrencesOfKeyword(lang: String, rdd: RDD[EnronEmail]): Int = {
    ???
  }

  /** Compute an inverted index of the set of emails, mapping each keyword
   * to the Enron emails in which it occurs.
   */
  def makeInvertedIndex(keywords: List[String], rdd: RDD[EnronEmail]): RDD[(String, Iterable[EnronEmail])] = {
    ???
  }

  /** Helpful method, which can be reused between makeInvertedIndex and rankKeywordsUsingIndex
   * Consider caching the results using `cache`
   */
  private def reverseIndex(rdd: RDD[EnronEmail], keywords: List[String]): RDD[(String, EnronEmail)] = {
    ???
  }

  /** Compute the keywords ranking using the inverted index.
     */
  def rankKeywordsUsingIndex(index: RDD[(String, Iterable[EnronEmail])]): List[(String, Int)] = {
    ???
  }

  /** Use `reduceByKey` so that the computation of the index and the ranking are combined.
   */
  def rankKeywordsReduceByKey(keywords: List[String], rdd: RDD[EnronEmail]): List[(String, Int)] = {
    ???
  }

  def main(args: Array[String]) {

    /* Number of emails sent by each user */
    timed("Number of emails sent by top users", emailsPerUser(enronRdd).take(10).foreach(println))

    /* An inverted index mapping keywords to Enron emails on which they appear */
    def index: RDD[(String, Iterable[EnronEmail])] = makeInvertedIndex(topic1, enronRdd)

    /* Keywords ranked, using the inverted index */
    val topicRanked1: List[(String, Int)] = timed("Ranking using inverted index", rankKeywordsUsingIndex(index))
    topicRanked1.take(10).foreach(println)

    /* Keywords ranked with the use of RecudeByKey */
    val topicRanked2: List[(String, Int)] = timed("Ranking using inverted index and reduceByKey", rankKeywordsReduceByKey(topic1, enronRdd))
    topicRanked2.take(10).foreach(println)

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
