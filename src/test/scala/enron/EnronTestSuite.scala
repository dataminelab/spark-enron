package enron

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EnronTestSuite extends FunSuite with BeforeAndAfterAll {

  def initializeEnronAnalysis(): Boolean =
    try {
      EnronAnalysis
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeEnronAnalysis())
    import EnronAnalysis._
    sc.stop()
  }

  test("'occurrencesOfKeyword' should work for (specific) RDD with one element") {
    assert(initializeEnronAnalysis())
    import EnronAnalysis._
    val rdd = sc.parallelize(Seq(EnronEmail(sender = "ceo@enron.com", subject = "Fraud", body = "Thanks for doing business with us")))
    val res = (occurrencesOfKeyword("Fraud", rdd) == 1)
    assert(res, "occurrencesOfKeyword given (specific) RDD with one element should equal to 1")
  }

  test("'makeInvertedIndex' creates a simple index with two entries") {
    assert(initializeEnronAnalysis())
    import EnronAnalysis._
    val topic1 = List("message", "attach")
    val articles = List(
      EnronEmail(sender = "ceo@enron.com", subject = "Subject 1", body = "Thanks for your attachment"),
      EnronEmail(sender = "ceo@enron.com", subject = "See attached", body = "See attached"),
      EnronEmail(sender = "ceo@enron.com", subject = "Subject 2 message", body = "Thanks for doing business with us")
      )
    val rdd = sc.parallelize(articles)
    val index = makeInvertedIndex(topic1, rdd)
    val res = index.count() == 2
    assert(res)
  }

  test("'rankKeywordsUsingIndex' should work for a simple RDD with three elements") {
    assert(initializeEnronAnalysis())
    import EnronAnalysis._
    val topic = List("message", "attach")
    val articles = List(
      EnronEmail(sender = "ceo@enron.com", subject = "Subject 1", body = "Thanks for your attachment"),
      EnronEmail(sender = "ceo@enron.com", subject = "See attached", body = "See attached"),
      EnronEmail(sender = "ceo@enron.com", subject = "Subject 2", body = "Thanks for doing business with us")
    )
    val rdd = sc.parallelize(articles)
    val index = makeInvertedIndex(topic, rdd)
    val ranked = rankKeywordsUsingIndex(index)
    val res = (ranked.head._1 == "attach")
    assert(res)
  }

  test("'rankKeywordsReduceByKey' should work for a simple RDD with four elements") {
    assert(initializeEnronAnalysis())
    import EnronAnalysis._
    val langs = List("message", "attach", "pleas", "email")
    val articles = List(
        EnronEmail(sender = "ceo@enron.com", subject = "Subject 1", body = "Thanks for your attachment"),
        EnronEmail(sender = "ceo@enron.com", subject = "See attached", body = "See attached"),
        EnronEmail(sender = "ceo@enron.com", subject = "Subject 2", body = "Thanks for doing business with us"),
        EnronEmail(sender = "ceo@enron.com", subject = "Subject 2", body = "Thanks for doing business with us")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankKeywordsReduceByKey(langs, rdd)
    val res = (ranked.head._1 == "attach")
    assert(res)
  }


}
