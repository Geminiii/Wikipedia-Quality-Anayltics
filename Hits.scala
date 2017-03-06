import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.control.Breaks._
/**
  * Object to implement HITS
  */
object Hits {
  val path = "s3://wikipedia-hits/input/"
  //val path = "/Users/Li/Downloads/parsed/sub1/sub34/"
  //val conf = new SparkConf().setMaster("local[*]").setAppName("Test Spark")
   //.set("spark.executor.memory","8g")
  val conf = new SparkConf().setAppName("Hits")

  val sc = new SparkContext(conf)

  val contributors = readContributorsOrPages("contributor*")
  val pages = readContributorsOrPages("page*")

  /**
    * Read the revisions into Spark
    * @param fileName filename of the revision
    * @return RDD of revisions
    */
  def readRevisions(fileName: String): RDD[(String, String)] ={
    val revisions = sc.textFile(path + fileName).filter(line => line !=null && line.contains(","))
      .map{line => line.split(",")}
      .filter(_.length == 2)
      .map(x => (x(0), x(1).split("\\|")))
      .flatMapValues(x => x)

    revisions.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Read the contributors or pages into Spark
    * @param fileName filename of the contributors or pages
    * @return RDD of contributors or pages
    */
  def readContributorsOrPages(fileName: String): RDD[(String, String)] ={
    val rdd = sc.textFile(path + fileName).map{line =>
      val p = line.split("\t")
      if(p.length == 1){
        (p(0), null)
      }else if(p.length == 2){
        (p(0), p(1))
      }else{
        ("", "")
      }
    }.filter(x => x._1 != null && !x._1.equals(""))

    rdd
  }

  /**
    * Initialize the HUbs and Authorities
    * @param contributor the RDD of contributor
    * @return Hubs or authorities RDD
    */
  def createAuthoritiesOrHubs(contributor: RDD[(String, String)]): RDD[(String, Double)] ={
    val authority = contributor.map(x => (x._1, 1.0)).distinct()

    authority.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Updates the scores of Hubs or authorities in each iteration
    * @param rdd1 The RDD to be updated
    * @param revisions The revision
    * @return updated hubs or authorities
    */
  def update(rdd1: RDD[(String, Double)], revisions: RDD[(String, String)]): RDD[(String, Double)] ={
    val updated = revisions.join(rdd1)
      .map(x => (x._2._1, x._2._2))
      .reduceByKey(_+_)

    updated.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Normalize the scores' summation into 1
    * @param rdd RDD tobe normalized
    * @return Noralized RDD
    */
  def normalize(rdd: RDD[(String, Double)]): RDD[(String, Double)]={
    val sum = rdd.map(_._2).sum()
    val updated = rdd.map(x => (x._1, x._2/sum)).sortBy(x => x._2, ascending = false)

    updated.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Implement the iterations
    * @param authorities
    * @param hubs
    * @param revisions
    * @param reversedRevisions
    * @param times iteration times
    */
  def iterate(authorities: RDD[(String, Double)], hubs:RDD[(String, Double)],
              revisions:RDD[(String, String)], reversedRevisions:RDD[(String, String)], times:Int): Unit={
    val i = 0
    var authorities1 = authorities
    var hubs1 = hubs
    val n = 200
    var topN = Set.empty[String]

    for( i <- 1 to times){
      val time1 = System.currentTimeMillis()

      authorities1 = update(hubs1, revisions)

      //val time2 = System.currentTimeMillis()
      //println("Authorities is updated and normalized in " + (time2-time1)/1000 + " seconds.")


      hubs1 = update(authorities1, reversedRevisions)

      hubs1 = normalize(hubs1)

      val time3 = System.currentTimeMillis()
      println("Hubs is updated and normalized in " + (time3-time1)/1000 + " seconds.")
      if(i > 8) hubs1.saveAsTextFile(path + "hubs-iter" + i)
      if(i == times) {
        val articles = sc.textFile(path + "page*").map{line =>
          val p = line.split("\t")
          if(p.length == 1){
            (p(0), null)
          }else if(p.length == 2){
            (p(0), p(1))
          }else{
            ("", "")
          }
        }.join(hubs1).sortBy(x => x._2._2, ascending = false).map(_._2._1)

        articles.saveAsTextFile(path + "articles")
      }


      val temp = hubs1.take(n).map(x => x._1).toSet[String]
      //val temp = Set(hubs1.take(n): _._1))
      if((topN: Set[String]) == (temp: Set[String])){
        println("Break!")
        break
      }else{
        topN = temp
      }
    }
  }

  def main(args: Array[String]): Unit ={

    val revisions = readRevisions("revisionList*")


    val authorities = createAuthoritiesOrHubs(contributors)


    val hubs = createAuthoritiesOrHubs(pages)

    //revisions may include articles with namespace != 0, join with hubs(pages that ns == 0) to filter
    val reversedRevisions = revisions.join(hubs).map(x => (x._2._1, x._1)).persist(StorageLevel.MEMORY_AND_DISK)


    iterate(authorities, hubs, revisions, reversedRevisions, 10)

  }
}
