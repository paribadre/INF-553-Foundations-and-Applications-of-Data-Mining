//import org.apache.spark.{SparkConf, SparkContext}
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
//import scala.util.parsing.json.JSONObject
//
//
//case class UserJson(user_id: String,
//                    name:String,
//                    review_count: Int,
//                    yelping_since: String,
//                    useful: Any,
//                    funny: Any,
//                    cool: Any,
//                    elite:Any,
//                    compliment_hot:Any,
//                    compliment_more:Any,
//                    compliment_profile:Any,
//                    compliment_cute:Any,
//                    friends:Any,
//                    fans:Any,
//                    average_stars:Any,
//                    compliment_list:Any,
//                    compliment_note:Any,
//                    compliment_plain:Any,
//                    compliment_cool:Any,
//                    compliment_funny:Any,
//                    compliment_writer:Any,
//                    compliment_photos:Any)
//
//case class Task1(var total_users: Float = 0.0f,
//                 var avg_reviews: Float = 0.0f,
//                 var distinct_usernames: Long = 0,
//                 var num_users: Long = 0,
//                 var top10_popular_names: Any = None,
//                 var top10_most_reviews: Any = None)
//
//object parinita_badre_task1 {
//
//    def toJson(x:String ): UserJson ={
//      val om = new ObjectMapper() with ScalaObjectMapper
//      om.registerModule(DefaultScalaModule)
//      val json = om.readValue(x, classOf[UserJson])
//      return json
//    }
//
//  def convertTask1ToJson (x: Task1, output_file_path: String): Unit = {
//    var mapper = new ObjectMapper() with ScalaObjectMapper
//    mapper.registerModule(DefaultScalaModule)
//    mapper.writeValue(new File(output_file_path), x)
//  }
//
//  def main(args: Array[String]) {
//    var conf = new SparkConf().setAppName("Data Mining Task 1").setMaster("local")
//    conf = conf.setMaster("local[*]")
//      .set("spark.executor.memory", "15G")
//      .set("spark.driver.memory", "45G")
//      .set("spark.driver.maxResultSize", "10G")
//    val sc = new SparkContext(conf)
//
//    val StartTime = System.nanoTime()
//
//    val input_file_name = args.apply(0)
//    val output_file_name = args.apply(1)
//
//    val lines = sc.textFile(input_file_name).map(x => toJson(x)).persist()
//
//    val totals = lines.map(x => (1, x.review_count)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
//    var task1Res = Task1.apply()
//
//    val count = totals._1
//
//    val totals1 = totals._2
//    var avg = (totals1.toFloat / totals1)
//
//    var df_pre = lines.map(x => (x.name, 1)).reduceByKey((x, y) => (x + y))
//    var df = df_pre.count()
//
//    var yelping = lines.filter(x=> (x.yelping_since.substring(0,4) == "2011")).count()
//
//    var pop = df_pre.sortBy(x => x._2, ascending = false)
//    var popular = pop.take(10).sortBy(x => (-x._2, x._1))
//
//    var top10s = lines.map(x => (x.user_id, x.review_count)).sortBy(x => x._2, ascending = false)
//    var top10users = top10s.take(10).sortBy(x => (-x._2, x._1))
//
//    task1Res.total_users = count
//    task1Res.avg_reviews = avg
//    task1Res.distinct_usernames = df
//    task1Res.num_users = yelping
//    task1Res.top10_popular_names = popular
//    task1Res.top10_most_reviews = top10users
//
//    convertTask1ToJson(task1Res, output_file_name)
//
//  }
//}