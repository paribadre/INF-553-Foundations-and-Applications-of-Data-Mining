import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.io.{PrintWriter, _}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.util.parsing.json.JSONObject
import java.util.Calendar

case class UserJson2(user_id: String,
                    name:String,
                    review_count: Int,
                    yelping_since: String,
                    useful: Any,
                    funny: Any,
                    cool: Any,
                    elite:Any,
                    friends:Any,
                    fans:Any,
                    average_stars:Any,
                    compliment_hot:Any,
                    compliment_more:Any,
                    compliment_profile:Any,
                    compliment_cute:Any,
                    compliment_list:Any,
                    compliment_note:Any,
                    compliment_plain:Any,
                    compliment_cool:Any,
                    compliment_funny:Any,
                    compliment_writer:Any,
                    compliment_photos:Any)

case class Task2Res1(
                        var default: Task2InnerRes = Task2InnerRes.apply(),
                        var customized: Task2InnerRes = Task2InnerRes.apply(),
                        var explanation: String = "Spark partitions by default using 74 partitions and hash function. By choosing a custom partition that is optimal for the given data, the data is distributed optimally on partitions to reduce communication cost. Using partitionby also helps shuffle functions like sortBy and join. Thus, using a custom partition performs better."
                      )
case class Task2InnerRes(
                           var n_partition: Any = None,
                           var n_items: Any = None,
                           var exe_time: Any = None
                         )

object parinita_badre_task2 {

  def UtoJson(x:String ): UserJson2 ={
    val om = new ObjectMapper() with ScalaObjectMapper
    om.registerModule(DefaultScalaModule)
    val json = om.readValue(x, classOf[UserJson2])
    return json
  }

  def WtoJson(x:Task2Res1, o: String ) {
    val om = new ObjectMapper() with ScalaObjectMapper
    om.registerModule(DefaultScalaModule)
    om.writeValue(new File(o), x)
  }

  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    conf = conf.setMaster("local[*]")
      .set("spark.executor.memory", "15G")
      .set("spark.driver.memory", "45G")
      .set("spark.driver.maxResultSize", "10G")
    val sc = new SparkContext(conf)

    var Def = Task2InnerRes.apply()
    var Cust = Task2InnerRes.apply()
    var Output = Task2Res1.apply()


    // TODO Remove time
    val now = Calendar.getInstance()
    val startTime=now.getTimeInMillis()


    val input_file = args.apply(0)
    val output_file = args.apply(1)
    val input_part = args.apply(2).toInt

    val lines = sc.textFile(input_file).map(x => UtoJson(x))

    var top10s = lines.map( x=> (x.user_id, x.review_count)).sortBy(x=> x._2, false).take(10).sortBy(x=> (-x._2, x._1))
    val now1 = Calendar.getInstance()
    val defaulttime = (now1.getTimeInMillis() - startTime)*0.001

    Def.n_partition = lines.getNumPartitions
    Def.n_items = lines.mapPartitions(iter => Iterator(iter.size), true).collect()
    Def.exe_time = defaulttime
    Output.default = Def
    val now2 = Calendar.getInstance()
    val currenttime = now2.getTimeInMillis()

    val total2 = lines.map( x=> (x.user_id, x.review_count)).partitionBy(new HashPartitioner(input_part)).sortBy( x=> x._1, false)
    top10s = total2.take(10)
    val now3 = Calendar.getInstance()
    val customtime = (now3.getTimeInMillis() - currenttime)*0.001

    Cust.n_partition = total2.getNumPartitions
    Cust.n_items = total2.mapPartitions(iter => Iterator(iter.size), true).collect()
    Cust.exe_time = customtime
    Output.customized = Cust

    WtoJson(Output, output_file)

  }
}