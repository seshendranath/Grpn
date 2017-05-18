import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.Path

val timeFormat = "yyyy-MM-dd HH:mm:ss"
try {
  throw new RuntimeException
}
catch {
  case e: RuntimeException => println("Part already exists")
}

(DateTime.now + 9.hours).toString(timeFormat)

val parts = List("BH", "NST")
val dts = (1 to 7).map(DateTime.now.minusDays(_).toString("yyyy-MM-dd")).toList

val files = for (part <- parts; dt <- dts) yield {
  val path = new Path(s"hdfs://gdoop-namenode-staging-vip.snc1:8020/user/grp_gdoop_edw_etl_dev/" +
    s"prod_groupondw.gbl_traffic_superfunnel_deal_dev/source_type=$part/event_date=$dt")
  path
}


