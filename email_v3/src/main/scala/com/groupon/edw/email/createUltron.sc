import okhttp3.{MediaType, OkHttpClient, RequestBody, Request}
import play.api.libs.json.Json

import Ultron._
import org.apache.log4j.{Level, Logger}
import scala.concurrent.duration._

object Ultron {

  val log = Logger.getLogger(getClass)
  log.setLevel(Level.toLevel("Info"))

  val client = new OkHttpClient()

  val url = "http://ultron-staging-app1.snc1:9000/job/instance"
  val jobName = "email"
  val successCode = 200
  val timeout = 5.seconds

  def startJob() = {
    val request = new Request.Builder()
      .url(s"$url/start/$jobName")
      .post(RequestBody.create(null, Array[Byte]()))

    val response = client.newCall(request.build()).execute()
    response.body()
    val result = Json.parse(new String(response.body().bytes()))
    val instanceId = (result \ "instance_id").as[String]
    val dataStartTimestamp = (result \ "data_start_timestamp").as[String]

    (instanceId, dataStartTimestamp)
  }

  def endJob(instanceId: String, status: String, dataStartTimestamp: String, dataEndTimestamp: String) = {

    val requestBody = Json.obj(
      "data_start_timestamp" -> dataStartTimestamp,
      "data_end_timestamp" -> dataEndTimestamp
    )

    val endRequest = new Request.Builder()
      .url(s"$url/end/$instanceId/$status")
      .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
        Json.stringify(requestBody)))

    val response = client.newCall(endRequest.build()).execute()
    val message = response.message()
    println(response.code())
    if (response.code() != successCode) {
      println(message)
      throw new RuntimeException("Something went wrong while updating Ultron")
    }
  }
}
val (id, st) = startJob()

endJob(id, "succeeded", "2017-03-01 12:00:00", "2017-03-01 12:00:00")

