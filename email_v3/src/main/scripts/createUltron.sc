import okhttp3.{MediaType, OkHttpClient, RequestBody, Request}
import org.apache.log4j.{Level, Logger}
import play.api.libs.json.Json
import scala.concurrent.duration._


  val log = Logger.getLogger(getClass)
  log.setLevel(Level.toLevel("Info"))

  val client = new OkHttpClient()

  val url = "http://ultron-staging-app.snc1/job/instance"
  val jobName = "email"
  val successCode = 200
  val timeout = 5.seconds

  def startJob() = {
    val request = new Request.Builder()
      .url(s"$url/start/$jobName")
      .post(RequestBody.create(null, Array[Byte]()))

    val response = client.newCall(request.build()).execute()

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

    checkResponseStatus(response.code(), response.message())
  }

  def checkResponseStatus(code: Int, message: String) = {
    if (code != successCode) {
      log.info(s"Response Code: $code")
      log.error(s"Response Message: $message")
      throw new RuntimeException("Something went wrong while updating Ultron")
    }
    log.info("Updated Ulton Successfully")
  }


val (instanceId, startTime) = startJob()
endJob(instanceId, "succeeded", "2017-03-09 02:00:00", "2017-03-09 02:00:00")