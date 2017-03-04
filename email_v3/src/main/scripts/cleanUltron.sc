import okhttp3.{MediaType, OkHttpClient, RequestBody, Request}
import play.api.libs.json.{JsObject, Json}

val client = new OkHttpClient()

val requestRun = new Request.Builder()
  .url("http://ultron-staging-app1.snc1:9000/job/instance/19")
  .get()

val responseRun = client.newCall(requestRun.build()).execute()
val runs = Json.parse(new String(responseRun.body().bytes()))

runs.as[List[JsObject]]
  .filter(x => (x \ "status").as[Int] == 2)
  .map(x => (x \ "id").as[String])
  .foreach {x =>
    println(x)
    kill(x)
  }
def kill(id: String) = {
  val requestBody = Json.obj(
    "data_end_timestamp" -> "2015-01-22 12:00:54"
  )
  val endRequest = new Request.Builder()
    .url(s"http://ultron-staging-app1.snc1:9000/job/instance/end/$id/failed")
    .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
      Json.stringify(requestBody)))
  val response = client.newCall(endRequest.build()).execute()
  println(response.code())
  println(response.message())
  println(new String(response.body().bytes()))
}


