import com.github.nscala_time.time.Imports._
val timeFormat = "yyyy-MM-dd HH:mm:ss"
try {
  throw new RuntimeException
}
catch {
  case e: RuntimeException => println("Part already exists")
}

(DateTime.now+9.hours).toString(timeFormat)
