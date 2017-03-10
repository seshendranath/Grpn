try {
  throw new RuntimeException
}
catch {
  case e: RuntimeException => println("Part already exists")
}