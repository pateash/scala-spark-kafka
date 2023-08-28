package live.ashish.testpkg

object config {
  def method(str: String): String = {
    println("CALLED")
    s"METHOD: $str"
  }
}

class config{

}
