package live.ashish

import org.apache.spark.internal.config

import scala.util.Try

object ConditionalPackageImport {

 def main(args: Array[String]): Unit = {
//   (1 to 10).map(println)
   val package_import = "live.ashish.testpkg2.config"
   if (Try(Class.forName(package_import)).isSuccess) {
     println(Class.forName(package_import).getMethod("method", classOf[String]).invoke(Class.forName(package_import).newInstance(), "value"))
   } else {
     val method2 = (s: String) => println(s"haha, using other object ${s}")
     println(method2("haha"))
   }
 }

}
