//package live.ashish.spark.etl.drools
//
//import breeze.numerics.log
//import kafka.tools.StateChangeLogMerger.partitions
//import org.apache.spark.api.java.JavaRDD
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
//import org.kie.api.{KieBase, KieServices}
//import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}
//
//import java.lang.reflect.Method
//import java.util
//import java.util.Arrays
//import java.util.Spliterators
//import java.util.stream.{Collectors, StreamSupport}
//
//// use jdk8
//// this creates single session for each partition and per row
//
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//import org.kie.api.KieServices
//import org.kie.api.runtime.rule.FactHandle
//import scala.collection.JavaConverters._
//
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//import org.kie.api.KieServices
//import org.kie.api.runtime.rule.FactHandle
//import scala.collection.JavaConverters._
//
//class DroolsExecutor {
//  def executeDrools(
//      droolsservicesdf: Dataset[Row],
//      spark: SparkSession,
////      fsConfig: FileSourceConfig,
//      droolsSession: String,
//      droolsSchema: String,
//      droolsSetColumns: String
//  ): Dataset[Row] = {
//    var DroolsDataset: Dataset[Row] = null
//    try {
//      val df = droolsservicesdf.drop("LAST_UPDATE_DT")
//
//      var DroolsRowRDD: RDD[Row] = null
//      val bronzeTableRDD = df.rdd
//
//      val getColumnList = new util.ArrayList[String](util.Arrays.asList(df.columns: _*))
//      val setColumnList = new util.ArrayList[String](util.Arrays.asList(df.columns: _*))
//
//      val items = droolsSetColumns.split("\\s*,\\s*")
//      setColumnList.addAll(util.Arrays.asList(items: _*))
//
//      DroolsRowRDD = bronzeTableRDD.mapPartitions { partitions =>
//        val kieServices = KieServices.Factory.get()
//        val kContainer = kieServices.getKieClasspathContainer()
////        val ksession = kContainer.newKieSession(fsConfig.getProperty("drools.sessionname"))
//        val ksession = kContainer.newKieSession("drools.sessionname")
//
//        val resRow = new util.ArrayList[Row]()
////        val modelName = fsConfig.getProperty("drools.modelname")
//        val modelName = "drools.modelname"
//
//        var objInstance = Class.forName(modelName).newInstance()
//        val method = objInstance.getClass.getSuperclass.getDeclaredMethods
//
//        val setMethodList = method.filter(S => S.getName != null && S.getName.contains("set")).toList
//        val getMethodList = method.filter(S => S.getName != null && (S.getName.contains("get") || S.getName.contains("is"))).toList
//        val newGetColumnList = getColumnList.stream().map(S => "set" + S).collect(Collectors.toList())
//
//        val tempGetColumnList = new util.ArrayList[String]()
//        tempGetColumnList.addAll(newGetColumnList)
//
//        val temp = new util.ArrayList[Method]()
//        temp.addAll(setMethodList)
//
//        setMethodList.foreach { m =>
//          tempGetColumnList.foreach { l =>
//            if (l.equalsIgnoreCase(m.getName)) {
//              temp.remove(m)
//            }
//          }
//        }
//
//        setMethodList.removeAll(temp)
//
//        val spliterator = Spliterators.spliteratorUnknownSize(partitions, 0)
//        val targetStream = StreamSupport.stream(spliterator, false)
//        targetStream.forEach { row =>
//          if (row != null) {
//            objInstance = Class.forName(modelName).newInstance()
//            val modelInstance = getModelInstance(row, newGetColumnList, setMethodList, objInstance)
//            val fact = ksession.insert(modelInstance)
//            ksession.fireAllRules()
//            ksession.delete(fact)
//            val newRow = RowFactory.create(callAllGetterMethods(modelInstance, setColumnList, getMethodList))
//            resRow.add(newRow)
//          }
//        }
//
//        ksession.destroy()
//        ksession.dispose()
//        resRow.iterator().asScala
//      }
//
//      val silverSchema = StructType.fromJson(droolsSchema)
//      DroolsDataset = spark.createDataFrame(DroolsRowRDD, silverSchema)
//    } catch {
//      case ex: Exception =>
//        log.error(s"Exception found in executeDrools class ${ex.getMessage}")
//        ex.printStackTrace()
//        throw ex
//    }
//    DroolsDataset
//  }
//}
//
////object DroolsExecutor {
////  def executeDrools(
////                     droolsservicesdf: Dataset[Row],
////                     spark: SparkSession,
//////                     fsConfig: FileSourceConfig,
////                     droolsSession: String,
////                     droolsSchema: String,
////                     droolsSetColumns: String
////                   ): Dataset[Row] = {
////    try {
////      val df = droolsservicesdf.drop("LAST_UPDATE_DT")
////
////      val DroolsRowRDD: Dataset[Row] = df.rdd.mapPartitions { partitions =>
////        val kieServices = KieServices.Factory.get()
////        val kContainer = kieServices.getKieClasspathContainer()
//////        val ksession = kContainer.newKieSession(fsConfig.getProperty("drools.sessionname"))
////        val ksession = kContainer.newKieSession("drools.sessionname")
////
////        val resRow = partitions.flatMap { row =>
////          if (row != null) {
////            try {
//////              val modelName = fsConfig.getProperty("drools.modelname")
////              val modelName = "drools.modelname"
////              var objInstance = Class.forName(modelName).newInstance()
////
////              val method = objInstance.getClass.getSuperclass.getDeclaredMethods
////
////              val setMethodList = method
////                .filter(m => m.getName != null && m.getName.contains("set"))
////                .toList
////
////              val getMethodList = method
////                .filter(m =>
////                  m.getName != null && (m.getName.contains("get") || m.getName.contains("is"))
////                )
////                .toList
////
////              val newGetColumnList = getColumnList.map(column => s"set$column")
////
////              val tempGetColumnList = newGetColumnList.toList
////
////              val temp = setMethodList.toList
////
////              setMethodList.foreach { m =>
////                tempGetColumnList.foreach { l =>
////                  if (l.equalsIgnoreCase(m.getName)) {
////                    temp -= m
////                  }
////                }
////              }
////
////              setMethodList --= temp
////
////              val fact = ksession.insert(getModelInstance(row, newGetColumnList, setMethodList, objInstance))
////              ksession.fireAllRules()
////              ksession.delete(fact)
////              val newRow = RowFactory.create(callAllGetterMethods(objInstance, setColumnList, getMethodList))
////              Some(newRow)
////            } catch {
////              case e: Exception =>
////                e.printStackTrace()
////                None
////            }
////          } else {
////            None
////          }
////        }
////
////        ksession.destroy()
////        ksession.dispose()
////        resRow.iterator
////      }
////
////      val silverSchema = StructType.fromJson(droolsSchema)
////      val DroolsDataset = spark.createDataFrame(DroolsRowRDD, silverSchema)
////      DroolsDataset
////    } catch {
////      case ex: Exception =>
////        log.error(s"Exception found in executeDrools class ${ex.getMessage}")
////        ex.printStackTrace()
////        throw ex
////    }
////  }
////}
//
//
