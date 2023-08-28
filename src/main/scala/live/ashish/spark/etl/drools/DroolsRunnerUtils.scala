package live.ashish.spark.etl.drools

import org.apache.spark.sql.{Row, SparkSession}
import org.kie.api.builder.Message
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}
import org.kie.api.{KieBase, KieServices}

// use jdk8
// this creates single session for each partition and per row
object DroolsRunnerUtils {
  def processPartitionPerSession(iterator: Iterator[Row])(implicit kieBase: KieBase): Iterator[Row] = {
    val kieSession = kieBase.newKieSession()
    try {
      iterator.map { row =>
        val name = row.getAs[String]("name")
        val age = row.getAs[Int]("age")
        val classification = row.getAs[String]("classification")
        val person = Person(name, age, classification)
        try {
          val kieSession = kieBase.newKieSession()
// add this to debug
          kieSession.addEventListener(new RuleRuntimeEventListener {
            override def objectInserted(event: ObjectInsertedEvent): Unit = println(event.getObject+" inserted")
            override def objectUpdated(event: ObjectUpdatedEvent): Unit = println(event.getObject +" updated from "+ event.getOldObject)
            override def objectDeleted(event: ObjectDeletedEvent): Unit = println(event.getOldObject + " deleted")
          })

          kieSession.insert(person)
          val rulesFired = kieSession.fireAllRules()
//          println(s"Rules fired $rulesFired")
          Row.fromSeq(Seq(person.getName, person.getAge, person.getClassification))
        } catch {
          case e: Exception =>
            println(e.getMessage)
            Row.fromSeq(Seq(null, 0, null))
        }
      }
    } catch {
      case e: Exception =>
        println(e.getMessage)
        null
    }
    finally {
      kieSession.dispose()
    }

  }

  class Person(private var _name: String, private var _age: Int, private var _classification: String) {

    // Getter methods
    def getName: String = _name
    def getAge: Int = _age
    def getClassification: String = _classification

    // Setter methods
    def setName(newName: String): Unit = {
      _name = newName
    }
    def setAge(newAge: Int): Unit = {
      if (newAge >= 0) {
        _age = newAge
      } else {
        throw new IllegalArgumentException("Age cannot be negative.")
      }
    }
    def setClassification(newClassification: String) : Unit = {
      _classification = newClassification
    }
    override def toString = s"Person($getName, $getAge, $getClassification)"
  }
  object Person {
    def apply(name: String, age: Int, classification: String): Person = new Person(name, age, classification)
  }
}
