import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.api.common._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Main extends App{
  case class WordWithCount(word: String, count: Long)

  val properties = new Properties
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "test")

  // using 0.10 consumer, so event time should come free in the stream
  val consumer = new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema, properties)
  consumer.setStartFromEarliest()

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tenv = TableEnvironment.getTableEnvironment(env)
  env.enableCheckpointing(5000)

  // Following https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/connectors/kafka.html#using-kafka-timestamps-and-flink-event-time-in-kafka-010
  // looks like the 0.10 consumer it will emit the event time timestamps as a data field.   How to use it in
  // time windows?
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val stream = env
    .addSource(consumer)
    .flatMap(_.split("\\s"))

  // Where is the DSL for fields Expression* objects like 'w described?
  // None of the examples have the 'import org.apache.flink.table.api.scala._' showing
  // or reference to the DSL syntax
  tenv.registerDataStream("Words", stream, 'w)

  // Basically following: https://flink.apache.org/news/2017/04/04/dynamic-tables.html
  // How do I extract the event time which FlinkKafkaConsumer010 is supposed to provide?
  //  'rowtime' is not found (not clear from the example on the page where it comes from)
  // Can be interpreted as a keyword, which it is not.
  val res = tenv.sql(
    """
      | select w, count(*)
      | from Words
      | group by tumble(rowtime, interval '1' second)
    """.stripMargin
  )
  res.toAppendStream[(String, Long)].print

  env.execute("andrey-job")
}
