import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Main extends App{
  case class WordWithCount(word: String, count: Long)

  val properties = new Properties
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "test")

  // using 0.10 consumer, so event time should come free in the stream
  val consumer = new FlinkKafkaConsumer010("test", new SimpleStringSchema, properties)
  consumer.setStartFromEarliest()

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tenv = TableEnvironment.getTableEnvironment(env)

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  class KafkaAssigner[T] extends AssignerWithPeriodicWatermarks[T] {
    var maxTs = 0L
    override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
      maxTs = Math.max(maxTs, previousElementTimestamp)
      previousElementTimestamp
    }
    override def getCurrentWatermark: Watermark = new Watermark(maxTs - 1L)
  }

  val stream = env
    .addSource(consumer)
    .assignTimestampsAndWatermarks(new KafkaAssigner[String])
    .flatMap(_.split("\\W+").filter(_.nonEmpty))
    // Wrapping in a Tuple to workaround a bug in Flink 1.3.2.
    // See https://stackoverflow.com/questions/46987246/flinkkafka-0-10-how-to-extract-kafka-message-timestamps-as-values-in-the-strea
    .map(w => Tuple1(w))

  val tbl = tenv.fromDataStream(stream, 'w, 'ts.rowtime)
  tenv.registerTable("Words", tbl)

  val res = tenv.sql(
    """
      | select w, count(*)
      | from Words
      | group by w, tumble(ts, interval '5' second)
    """.stripMargin
  )

  res.toAppendStream[(String, Long)].print

  env.execute("wordcount-job")
}
