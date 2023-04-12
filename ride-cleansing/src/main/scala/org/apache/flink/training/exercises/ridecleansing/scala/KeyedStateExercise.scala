package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.training.scala.Event
import org.apache.flink.util.Collector

/**
  * In this example, imagine you have a stream of events that you want to de-duplicate,
  * so that you only keep the first event with each key.
  * Here’s an application that does that, using a RichFlatMapFunction called Deduplicator:
  */
object KeyedStateExercise {

  def main(args: Array[String]): Unit = {
    val job = new KeyedStateJob()

    job.runJob();
  }
  private class KeyedStateJob {

    def runJob(): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment;

      env
        .fromCollection(eventSource())
        .keyBy(_.key)
        .flatMap(new Deduplicator())
        .print()

      env.execute("Josemy - use Rich Flatmap")
    }

    private def eventSource(): List[Event] = {
      List(
        new Event("test-1", System.currentTimeMillis()),
        new Event("test-2", System.currentTimeMillis()),
        new Event("test-1", System.currentTimeMillis()),
        new Event("test-4", System.currentTimeMillis()),
        new Event("test-2", System.currentTimeMillis())
      )
    }
  }

  private class Deduplicator extends RichFlatMapFunction[Event, Event] {

    private var keyHasBeenSeen: ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
      val desc = new ValueStateDescriptor[Boolean]("keyHasBeenSeen", createTypeInformation[Boolean])
      keyHasBeenSeen = getRuntimeContext.getState(desc)
    }

    override def flatMap(value: Event, out: Collector[Event]): Unit = {
      if (keyHasBeenSeen.value() == null) {
        out.collect(value)
        keyHasBeenSeen.update(true)
      }
    }
  }

}
