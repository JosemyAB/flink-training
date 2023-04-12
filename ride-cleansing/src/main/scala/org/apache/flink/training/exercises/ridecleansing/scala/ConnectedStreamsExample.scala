package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object ConnectedStreamsExample {

  def main(args: Array[String]): Unit = {
    val job = new ConnectedStreamsJob()

    job.runJob
  }

  private class ConnectedStreamsJob {

    def runJob: JobExecutionResult = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val controlStream = env.fromCollection(controlEvents).keyBy(x => x)
      val wordsStream = env.fromCollection(wordsEvents).keyBy(x => x)

      controlStream
        .connect(wordsStream)
        .flatMap(new ControlFunction())
        .print()

      env.execute("JOSEMY - Connected Streams")
    }

    private def wordsEvents: List[String] = {
      List("Must", "DROP", "Appear", "IGNORE")
    }

    private def controlEvents: List[String] = {
      List("DROP", "IGNORE")
    }

  }

  private class ControlFunction extends RichCoFlatMapFunction[String, String, String] {
    var blocked: ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
      blocked = getRuntimeContext.getState(
        new ValueStateDescriptor[Boolean]("blocked", createTypeInformation[Boolean]))
    }

    override def flatMap1(control: String, out: Collector[String]): Unit = blocked.update(true)

    override def flatMap2(word: String, out: Collector[String]): Unit = {
      if (blocked.value() == null) {
        out.collect(word)
      }
    }
  }
}
