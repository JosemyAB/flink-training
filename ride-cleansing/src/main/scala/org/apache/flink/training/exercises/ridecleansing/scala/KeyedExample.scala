package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.training.exercises.common.EnrichedRide
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.util.Collector

object KeyedExample {

  def main(args: Array[String]): Unit = {
    val job = new KeyedExampleJob()

    job.runJob()
  }

  private class KeyedExampleJob() {
    def runJob(): JobExecutionResult = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val rides = env.fromCollection(taxiRides)

      rides.flatMap(new NYCEnriched()).keyBy(_.startCell).print()

      env.execute("JOSEMY - Keyed Example")
    }

    private def taxiRides: List[TaxiRide] = {

      List(new TaxiRide(1L, true),
        new TaxiRide(2L, true),
        new TaxiRide(3L, true),
        new TaxiRide(4L, true))
    }

  }

  private class NYCEnriched extends FlatMapFunction[TaxiRide, EnrichedRide] {
    override def flatMap(taxiRide: TaxiRide, out: Collector[EnrichedRide]): Unit = {

      val NYCValidator = new RideCleansingExercise.NYCFilter
      if (NYCValidator.filter(taxiRide)) {
        out.collect(new EnrichedRide(taxiRide))
      }
    }
  }

}
