package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.training.exercises.common.EnrichedRide
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.util.Collector

object FlatMapExercise {

  def main(args: Array[String]): Unit = {
    val job = new FlatMapExerciseJob()

    job.runJob()
  }

  private class FlatMapExerciseJob {
    def runJob(): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val rideEvents = env.fromCollection(sourceRides())

      rideEvents.flatMap(new OnlyNYRidesEnriched).print()

      env.execute("JOSEMY - FlatMap exercise SCALA")
    }
    private def sourceRides(): List[TaxiRide] = {
      List[TaxiRide](
        new TaxiRide(1L, true),
        new TaxiRide(2L, false),
        new TaxiRide(3L, false),
        new TaxiRide(4L, true)
      )
    }
  }

  private class OnlyNYRidesEnriched() extends FlatMapFunction[TaxiRide, EnrichedRide] {
    override def flatMap(value: TaxiRide, out: Collector[EnrichedRide]): Unit = {
      val rideValidator = new RideCleansingExercise.NYCFilter()
      if (rideValidator.filter(value)) {
        out.collect(new EnrichedRide(value))
      }
    }
  }
}
