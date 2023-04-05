package org.apache.flink.training.exercises.ridecleansing.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.training.exercises.common.EnrichedRide
import org.apache.flink.training.exercises.common.datatypes.TaxiRide

object MapExercise {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new RideEnrichmentExerciseJob()

    job.runJob()
  }

  private class RideEnrichmentExerciseJob() {
    def runJob(): JobExecutionResult = {

      val rides = List(new TaxiRide(1L, true),
                       new TaxiRide(2L, true),
                       new TaxiRide(3L, true),
                       new TaxiRide(4L, true))

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val ridesStream = env.fromCollection(rides)

      val enrichedRides =
        ridesStream.filter(new RideCleansingExercise.NYCFilter()).map(new EnrichmentMap())

      enrichedRides.print()

      env.execute("Josemy - Map exercise SCALA")
    }
  }

  private class EnrichmentMap extends MapFunction[TaxiRide, EnrichedRide] {
    override def map(taxiRide: TaxiRide): EnrichedRide = new EnrichedRide(taxiRide)
  }
}
