package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlatMapExercise {

    public static void main(String[] args) throws Exception {
        FlatMapExercise job = new FlatMapExercise();

        job.runJob();
    }

    public void runJob() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rideEvents = env.fromCollection(sourceRides());

        DataStream<EnrichedRide> nyEnrichedRides = rideEvents.flatMap(new OnlyNYRidesEnritched());

        nyEnrichedRides.print();

        env.execute("JOSEMY - FlatMap exercise");
    }

    /**
     * Return only one element.
     */
    public static class OnlyNYRidesEnritched implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            //Create function to filter by NY Rides
            FilterFunction<TaxiRide> rideValidator = new RideCleansingExercise.NYCFilter();
            if (rideValidator.filter(taxiRide)) {
                // If the Ride is in NY we collect it
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }

    private List<TaxiRide> sourceRides() {

        List<TaxiRide> sourceRides = new ArrayList<>();

        sourceRides.add(new TaxiRide(1L, true));
        sourceRides.add(new TaxiRide(2L, false));
        sourceRides.add(new TaxiRide(3L, false));
        sourceRides.add(new TaxiRide(4L, true));

        return sourceRides;
    }
}
