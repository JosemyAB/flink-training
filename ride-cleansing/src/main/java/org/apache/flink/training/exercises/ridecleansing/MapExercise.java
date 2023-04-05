package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.util.ArrayList;
import java.util.List;

public class MapExercise {

    public static void main(String[] args) throws Exception {
        MapExercise exercise1 = new MapExercise();

        exercise1.runJob();
    }
    public JobExecutionResult runJob() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.fromCollection(ridesEvents());

        DataStream<EnrichedRide> enrichedRides = rides
                .filter(new RideCleansingExercise.NYCFilter())
                .map(new Enrichment());

        enrichedRides.print();

        return env.execute("Map Exercise");
    }

    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }

    private List<TaxiRide> ridesEvents() {
        List<TaxiRide> sourceRides = new ArrayList<>();

        sourceRides.add(new TaxiRide(1L, true));
        sourceRides.add(new TaxiRide(2L, true));
        sourceRides.add(new TaxiRide(3L, true));
        sourceRides.add(new TaxiRide(4L, true));

        return sourceRides;
    }
}
