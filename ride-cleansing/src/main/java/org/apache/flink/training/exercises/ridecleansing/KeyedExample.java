package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.EnrichedRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class KeyedExample {
    public static void main(String[] args) throws Exception {
        KeyedExample job = new KeyedExample();

        job.runJob();
    }

    public JobExecutionResult runJob() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> taxiRides = env.fromCollection(rideEvents());

        taxiRides.flatMap(new NYCEnriched())
                .keyBy(taxiRide -> taxiRide.startCell)
                .print();

        taxiRides.flatMap(new FlatMapFunction<TaxiRide, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        if (!taxiRide.isStart) {
                            out.collect(new Tuple2<>(taxiRide.hashCode(), 2));
                        }
                    }
                }).keyBy(value -> value.f1)
                .maxBy(1).print();

        return env.execute();
    }


    public static class NYCEnriched implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> NYCFilter = new RideCleansingExercise.NYCFilter();
            if (NYCFilter.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }


    private List<TaxiRide> rideEvents() {
        List<TaxiRide> rideEvents = new ArrayList<>();

        rideEvents.add(new TaxiRide(1L, true));
        rideEvents.add(new TaxiRide(1L, false));
        rideEvents.add(new TaxiRide(2L, true));
        rideEvents.add(new TaxiRide(2L, false));

        return rideEvents;
    }
}
