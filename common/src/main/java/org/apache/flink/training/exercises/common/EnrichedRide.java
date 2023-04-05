package org.apache.flink.training.exercises.common;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

public class EnrichedRide extends TaxiRide {

    public int startCell;
    public int endCell;

    public EnrichedRide() {}

    public EnrichedRide(TaxiRide taxiRide) {
        this.rideId = taxiRide.rideId;
        this.isStart = taxiRide.isStart;

        startCell = GeoUtils.mapToGridCell(taxiRide.startLon, startLat);
        endCell = GeoUtils.mapToGridCell(taxiRide.endLon, endLat);
    }

    @Override
    public String toString() {
        return super.toString() + " Added data:" + Integer.toString(this.startCell) + "," + Integer.toString(this.endCell);
    }
}
