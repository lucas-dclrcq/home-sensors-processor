package me.ldclrcq.home.sensors.processor;

public record PowerMeasurement(long currentSummDeliveredWh, long powerConsumedWh, double apparentPower) {
}
