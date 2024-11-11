package me.ldclrcq.home.sensors.processor.raw_measurements.temperature;

import java.util.Date;

public class RawTempMeasurement {
    public int battery;
    public RawTempMeasurementDevice device;
    public double humidity;
    public Date last_seen;
    public int linkquality;
    public double temperature;
    public int voltage;
}