package me.ldclrcq.home.sensors.processor;

import me.ldclrcq.home.sensors.processor.raw_measurements.temperature.RawTempMeasurement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.hoohoot.homelab.manager.sensors.TemperatureMeasurement;

public class RawTempMeasurementMapper implements Processor<String, RawTempMeasurement, String, TemperatureMeasurement> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext<String, TemperatureMeasurement> context) {
       this.context = context;
    }

    @Override
    public void process(Record<String, RawTempMeasurement> record) {
        var output = new TemperatureMeasurement(record.key(), record.value().temperature, record.value().humidity, record.timestamp());
        this.context.forward(new Record<>(record.key(), output, record.timestamp()));
    }
}
