package me.ldclrcq.home.sensors.processor;

import me.ldclrcq.home.sensors.processor.raw_measurements.linky.RawLinkyMeasurement;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.jboss.logging.Logger;

import java.util.Objects;


public class LinkyDedupValueTransformer implements FixedKeyProcessor<String, RawLinkyMeasurement, RawLinkyMeasurement> {
    private final static Logger logger = Logger.getLogger(LinkyDedupValueTransformer.class);
    private RawLinkyMeasurement previousMeasurement;
    private FixedKeyProcessorContext<String, RawLinkyMeasurement> context;

    @Override
    public void init(FixedKeyProcessorContext<String, RawLinkyMeasurement> context) {
        this.context = context;
        this.previousMeasurement = null;
    }

    @Override
    public void process(FixedKeyRecord<String, RawLinkyMeasurement> record) {
        if (previousMeasurement == null) {
            previousMeasurement = record.value();
            context.forward(record);
        }

        if (!Objects.equals(previousMeasurement.currentSummDelivered(), record.value().currentSummDelivered())) {
            previousMeasurement = record.value();
            context.forward(record);
        } else {
            logger.debugf("Filtering duplicate Linky measurement: %s", record.value());
        }
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}
