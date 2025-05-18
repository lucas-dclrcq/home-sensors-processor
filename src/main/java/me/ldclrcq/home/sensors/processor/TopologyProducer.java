package me.ldclrcq.home.sensors.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import me.ldclrcq.home.sensors.processor.raw_measurements.RawPayload;
import me.ldclrcq.home.sensors.processor.raw_measurements.linky.RawLinkyMeasurement;
import me.ldclrcq.home.sensors.processor.raw_measurements.temperature.RawTempMeasurement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;


@ApplicationScoped
public class TopologyProducer {
    public record Zigbee2MQTTKey(String schema, String payload) {
    }

    private final static Logger logger = Logger.getLogger(TopologyProducer.class);

    private final static String ZIGBEE2MQTT_TOPIC = "zigbee2mqtt";
    private final static String SENSORS_TEMPS_TOPIC = "sensors-temperatures";
    private final static String SENSORS_POWER_TOPIC = "sensors-power";
    private final static String SENSORS_LINKY_TOPIC = "sensors-linky";

    private final ObjectMapper objectMapper;
    private final int linkyMeasureWindowInMinutes;

    public TopologyProducer(ObjectMapper objectMapper, @ConfigProperty(name = "linky.measure.window") int linkyMeasureWindowInMinutes) {
        this.objectMapper = objectMapper;
        this.linkyMeasureWindowInMinutes = linkyMeasureWindowInMinutes;
    }

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<Zigbee2MQTTKey> zigbee2MQTTKeySerde = new ObjectMapperSerde<>(Zigbee2MQTTKey.class);
        ObjectMapperSerde<RawTempMeasurement> rawTempMeasurementSerde = new ObjectMapperSerde<>(RawTempMeasurement.class);
        ObjectMapperSerde<PowerMeasurement> rawPowerMeasurementSerde = new ObjectMapperSerde<>(PowerMeasurement.class);
        ObjectMapperSerde<RawLinkyMeasurement> rawLinkyMeasurementSerde = new ObjectMapperSerde<>(RawLinkyMeasurement.class);
        ObjectMapperSerde<LinkyAggregation> linkyAggregationSerde = new ObjectMapperSerde<>(LinkyAggregation.class);

        KStream<Zigbee2MQTTKey, RawPayload> zigbee2mqtt = builder
                .stream(ZIGBEE2MQTT_TOPIC, Consumed.with(zigbee2MQTTKeySerde, Serdes.String()))
                .mapValues(value -> this.deserialize(value, RawPayload.class));

        Branched<Zigbee2MQTTKey, RawPayload> temperatureBranch = Branched.withConsumer(ks -> ks.selectKey((key, value) -> {
                    Pattern pattern = Pattern.compile("zigbee2mqtt/temp_([^.]+)\\.");
                    Matcher matcher = pattern.matcher(key.payload);

                    if (!matcher.find()) {
                        logger.errorf("Could not extract temp location from key : %s", key.payload);
                    }

                    return matcher.group(1);
                }, Named.as("extract-sensor-location-to-key"))
                .mapValues(value -> this.deserialize(value.payload(), RawTempMeasurement.class), Named.as("deserialize-temp-payload"))
                .to(SENSORS_TEMPS_TOPIC, Produced.with(Serdes.String(), rawTempMeasurementSerde)));


        Branched<Zigbee2MQTTKey, RawPayload> linkyBranch = Branched.withConsumer(ks -> ks.selectKey((key, value) -> "linky", Named.as("set-linky-key"))
                .mapValues(value -> this.deserialize(value.payload(), RawLinkyMeasurement.class), Named.as("deserialize-linky-payload"))
                .to(SENSORS_LINKY_TOPIC, Produced.with(Serdes.String(), rawLinkyMeasurementSerde)));

        builder.stream(SENSORS_LINKY_TOPIC, Consumed.with(Serdes.String(), rawLinkyMeasurementSerde))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(linkyMeasureWindowInMinutes)))
                .aggregate(LinkyAggregation::init, (key, value, aggregate) ->
                {
                    // In which case is this null ?!
                    if (aggregate.currentSummDeliveredValues() == null) {
                        return LinkyAggregation.init();
                    }

                    List<Long> copyOfCurrentSummDeliveredValues = new ArrayList<>(aggregate.currentSummDeliveredValues());
                    List<Double> copyOfApparentPowerValues = new ArrayList<>(aggregate.apparentPowerValues());

                    long currentSummDeliveredWh = Math.round(value.currentSummDelivered() * 1000);

                    copyOfCurrentSummDeliveredValues.add(currentSummDeliveredWh);
                    copyOfApparentPowerValues.add(value.apparentPower());

                    var currentSummDeliveredStats = copyOfCurrentSummDeliveredValues
                            .stream()
                            .mapToLong(Long::longValue)
                            .summaryStatistics();

                    var computedAggregatedPowerConsumption = currentSummDeliveredStats.getMax() - currentSummDeliveredStats.getMin();
                    var computedAverageApparentPower = copyOfApparentPowerValues.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                    LinkyAggregation linkyAggregation = new LinkyAggregation(copyOfCurrentSummDeliveredValues, computedAggregatedPowerConsumption, copyOfApparentPowerValues, computedAverageApparentPower);
                    Log.infof("Linky aggregation: %s", linkyAggregation);
                    return linkyAggregation;
                }, Named.as("compute-linky-aggregation"), Materialized.with(Serdes.String(), linkyAggregationSerde))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .mapValues((readOnlyKey, value) -> new PowerMeasurement(value.getCurrentSummDeliveredKwh(), value.aggregatedPowerConsumption(), value.averageApparentPower()), Named.as("map-power-measurement"))
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value), Named.as("reset-key-to-linky"))
                .peek((key, value) -> Log.infof("Power measurement: %s", value))
                .to(SENSORS_POWER_TOPIC, Produced.with(Serdes.String(), rawPowerMeasurementSerde));

        zigbee2mqtt
                .split()
                .branch((key, value) -> key.payload.startsWith("zigbee2mqtt/temp"), temperatureBranch)
                .branch((key, value) -> key.payload.startsWith("zigbee2mqtt/linky"), linkyBranch)
                .noDefaultBranch();

        return builder.build();
    }

    private <T> T deserialize(String value, Class<T> type) {
        try {
            return this.objectMapper.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}