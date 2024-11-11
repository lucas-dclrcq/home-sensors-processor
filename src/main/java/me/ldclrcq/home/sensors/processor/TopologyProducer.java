package me.ldclrcq.home.sensors.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import me.ldclrcq.home.sensors.processor.raw_measurements.RawPayload;
import me.ldclrcq.home.sensors.processor.raw_measurements.linky.RawLinkyMeasurement;
import me.ldclrcq.home.sensors.processor.raw_measurements.temperature.RawTempMeasurement;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.hoohoot.homelab.manager.sensors.PowerMeasurement;
import org.hoohoot.homelab.manager.sensors.TemperatureAggregation;
import org.hoohoot.homelab.manager.sensors.TemperatureMeasurement;
import org.hoohoot.homelab.manager.sensors.TemperatureSensorInfo;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@ApplicationScoped
public class TopologyProducer {
    public record Zigbee2MQTTKey(String schema, String payload) {
    }

    private final static Logger logger = Logger.getLogger(TopologyProducer.class);

    private final static String ZIGBEE2MQTT_TOPIC = "zigbee2mqtt";
    private final static String SENSORS_TEMPS_TOPIC = "sensors-temperatures";
    private final static String SENSORS_TEMPS_INFO_TOPIC = "sensors-temperatures-info";
    private final static String SENSORS_TEMPS_AGGREGATED_TOPIC = "sensors-temperatures-aggregated";
    private final static String SENSORS_LINKY_TOPIC = "sensors-linky";

    private final ObjectMapper objectMapper;
    private final Serde<TemperatureMeasurement> tempMeasurementSerde;
    private final Serde<PowerMeasurement> powerMeasurementSerde;
    private final Serde<TemperatureAggregation> temperatureAggregationSerde;
    private final Serde<TemperatureSensorInfo> temperatureSensorInfoSerde;

    public TopologyProducer(ObjectMapper objectMapper, Serde<TemperatureMeasurement> measurementSerde, Serde<PowerMeasurement> powerMeasurementSerde, Serde<TemperatureAggregation> temperatureAggregationSerde, Serde<TemperatureSensorInfo> temperatureSensorInfoSerde) {
        this.objectMapper = objectMapper;
        this.tempMeasurementSerde = measurementSerde;
        this.powerMeasurementSerde = powerMeasurementSerde;
        this.temperatureAggregationSerde = temperatureAggregationSerde;
        this.temperatureSensorInfoSerde = temperatureSensorInfoSerde;
    }

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<Zigbee2MQTTKey> zigbee2MQTTKeySerde = new ObjectMapperSerde<>(Zigbee2MQTTKey.class);

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
                .mapValues(value -> new TemperatureMeasurement(value.device.friendlyName, value.temperature, value.humidity), Named.as("map-rawmeasurement-to-tempmeasurement"))
                .to(SENSORS_TEMPS_TOPIC, Produced.with(Serdes.String(), tempMeasurementSerde)));

        Branched<Zigbee2MQTTKey, RawPayload> powerBranch = Branched.withConsumer(ks -> ks
                .selectKey((key, value) -> key.payload)
                .mapValues(value -> this.deserialize(value.payload(), RawLinkyMeasurement.class), Named.as("deserialize-linky-payload"))
                .mapValues((readOnlyKey, value) -> new PowerMeasurement(value.apparentPower(), value.availablePower(), value.currentSummDelivered()), Named.as("map-rawmeasurement-to-powermeasurement"))
                .to(SENSORS_LINKY_TOPIC, Produced.with(Serdes.String(), powerMeasurementSerde)));

        zigbee2mqtt
                .split()
                .branch((key, value) -> key.payload.startsWith("zigbee2mqtt/temp"), temperatureBranch)
                .branch((key, value) -> key.payload.startsWith("zigbee2mqtt/linky"), powerBranch)
                .noDefaultBranch();

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("temperature-sensors");
        GlobalKTable<String, TemperatureSensorInfo> stations = builder.globalTable(SENSORS_TEMPS_INFO_TOPIC, Consumed.with(Serdes.String(), temperatureSensorInfoSerde));

        builder.stream(SENSORS_TEMPS_TOPIC, Consumed.with(Serdes.String(), tempMeasurementSerde))
                .join(
                        stations,
                        (sensorId, temperature) -> sensorId,
                        (temperature, sensorInfo) -> new TempMeasurementSummary(sensorInfo.getId(), sensorInfo.getName(), Instant.now(), temperature.getTemperature()))
                .groupByKey()
                .aggregate(
                        () -> new TemperatureAggregation(null, null, 200d, -200d, 0, 0d, 0d),
                        (key, value, aggregate) -> {
                            aggregate.setSensorId(value.sensorId);
                            aggregate.setSensorName(value.sensorName);
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value.value);
                            aggregate.setAvg(BigDecimal.valueOf(aggregate.getSum() / aggregate.getCount()).setScale(1, RoundingMode.HALF_UP).doubleValue());
                            aggregate.setMin(Math.min(aggregate.getMin(), value.value));
                            aggregate.setMax(Math.max(aggregate.getMax(), value.value));

                            return aggregate;
                        },
                        Materialized.<String, TemperatureAggregation>as(storeSupplier)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(temperatureAggregationSerde)
                )
                .toStream()
                .to(SENSORS_TEMPS_AGGREGATED_TOPIC, Produced.with(Serdes.String(), temperatureAggregationSerde));

        return builder.build();
    }

    private <T> T deserialize(String value, Class<T> type) {
        try {
            return this.objectMapper.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public record TempMeasurementSummary(String sensorId, String sensorName, Instant timestamp, double value) {
    }
}