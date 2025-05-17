package me.ldclrcq.home.sensors.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import me.ldclrcq.home.sensors.processor.raw_measurements.RawPayload;
import me.ldclrcq.home.sensors.processor.raw_measurements.temperature.RawTempMeasurement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.jboss.logging.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


@ApplicationScoped
public class TopologyProducer {
    public record Zigbee2MQTTKey(String schema, String payload) {
    }

    private final static Logger logger = Logger.getLogger(TopologyProducer.class);

    private final static String ZIGBEE2MQTT_TOPIC = "zigbee2mqtt";
    private final static String SENSORS_TEMPS_TOPIC = "sensors-temperatures";

    private final ObjectMapper objectMapper;

    public TopologyProducer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<Zigbee2MQTTKey> zigbee2MQTTKeySerde = new ObjectMapperSerde<>(Zigbee2MQTTKey.class);
        ObjectMapperSerde<RawTempMeasurement> rawTempMeasurementSerde = new ObjectMapperSerde<>(RawTempMeasurement.class);

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

        zigbee2mqtt
                .split()
                .branch((key, value) -> key.payload.startsWith("zigbee2mqtt/temp"), temperatureBranch)
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