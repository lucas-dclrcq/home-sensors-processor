package me.ldclrcq.home.sensors.processor;

import io.smallrye.common.annotation.NonBlocking;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import me.ldclrcq.home.sensors.processor.raw_measurements.temperature.RawTempMeasurement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@ApplicationScoped
public class TemperatureConsumer {
    private final Pool pool;

    public TemperatureConsumer(Pool pool) {
        this.pool = pool;
    }

    @Incoming("temperatures")
    @NonBlocking
    public Uni<Void> consume(ConsumerRecord<String, RawTempMeasurement> record) {
        return Uni.createFrom().item(record)
                .map(tempRecord -> {
                    String room = tempRecord.key();
                    RawTempMeasurement value = tempRecord.value();

                    long timestamp = tempRecord.timestamp();
                    Instant instant = Instant.ofEpochMilli(timestamp);
                    OffsetDateTime observedAt = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);

                    return Tuple.from(List.of(observedAt, room, value.battery, value.humidity, value.temperature, value.linkquality, value.voltage));
                })
                .flatMap(parameters -> pool.withTransaction(sqlConnection -> sqlConnection
                        .preparedQuery("""
                                INSERT INTO temperatures (observed_at, room, battery, humidity, temperature, link_quality, voltage)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                """).execute(parameters)))
                .replaceWithVoid();
    }
}
