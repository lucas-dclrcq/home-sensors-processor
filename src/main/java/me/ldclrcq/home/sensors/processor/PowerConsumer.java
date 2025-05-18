package me.ldclrcq.home.sensors.processor;

import io.smallrye.common.annotation.NonBlocking;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@ApplicationScoped
public class PowerConsumer {
    private final Pool pool;

    public PowerConsumer(Pool pool) {
        this.pool = pool;
    }

    @Incoming("power")
    @NonBlocking
    public Uni<Void> consume(ConsumerRecord<String, PowerMeasurement> record) {
        return Uni.createFrom().item(record)
                .map(tempRecord -> {
                    PowerMeasurement value = tempRecord.value();

                    long timestamp = tempRecord.timestamp();
                    Instant instant = Instant.ofEpochMilli(timestamp);
                    OffsetDateTime observedAt = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);

                    return Tuple.from(List.of(observedAt, value.currentSummDeliveredWh(), value.powerConsumedWh(), value.apparentPower()));
                })
                .flatMap(parameters -> pool.withTransaction(sqlConnection -> sqlConnection
                        .preparedQuery("""
                                INSERT INTO power (observed_at, current_summ_delivered, power_consumed, apparent_power)
                                VALUES ($1, $2, $3, $4)
                                """).execute(parameters)))
                .replaceWithVoid();
    }
}
