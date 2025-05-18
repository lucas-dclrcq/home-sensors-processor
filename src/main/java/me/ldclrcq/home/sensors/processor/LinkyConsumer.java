package me.ldclrcq.home.sensors.processor;

import io.smallrye.common.annotation.NonBlocking;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import me.ldclrcq.home.sensors.processor.raw_measurements.linky.RawLinkyMeasurement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@ApplicationScoped
public class LinkyConsumer {
    private final Pool pool;

    public LinkyConsumer(Pool pool) {
        this.pool = pool;
    }

    @Incoming("linky")
    @NonBlocking
    public Uni<Void> consume(ConsumerRecord<String, RawLinkyMeasurement> record) {
        return Uni.createFrom().item(record)
                .map(tempRecord -> {
                    String room = tempRecord.key();
                    RawLinkyMeasurement value = tempRecord.value();

                    long timestamp = tempRecord.timestamp();
                    Instant instant = Instant.ofEpochMilli(timestamp);
                    OffsetDateTime observedAt = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);

                    return Tuple.from(List.of(observedAt, room, value.currentSummDelivered(), value.apparentPower(), value.availablePower(), value.currentTarif()));
                })
                .flatMap(parameters -> pool.withTransaction(sqlConnection -> sqlConnection
                        .preparedQuery("""
                                INSERT INTO linky (observed_at, current_summ_delivered, apparent_power, available_power, current_tarif)
                                VALUES ($1, $2, $3, $4, $5)
                                """).execute(parameters)))
                .replaceWithVoid();
    }
}
