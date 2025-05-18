package me.ldclrcq.home.sensors.processor;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.List;

public record LinkyAggregation(List<Long> currentSummDeliveredValues, long aggregatedPowerConsumption,
                               List<Double> apparentPowerValues, double averageApparentPower) {

    public static LinkyAggregation init() {
        return new LinkyAggregation(Collections.emptyList(), 0L, Collections.emptyList(), 0.0);
    }

    @JsonIgnore
    public long getCurrentSummDeliveredKwh() {
        if (currentSummDeliveredValues == null) {
            return 0L;
        }

        return currentSummDeliveredValues.stream().max(Long::compare).orElse(0L);
    }
}
