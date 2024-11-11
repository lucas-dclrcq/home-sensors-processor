package me.ldclrcq.home.sensors.processor.raw_measurements.linky;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Update(

	@JsonProperty("latest_version")
	double latestVersion,

	@JsonProperty("installed_version")
	double installedVersion,

	@JsonProperty("state")
	String state
) {
}