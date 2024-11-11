package me.ldclrcq.home.sensors.processor.raw_measurements.linky;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Device(

	@JsonProperty("applicationVersion")
	double applicationVersion,

	@JsonProperty("manufacturerName")
	String manufacturerName,

	@JsonProperty("dateCode")
	String dateCode,

	@JsonProperty("manufacturerID")
	double manufacturerID,

	@JsonProperty("powerSource")
	String powerSource,

	@JsonProperty("type")
	String type,

	@JsonProperty("networkAddress")
	double networkAddress,

	@JsonProperty("stackVersion")
	double stackVersion,

	@JsonProperty("ieeeAddr")
	String ieeeAddr,

	@JsonProperty("zclVersion")
	double zclVersion,

	@JsonProperty("hardwareVersion")
	double hardwareVersion,

	@JsonProperty("model")
	String model,

	@JsonProperty("softwareBuildID")
	String softwareBuildID,

	@JsonProperty("friendlyName")
	String friendlyName
) {
}