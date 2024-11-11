package me.ldclrcq.home.sensors.processor.raw_measurements.linky;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RawLinkyMeasurement(

	@JsonProperty("apparent_power")
	double apparentPower,

	@JsonProperty("last_seen")
	String lastSeen,

	@JsonProperty("rms_current")
	int rmsCurrent,

	@JsonProperty("MOTDETAT")
	String mOTDETAT,

	@JsonProperty("update")
	Update update,

	@JsonProperty("active_register_tier_delivered")
	String activeRegisterTierDelivered,

	@JsonProperty("rms_current_max")
	double rmsCurrentMax,

	@JsonProperty("mot_d_etat")
	String motDEtat,

	@JsonProperty("current_tarif")
	String currentTarif,

	@JsonProperty("linkquality")
	double linkquality,

	@JsonProperty("current_summ_delivered")
	double currentSummDelivered,

	@JsonProperty("device")
	Device device,

	@JsonProperty("available_power")
	double availablePower,

	@JsonProperty("update_available")
	String updateAvailable,

	@JsonProperty("meter_serial_number")
	String meterSerialNumber,

	@JsonProperty("warn_d_p_s")
	double warnDPS
) {
}