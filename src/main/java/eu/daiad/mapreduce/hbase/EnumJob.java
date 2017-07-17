package eu.daiad.mapreduce.hbase;

/**
 * Enumeration of implemented jobs.
 */
public enum EnumJob {
    /**
     * Smart water meter data aggregation.
     */
    METER_DATA_AGGREGATE("meter-data-pre-aggregation"),
    /**
     * Smart water meter forecasting data aggregation.
     */
    METER_FORECASTING_DATA_AGGREGATE("meter-forecasting-data-pre-aggregation");

    private final String value;

    public String getValue() {
        return value;
    }

    private EnumJob(String value) {
        this.value = value;
    }

    public static EnumJob fromValue(String value) {
        for (EnumJob item : EnumJob.values()) {
            if (item.getValue().equalsIgnoreCase(value)) {
                return item;
            }
        }
        return null;
    }
}
