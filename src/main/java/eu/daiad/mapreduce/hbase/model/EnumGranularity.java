package eu.daiad.mapreduce.hbase.model;

/**
 * Levels of granularity
 */
public enum EnumGranularity {
    HOUR((byte) 1), DAY((byte) 2), WEEK((byte) 3), MONTH((byte) 4), YEAR((byte) 5);

    private final byte value;

    private EnumGranularity(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }
}