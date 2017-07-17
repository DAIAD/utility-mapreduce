package eu.daiad.mapreduce.hbase.model;

public enum EnumGroupType {
    /**
     * An area
     */
    AREA(),
    /**
     * Custom group of users.
     */
    SET(),
    /**
     * Commons group.
     */
    COMMONS(),
    /**
     * Cluster segment.
     */
    SEGMENT(),
    /**
     * Entire utility.
     */
    UTILITY();

    public static EnumGroupType fromString(String name) {
        for (EnumGroupType item : EnumGroupType.values()) {
            if (item.name().equalsIgnoreCase(name)) {
                return item;
            }
        }
        throw new IllegalArgumentException(String.format("Group type [%s] is not supported.", name));
    }
}
