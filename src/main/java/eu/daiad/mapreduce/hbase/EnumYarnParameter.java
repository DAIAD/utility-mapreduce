package eu.daiad.mapreduce.hbase;

/**
 * Enumeration of YARN parameters.
 */
public enum EnumYarnParameter {
    /**
     * Not supported parameter.
     */
    NOT_SUPPORTED(""),
    /**
     * The host name of the RM.
     */
    RESOURCE_MANAGER_HOSTNAME("yarn.resourcemanager.hostname");

    private final String value;

    public String getValue() {
        return value;
    }

    private EnumYarnParameter(String value) {
        this.value = value;
    }

    public static EnumYarnParameter fromString(String value) {
        for (EnumYarnParameter item : EnumYarnParameter.values()) {
            if (item.getValue().equalsIgnoreCase(value)) {
                return item;
            }
        }
        return NOT_SUPPORTED;
    }
}
