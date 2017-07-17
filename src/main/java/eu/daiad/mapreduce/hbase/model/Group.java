package eu.daiad.mapreduce.hbase.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTimeZone;

public class Group {

    private List<String> serial = new ArrayList<String>();

    private List<byte[]> serialHash = new ArrayList<byte[]>();

    private EnumGroupType type;

    private String key;

    private DateTimeZone timezone;

    public Group(EnumGroupType type, String key, DateTimeZone timezone) {
        this.type = type;
        this.key = key;
        this.timezone = timezone;
    }

    public void add(String serial, byte[] serialHash) {
        this.serial.add(serial);
        this.serialHash.add(serialHash);
    }

    public int indexOf(byte[] serial) {
        return inArray(serialHash, serial);
    }

    public String getSerial(int index) {
        return serial.get(index);
    }

    public EnumGroupType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public DateTimeZone getTimezone() {
        return timezone;
    }

    /**
     * Checks if the a byte array is contained in a list of byte arrays.
     *
     * @param array the list of arrays to search.
     * @param hash the array to find.
     * @return if the array is contained in the list.
     */
    private int inArray(List<byte[]> array, byte[] hash) {
        int index = 0;
        for (byte[] entry : array) {
            if (Arrays.equals(entry, hash)) {
                return index;
            }
            index++;
        }
        return -1;
    }

}
