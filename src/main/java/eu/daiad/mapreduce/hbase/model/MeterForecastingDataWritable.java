package eu.daiad.mapreduce.hbase.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Writable;

public class MeterForecastingDataWritable implements Writable {

    private long datetime;

    private String serial;

    private byte[] serialHash;

    private float difference;

    private long count;

    public MeterForecastingDataWritable() {

    }

    public MeterForecastingDataWritable(MeterForecastingDataWritable writable) {
        datetime = writable.datetime;
        serial = writable.serial;
        serialHash = ArrayUtils.clone(writable.serialHash);
        difference = writable.difference;
        count = writable.count;
    }

    public MeterForecastingDataWritable(long datetime, String serial, byte[] serialHash, float difference) {
        this.datetime = datetime;
        this.serial = serial;
        this.serialHash = ArrayUtils.clone(serialHash);
        this.difference = difference;
        count = 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(datetime);
        out.writeUTF(serial);
        out.write(serialHash);
        out.writeFloat(difference);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        datetime = in.readLong();
        serial = in.readUTF();
        serialHash = new byte[16];
        in.readFully(serialHash);
        difference = in.readFloat();
        count = in.readLong();
    }

    public void merge(MeterForecastingDataWritable writable) throws IOException {
        if ((datetime != writable.datetime) || (!serial.equals(writable.serial))) {
            throw new IOException(String.format("Combiner error [%d , %s] [%d , %s]", datetime, serial,
                            writable.datetime, writable.serial));
        }
        difference += writable.difference;

        count += writable.count;
    }

    public static MeterForecastingDataWritable read(DataInput in) throws IOException {
        MeterForecastingDataWritable w = new MeterForecastingDataWritable();
        w.readFields(in);
        return w;
    }

    public long getDatetime() {
        return datetime;
    }

    public String getSerial() {
        return serial;
    }

    public byte[] getSerialHash() {
        return serialHash;
    }

    public float getDifference() {
        return difference;
    }

}
