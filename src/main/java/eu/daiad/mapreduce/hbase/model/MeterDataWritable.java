package eu.daiad.mapreduce.hbase.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Writable;

public class MeterDataWritable implements Writable {

    private long datetime;

    private String serial;

    private byte[] serialHash;

    private float volume;

    private float difference;

    private float min;

    private float max;

    private long count;

    public MeterDataWritable() {

    }

    public MeterDataWritable(MeterDataWritable writable) {
        datetime = writable.datetime;
        serial = writable.serial;
        serialHash = ArrayUtils.clone(writable.serialHash);
        volume = writable.volume;
        min = writable.min;
        max = writable.max;
        difference = writable.difference;
        count = writable.count;
    }

    public MeterDataWritable(long datetime, String serial, byte[] serialHash, float volume, float difference) {
        this.datetime = datetime;
        this.serial = serial;
        this.serialHash = ArrayUtils.clone(serialHash);
        this.volume = volume;
        min = volume - difference;
        max = volume;
        this.difference = difference;
        count = 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(datetime);
        out.writeUTF(serial);
        out.write(serialHash);
        out.writeFloat(volume);
        out.writeFloat(min);
        out.writeFloat(max);
        out.writeFloat(difference);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        datetime = in.readLong();
        serial = in.readUTF();
        serialHash = new byte[16];
        in.readFully(serialHash);
        volume = in.readFloat();
        min = in.readFloat();
        max = in.readFloat();
        difference = in.readFloat();
        count = in.readLong();
    }

    public void merge(MeterDataWritable writable) throws IOException {
        if ((datetime != writable.datetime) || (!serial.equals(writable.serial))) {
            throw new IOException(String.format("Combiner error [%d , %s] [%d , %s]", datetime, serial, writable.datetime, writable.serial));
        }
        volume = (volume > writable.volume ? volume : writable.volume);
        difference += writable.difference;
        min = (min < writable.min ? min : writable.min);
        max = (max > writable.max ? max : writable.max);

        count += writable.count;
    }

    public static MeterDataWritable read(DataInput in) throws IOException {
        MeterDataWritable w = new MeterDataWritable();
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

    public float getVolume() {
        return volume;
    }

    public float getDifference() {
        return difference;
    }

}
