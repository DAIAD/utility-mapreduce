package eu.daiad.mapreduce.hbase.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

public class AggregatedMeterForecastingData {

    private boolean isSorted = false;

    private byte[] rowKey;

    private int limit;

    private float sum;

    private List<byte[]> keys = new ArrayList<byte[]>();

    private List<MeterForecastingDataWritable> values = new ArrayList<MeterForecastingDataWritable>();

    public AggregatedMeterForecastingData(byte[] rowKey, int limit) {
        this.rowKey = rowKey;
        this.limit = limit;
    }

    public void add(MeterForecastingDataWritable value) throws IOException {
        int index = inArray(keys, value.getSerialHash());
        if (index < 0) {
            keys.add(ArrayUtils.clone(value.getSerialHash()));
            values.add(new MeterForecastingDataWritable(value));

            isSorted = false;
        } else {
            values.get(index).merge(value);
        }
        sum += value.getDifference();
    }

    private void sortRanking() {
        Collections.sort(values, new Comparator<MeterForecastingDataWritable>() {
            @Override
            public int compare(MeterForecastingDataWritable w1, MeterForecastingDataWritable w2) {
                if (w1.getDifference() < w2.getDifference()) {
                    return -1;
                } else if (w1.getDifference() > w2.getDifference()) {
                    return 1;
                }
                return 0;
            }
        });

        isSorted = true;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public float getSum() {
        return sum;
    }

    public float getMin() {
        if (!isSorted) {
            sortRanking();
        }
        return values.get(0).getDifference();
    }

    public float getMax() {
        if (!isSorted) {
            sortRanking();
        }
        return values.get(values.size() - 1).getDifference();
    }

    public float getAverage() {
        if (getCount() > 0) {
            return (sum / getCount());
        }
        return 0;
    }

    public int getCount() {
        return keys.size();
    }

    public List<MeterForecastingDataWritable> getTop() {
        if (!isSorted) {
            sortRanking();
        }
        List<MeterForecastingDataWritable> top = new ArrayList<MeterForecastingDataWritable>();
        for (int i = Math.max(0, values.size() - limit), count = values.size(); i < count; i++) {
            top.add(values.get(i));
        }
        return top;
    }

    public List<MeterForecastingDataWritable> getBottom() {
        if (!isSorted) {
            sortRanking();
        }
        List<MeterForecastingDataWritable> bottom = new ArrayList<MeterForecastingDataWritable>();
        for (int i = 0, count = Math.min(limit, values.size()); i < count; i++) {
            bottom.add(values.get(i));
        }

        return bottom;
    }

    /**
     * Checks if the a byte array is contained in a list of byte arrays.
     *
     * @param array the list of arrays to search.
     * @param hash the array to find.
     * @return the index in the array if the item exists or -1.
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
