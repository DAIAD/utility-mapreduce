package eu.daiad.mapreduce.hbase.combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import eu.daiad.mapreduce.hbase.model.MeterForecastingDataWritable;

/**
 * Reducer for smart water meter data aggregation job.
 */
public class MeterForecastingAggregatorCombiner extends Reducer<ImmutableBytesWritable, MeterForecastingDataWritable, ImmutableBytesWritable, MeterForecastingDataWritable> {

    /**
     * Counter for the reducer.
     */
    private static enum Counters {
        COMBINER_INPUT_ROWS,
        COMBINER_INPUT_VALUES,
        COMBINER_OUTPUT_ROWS
    }

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Reducer<ImmutableBytesWritable, MeterForecastingDataWritable, ImmutableBytesWritable, MeterForecastingDataWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    /**
     * This method is called once for each key.
     */
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<MeterForecastingDataWritable> values, Context context) throws IOException, InterruptedException {
        List<byte[]> serialHashes = new ArrayList<byte[]>();
        List<MeterForecastingDataWritable> writables = new ArrayList<MeterForecastingDataWritable>();

        context.getCounter(Counters.COMBINER_INPUT_ROWS).increment(1);

        for (MeterForecastingDataWritable value : values) {
            int index = indexOf(serialHashes, value.getSerialHash());
            if (index < 0) {
                writables.add(new MeterForecastingDataWritable(value));
                serialHashes.add(ArrayUtils.clone(value.getSerialHash()));
            } else {
                writables.get(index).merge(value);
            }
            context.getCounter(Counters.COMBINER_INPUT_VALUES).increment(1);
        }

        for (MeterForecastingDataWritable w : writables) {
            context.write(key, w);
            context.getCounter(Counters.COMBINER_OUTPUT_ROWS).increment(1);
        }

    }

    /**
     * Checks if the a byte array is contained in a list of byte arrays.
     *
     * @param array the list of arrays to search.
     * @param hash the array to find.
     * @return the index of the item if it exists or -1.
     */
    private int indexOf(List<byte[]> array, byte[] hash) {
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
