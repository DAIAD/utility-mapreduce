package eu.daiad.mapreduce.hbase.reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;

import eu.daiad.mapreduce.hbase.EnumAggregationJobParameter;
import eu.daiad.mapreduce.hbase.model.AggregatedMeterForecastingData;
import eu.daiad.mapreduce.hbase.model.MeterForecastingDataWritable;
import eu.daiad.mapreduce.hbase.model.UserData;

/**
 * Reducer for smart water meter data aggregation job.
 */
public class MeterForecastingAggregatorReducer extends TableReducer<ImmutableBytesWritable, MeterForecastingDataWritable, ImmutableBytesWritable> {

    /**
     * Counter for the reducer.
     */
    private static enum Counters {
        /**
         * Reducer input rows.
         */
        REDUCER_INPUT_ROWS,
        /**
         * Reducer input values.
         */
        REDUCER_INPUT_VALUES,
        /**
         * Reducer output rows.
         */
        REDUCER_OUTPUT_ROWS,
        /**
         * Total users.
         */
        REDUCER_USERS;
    }

    /**
     * Column family bytes.
     */
    private byte[] columnFamily;

    /**
     * Limit of top-k / bottom-k users to select
     */
    private int limit;

    /**
     * All groups that require aggregation
     */
    private Map<String, UserData> users = new HashMap<String, UserData>();

    /**
     * Parses input file with users and populates a map of {@link UserData}.
     * @param conf job configuration.
     * @throws FileNotFoundException if the input file is not found.
     * @throws IOException if an I/O exception occurs.
     * @throws NoSuchAlgorithmException if the hashing algorithms is not supported.
     */
    private int parseUsers(Configuration conf) throws FileNotFoundException, IOException, NoSuchAlgorithmException {
        File cachedFile = Paths.get("./" + conf.get(EnumAggregationJobParameter.FILENAME_USERS.getValue())).toFile();

        try (BufferedReader reader = new BufferedReader(new FileReader(cachedFile))) {
            String line;

            line = reader.readLine();
            while (line != null) {
                String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ";", 0);

                users.put(tokens[0],  new UserData(tokens[1], tokens[2]));

                line = reader.readLine();
            }
        }

        return users.size();
    }

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Reducer<ImmutableBytesWritable, MeterForecastingDataWritable, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        String columnFamilyName = conf.get(EnumAggregationJobParameter.COLUMN_FAMILY.getValue());
        columnFamily = Bytes.toBytes(columnFamilyName);

        limit = context.getConfiguration().getInt("top.query.limit", 5);

        try {
            parseUsers(conf);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        }

        context.getCounter(Counters.REDUCER_USERS).increment(users.size());
    }

    /**
     * This method is called once for each key.
     */
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<MeterForecastingDataWritable> values, Context context) throws IOException, InterruptedException {

        context.getCounter(Counters.REDUCER_INPUT_ROWS).increment(1);

        AggregatedMeterForecastingData aggregate = new AggregatedMeterForecastingData(key.get(), limit);
        for (MeterForecastingDataWritable value : values) {
            aggregate.add(value);
            context.getCounter(Counters.REDUCER_INPUT_VALUES).increment(1);
        }

        Put p = new Put(key.get());

        byte[] column = Bytes.toBytes("sum");
        p.addColumn(columnFamily, column, Bytes.toBytes(aggregate.getSum()));

        column = Bytes.toBytes("min");
        p.addColumn(columnFamily, column, Bytes.toBytes(aggregate.getMin()));

        column = Bytes.toBytes("max");
        p.addColumn(columnFamily, column, Bytes.toBytes(aggregate.getMax()));

        column = Bytes.toBytes("cnt");
        p.addColumn(columnFamily, column, Bytes.toBytes(aggregate.getCount()));

        column = Bytes.toBytes("avg");
        p.addColumn(columnFamily, column, Bytes.toBytes(aggregate.getAverage()));

        column = Bytes.toBytes("top");
        p.addColumn(columnFamily, column, topToString(aggregate).getBytes(StandardCharsets.UTF_8));

        column = Bytes.toBytes("bottom");
        p.addColumn(columnFamily, column, bottomToString(aggregate).getBytes(StandardCharsets.UTF_8));

        context.write(key, p);
        context.getCounter(Counters.REDUCER_OUTPUT_ROWS).increment(1);
    }

    public String topToString(AggregatedMeterForecastingData aggregate) {
        return writableListToString(aggregate.getTop());
    }

    public String bottomToString(AggregatedMeterForecastingData aggregate) {
        return writableListToString(aggregate.getBottom());
    }

    private String writableListToString(List<MeterForecastingDataWritable> writables) {
        if (writables.size() == 0) {
            return "";
        }

        List<String> tokens = new ArrayList<String>();
        for (MeterForecastingDataWritable w : writables) {
            UserData user = users.get(w.getSerial());
            if (user == null) {
                throw new NullPointerException(String.format("Cannot find user for meter [%s].", w.getSerial()));
            }

            tokens.add(user.getKey());
            tokens.add(user.getUsername());
            tokens.add(w.getSerial());
            tokens.add(Float.toString(w.getDifference()));
        }
        return StringUtils.join(tokens, ";");
    }

}
