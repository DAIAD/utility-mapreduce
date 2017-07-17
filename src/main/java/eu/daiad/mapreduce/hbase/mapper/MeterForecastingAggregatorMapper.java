package eu.daiad.mapreduce.hbase.mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTimeZone;

import eu.daiad.mapreduce.hbase.EnumAggregationJobParameter;
import eu.daiad.mapreduce.hbase.model.DateUtils;
import eu.daiad.mapreduce.hbase.model.EnumGranularity;
import eu.daiad.mapreduce.hbase.model.EnumGroupType;
import eu.daiad.mapreduce.hbase.model.Group;
import eu.daiad.mapreduce.hbase.model.GroupCollection;
import eu.daiad.mapreduce.hbase.model.MeterForecastingData;
import eu.daiad.mapreduce.hbase.model.MeterForecastingDataWritable;

/**
 * Mapper for smart water meter data aggregation job.
 */
public class MeterForecastingAggregatorMapper extends AbstractMapper<ImmutableBytesWritable, MeterForecastingDataWritable> {

    /**
     * Job counters.
     */
    private static enum Counters {
        /**
         * Number of rows in the input table.
         */
        MAPPER_INPUT_ROWS,
        /**
         * Number of SWM readings in all input table rows.
         */
        MAPPER_INPUT_DATA_POINTS,
        /**
         * Number of rows written by the mapper.
         */
        MAPPER_OUTPUT_ROWS,
        /**
         * Number of groups.
         */
        MAPPER_GROUPS;
    }

    /**
     * Column family bytes.
     */
    private byte[] columnFamily;

    /**
     * All groups that require aggregation
     */
    private GroupCollection groups;

    /**
     * Parses input file with group members and populates an instance of {@link GroupCollection}.
     *
     * @param conf job configuration.
     * @return the number of unique groups.
     * @throws FileNotFoundException if the input file is not found.
     * @throws IOException if an I/O exception occurs.
     * @throws NoSuchAlgorithmException if the hashing algorithms is not supported.
     */
    private int parseMembers(Configuration conf) throws FileNotFoundException, IOException, NoSuchAlgorithmException {
        groups = new GroupCollection();

        File cachedFile = Paths.get("./", conf.get(EnumAggregationJobParameter.FILENAME_GROUPS.getValue())).toFile();

        try (BufferedReader reader = new BufferedReader(new FileReader(cachedFile))) {
            String line;

            line = reader.readLine();
            while (line != null) {
                String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ";", 0);

                groups.add(EnumGroupType.fromString(tokens[0]),
                           tokens[1],
                           tokens[2],
                           tokens[3],
                           md.digest(tokens[3].getBytes("UTF-8")),
                           DateTimeZone.forID(tokens[4]));

                line = reader.readLine();
            }
        }

        return groups.size();
    }

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, MeterForecastingDataWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        columnFamily = Bytes.toBytes(ensureParameter(conf, EnumAggregationJobParameter.COLUMN_FAMILY.getValue()));

        try {
            md = MessageDigest.getInstance("MD5");

            parseMembers(conf);

            context.getCounter(Counters.MAPPER_GROUPS).increment(groups.size());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        }

        // Compute time interval
        DateUtils dateUtils = new DateUtils(conf);
        dateInterval = dateUtils.getDateInterval();
    }

    /**
     * Called once for each key/value pair in the input split.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {

        // Extract data points
        List<MeterForecastingData> data = getMeterForecastingData(row, values, context);

        // Create key/value pairs
        createKeyValues(data, context);
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, MeterForecastingDataWritable>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    /**
     * Extracts meter data points from a single HBASE row.
     *
     * @param row the row key.
     * @param values HBASE row values.
     * @param context the context passed to the {@link Mapper} implementation.
     * @return a collection of {@link MeterForecastingData} objects.
     */
    private List<MeterForecastingData> getMeterForecastingData(ImmutableBytesWritable row, Result values, Context context) {
        // Meter data loaded from a single HBASE row
        List<MeterForecastingData> data = new ArrayList<MeterForecastingData>();

        // Get key values for the column family
        NavigableMap<byte[], byte[]> columnFamilyMap = values.getFamilyMap(columnFamily);

        // Get time stamp and meter serial number hash bytes
        byte[] timeBucketBytes = Arrays.copyOfRange(values.getRow(), 2, 10);
        byte[] serialHashBytes = Arrays.copyOfRange(values.getRow(), 10, 26);

        long timeBucket = Bytes.toLong(timeBucketBytes);

        Float difference = null;
        long timestamp = 0;

        for (Entry<byte[], byte[]> entry : columnFamilyMap.entrySet()) {
            // Extract time stamp offset and compute actual time stamp
            int offset = Bytes.toInt(Arrays.copyOfRange(entry.getKey(), 0, 4));
            timestamp = ((Long.MAX_VALUE / 1000) - (timeBucket + offset)) * 1000L;

            // Extract column qualifier bytes
            int length = Arrays.copyOfRange(entry.getKey(), 4, 5)[0];
            byte[] columnQualifierBytes = Arrays.copyOfRange(entry.getKey(), 5, 5 + length);

            if (Bytes.toString(columnQualifierBytes).equals("d")) {
                difference = Bytes.toFloat(entry.getValue());
            }

            if (difference != null) {
                data.add(new MeterForecastingData(timestamp, serialHashBytes, difference));
                difference = null;
            }
        }

        // Update counters
        context.getCounter(Counters.MAPPER_INPUT_ROWS).increment(1);
        context.getCounter(Counters.MAPPER_INPUT_DATA_POINTS).increment(data.size());

        return data;
    }

    /**
     * Creates new key/value pairs for inserting into HBASE output table.
     *
     * @param data Meter data.
     * @param context the context passed to the {@link Mapper} implementation.
     */
    private void createKeyValues(List<MeterForecastingData> data, Context context) throws IOException {
        byte[] rowKey;

        // For every SWM reading ...
        for (MeterForecastingData item : data) {
            // scan all groups ...
            for (Group group : groups.getValues()) {
                // and if a serial contributes to the total values ...
                int index = group.indexOf(item.getSerialHash());
                if (index > -1) {
                    // then for every granularity level create a key/value
                    for (EnumGranularity granularity : EnumGranularity.class.getEnumConstants()) {
                        long datetime = unixTimestampToLong(item.getUtcTimestamp(), group.getTimezone(), granularity);

                        if (checkInterval(datetime, group.getTimezone(), granularity)) {
                            // Construct key
                            try {
                                rowKey = createRowKey(group.getKey(), granularity, datetime);
                            } catch (Exception e) {
                                throw new IOException(e);
                            }

                            ImmutableBytesWritable key = new ImmutableBytesWritable(rowKey, 0, rowKey.length);

                            // Add key/value to the context
                            try {
                                context.write(key, new MeterForecastingDataWritable(datetime,
                                                                         group.getSerial(index),
                                                                         item.getSerialHash(),
                                                                         item.getDifference()));

                                context.getCounter(Counters.MAPPER_OUTPUT_ROWS).increment(1);
                            } catch (InterruptedException e) {
                                throw new IOException(e);
                            }
                        }
                    }
                }
            }
        }
    }

}
