package eu.daiad.mapreduce.hbase.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.joda.time.DateTime;

import eu.daiad.mapreduce.hbase.EnumAggregationJobParameter;
import eu.daiad.mapreduce.hbase.combiner.MeterAggregatorCombiner;
import eu.daiad.mapreduce.hbase.mapper.MeterAggregatorMapper;
import eu.daiad.mapreduce.hbase.model.DateUtils;
import eu.daiad.mapreduce.hbase.model.Interval;
import eu.daiad.mapreduce.hbase.model.MeterDataWritable;
import eu.daiad.mapreduce.hbase.reducer.MeterAggregatorReducer;

/**
 * Map Reduce job that computes aggregates over smart water meter data for
 * variable time intervals.
 */
public class MeterDataAggregator extends AbstractMeterDataAggregator {

    public MeterDataAggregator() {
        super(EnumTimeInterval.HOUR);
    }

    /**
     * Returns the {@link Class} that implements the job.
     *
     * @return the {@link Class} implementing the job.
     */
    @Override
    protected Class<?> getJobClass() {
        return MeterDataAggregator.class;
    }

    /**
     * Initializes the HBase table scanners for the job.
     *
     * @param conf the job configuration.
     * @param job the job being initialized.
     * @throws IOException if scanner initialization fails.
     */
    @Override
    protected void setScans(Configuration conf, Job job) throws IOException {
        String inputTableName = conf.get(EnumAggregationJobParameter.INPUT_TABLE.getValue());
        String outputTableName = conf.get(EnumAggregationJobParameter.OUTPUT_TABLE.getValue());
        String columnFamily = conf.get(EnumAggregationJobParameter.COLUMN_FAMILY.getValue());

        // Configure Mapper
        List<Scan> scans = new ArrayList<Scan>();

        short partitions = Short.parseShort(conf.get(EnumAggregationJobParameter.PARTITIONS.getValue()));

        // Compute time interval
        DateUtils dateUtils = new DateUtils(conf);
        Interval<DateTime> dateInterval = dateUtils.getDateInterval();


        for (short p = 0; p < partitions; p++) {
            Interval<byte[]> rowKeyInterval = getScanRowKeyInterval(p, dateInterval.getFrom(), dateInterval.getTo());
            Scan scan = new Scan();

            scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTableName));
            scan.setCaching(1000);
            scan.setCacheBlocks(false);

            scan.addFamily(Bytes.toBytes(columnFamily));

            scan.setStartRow(rowKeyInterval.getFrom());
            scan.setStopRow(rowKeyInterval.getTo());

            scans.add(scan);
        }

        // Configure Mapper
        TableMapReduceUtil.initTableMapperJob(scans,
                                              MeterAggregatorMapper.class,
                                              ImmutableBytesWritable.class,
                                              MeterDataWritable.class,
                                              job,
                                              false);

        // Configure Combiner
        job.setCombinerClass(MeterAggregatorCombiner.class);

        // Configure Reducer
        TableMapReduceUtil.initTableReducerJob(outputTableName,
                                               MeterAggregatorReducer.class,
                                               job,
                                               null,
                                               null,
                                               null,
                                               null,
                                               false);

    }

    /**
     * Performs any job specific initialization.
     *
     * @param job the job being initialized.
     */
    @Override
    protected void configureJob(Job job) {
        job.setNumReduceTasks(1);
    }

    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = createSubmittableJob(getConf(), args);

        if (job == null) {
            return -1;
        }
        job.waitForCompletion(true);

        Counters counters = job.getCounters();

        for (CounterGroup group : counters) {
            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": " + counter.getValue());
            }
        }

        JobStatus status = job.getStatus();
        if (status.getState() != State.SUCCEEDED) {
            throw new Exception("Job execution has failed.");
        }

        return 0;
    }

}
