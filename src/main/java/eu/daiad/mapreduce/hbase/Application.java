package eu.daiad.mapreduce.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import eu.daiad.mapreduce.hbase.job.MeterDataAggregator;
import eu.daiad.mapreduce.hbase.job.MeterForecastingDataAggregator;

public class Application {

    private final static char ARGUMENT_DELIMITER = '=';

    public static void main(String[] args) throws Exception {
        Application application = new Application();
        application.execute(args);
    }

    /**
     * Executes a map reduce job given its name. The job name is declared using
     * the argument with key {@code mapreduce.job.name}.
     *
     * The job arguments are expressed as key value pairs separated by = e.g.
     * key1=value1 key2=value2. During runtime, any white space characters
     * around = are considered part of the key and value respectively.
     *
     * @param args the job arguments expressed as key value pairs e.g. key1=value1 key2=value2.
     * @throws Exception if argument parsing fails or job does not exist.
     */
    private void execute(String[] args) throws Exception {
        // Create configuration
        Configuration config = HBaseConfiguration.create();

        for (String arugment : args) {
            String[] tokens = StringUtils.split(arugment, ARGUMENT_DELIMITER);

            if (tokens.length == 2) {
                config.set(tokens[0], tokens[1]);
            }
        }

        // Add default parameters
        config.set("mapreduce.map.speculative", "false");
        config.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");

        // Create tool
        Tool tool = createTool(config.get(EnumJobMapReduceParameter.JOB_NAME.getValue()));

        // Execute the job
        ToolRunner.run(config, tool, new String[0]);
    }

    /**
     * Creates the appropriate {@link Tool} for running the job given its name.
     *
     * @param jobName the job name.
     * @return a {@link Tool} implementation for running the job.
     * @throws Exception if job with the given name is not implemented.
     */
    private Tool createTool(String jobName) throws Exception {
        switch (EnumJob.fromValue(jobName)) {
            case METER_DATA_AGGREGATE:
                return new MeterDataAggregator();
            case METER_FORECASTING_DATA_AGGREGATE:
                return new MeterForecastingDataAggregator();
            default:
                throw new Exception(String.format("Implementation for job [%s] was not found.", jobName));
        }

    }
}
