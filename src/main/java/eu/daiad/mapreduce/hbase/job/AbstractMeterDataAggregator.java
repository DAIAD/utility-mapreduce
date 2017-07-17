package eu.daiad.mapreduce.hbase.job;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import eu.daiad.mapreduce.hbase.model.Interval;

public abstract class AbstractMeterDataAggregator extends HBaseMapReduceJob {

    protected enum EnumTimeInterval {
        UNDEFINED(0), HOUR(3600), DAY(86400);

        private final int value;

        private EnumTimeInterval(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    protected EnumTimeInterval interval;

    protected AbstractMeterDataAggregator(EnumTimeInterval interval) {
        this.interval = interval;
    }

    protected Interval<byte[]> getScanRowKeyInterval(short partition, DateTime dateFrom, DateTime dateTo) {
        byte[] partitionBytes = Bytes.toBytes(partition);

        long from = (Long.MAX_VALUE / 1000) - (dateTo.getMillis() / 1000);
        from = from - (from % interval.getValue());
        byte[] fromBytes = Bytes.toBytes(from);

        long to = (Long.MAX_VALUE / 1000) - (dateFrom.getMillis() / 1000);
        to = to - (to % interval.getValue());
        byte[] toBytes = Bytes.toBytes(to);

        // Scanner row key prefix start
        byte[] fromRowKey = new byte[partitionBytes.length + fromBytes.length];

        System.arraycopy(partitionBytes, 0, fromRowKey, 0, partitionBytes.length);
        System.arraycopy(fromBytes, 0, fromRowKey, partitionBytes.length, fromBytes.length);

        // Scanner row key prefix end
        byte[] toRowKey = new byte[partitionBytes.length + toBytes.length];

        System.arraycopy(partitionBytes, 0, toRowKey, 0, partitionBytes.length);
        System.arraycopy(toBytes, 0, toRowKey, partitionBytes.length, toBytes.length);

        toRowKey = calculateTheClosestNextRowKeyForPrefix(toRowKey);

        return new Interval<byte[]>(fromRowKey, toRowKey);
    }

}
