package eu.daiad.mapreduce.hbase.mapper;

import java.security.MessageDigest;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;

import eu.daiad.mapreduce.hbase.model.EnumGranularity;
import eu.daiad.mapreduce.hbase.model.Interval;

public abstract class AbstractMapper<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {

    /**
     * Date interval/
     */
    protected Interval<DateTime> dateInterval;


    /**
     * Message digest algorithm for hashing group keys and meter serials.
     */
    protected MessageDigest md;

    /**
     * Checks if a parameter exists and returns its value.
     *
     * @param conf job configuration.
     * @param name parameter name.
     * @throws IllegalArgumentException if parameter does not exist.
     */
    protected String ensureParameter(Configuration conf, String name) throws IllegalArgumentException {
        String value = conf.get(name);
        if (value == null) {
            throw new IllegalArgumentException(String.format("Missing parameter parameter '[%s]'.", name));
        }
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format("Parameter '[%s]' is empty.", name));
        }

        return value;
    }

    protected long unixTimestampToLong(long timestamp, DateTimeZone timezone, EnumGranularity granularity) {
        DateTime date = new DateTime(timestamp, timezone);

        long value = 0;

        switch(granularity) {
            case HOUR:
                value = date.getYear() * 1000000 +
                        date.getMonthOfYear() * 10000 +
                        date.getDayOfMonth() * 100 +
                        date.getHourOfDay();
                break;
            case DAY:
                value = date.getYear() * 1000000 +
                        date.getMonthOfYear() * 10000 +
                        date.getDayOfMonth() * 100;
                break;
            case WEEK:
                DateTime monday = date.withDayOfWeek(DateTimeConstants.MONDAY);

                value = monday.getYear() * 1000000 +
                        monday.getMonthOfYear() * 10000 +
                        monday.getDayOfMonth() * 100;
                break;
            case MONTH:
                value = date.getYear() * 1000000 +
                        date.getMonthOfYear() * 10000 + 100;
                break;
            case YEAR:
                value = date.getYear() * 1000000 + 10100;
                break;
            default:
                throw new IllegalArgumentException(String.format("Granularity [%s] is not supported.", granularity .toString()));
        }

        return value;
    }

    protected byte[] createRowKey(String group, EnumGranularity granularity, long datetime) throws Exception {
        byte[] groupHash = md.digest(group.getBytes("UTF-8"));
        byte[] datetimeBytes = Bytes.toBytes(datetime);

        byte[] rowKey = new byte[groupHash.length + 1 + datetimeBytes.length];

        System.arraycopy(groupHash, 0, rowKey, 0, groupHash.length);
        System.arraycopy(new byte[] { granularity.getValue() }, 0, rowKey, groupHash.length, 1);
        System.arraycopy(datetimeBytes, 0, rowKey, (groupHash.length + 1), datetimeBytes.length);

        return rowKey;
    }

    protected boolean checkInterval(long datetime, DateTimeZone timezone, EnumGranularity granularity) {
        DateTime localFrom = dateInterval.getFrom().withZoneRetainFields(timezone)
                                         .plusMonths(1).dayOfMonth().withMinimumValue();
        DateTime localTo = dateInterval.getTo().withZoneRetainFields(timezone)
                                       .minusMonths(1).dayOfMonth().withMaximumValue();

        long localFromLong = unixTimestampToLong(localFrom.getMillis(), timezone, granularity);
        long localToLong = unixTimestampToLong(localTo.getMillis(), timezone, granularity);

        return ((localFromLong <= datetime) && (datetime <= localToLong));
    }

}
