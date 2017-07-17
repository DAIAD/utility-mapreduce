package eu.daiad.mapreduce.hbase.model;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.Months;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import eu.daiad.mapreduce.hbase.EnumAggregationJobParameter;

public class DateUtils {

    private Configuration conf;

    public DateUtils(Configuration conf) {
        this.conf = conf;
    }

    public Interval<DateTime> getDateInterval() {
        // Compute time interval
        DateTime now = new DateTime();

        // By default the interval is equal to [fist day of the previous month,
        // last day of the next month]. This is a temporary solution for
        // handling UTC timestamps in HBase and different utility time zones.
        DateTime from = now.minusMonths(1).dayOfMonth().withMinimumValue();
        DateTime to = now.plusMonths(1).dayOfMonth().withMaximumValue();

        if ((!StringUtils.isBlank(conf.get(EnumAggregationJobParameter.DATE_FROM.getValue()))) ||
            (!StringUtils.isBlank(conf.get(EnumAggregationJobParameter.DATE_TO.getValue())))) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(conf.get(EnumAggregationJobParameter.DATE_FORMAT.getValue(), "dd/MM/yyyy"));

            from = formatter.parseDateTime(conf.get(EnumAggregationJobParameter.DATE_FROM.getValue())).minusMonths(1).dayOfMonth().withMinimumValue();
            to = formatter.parseDateTime(conf.get(EnumAggregationJobParameter.DATE_TO.getValue())).plusMonths(1).dayOfMonth().withMaximumValue();

            if (Months.monthsBetween(from, to).getMonths() < 2) {
                throw new IllegalArgumentException("Interval must be at least 3 months");
            }
        }
        from = from.hourOfDay().setCopy(0).minuteOfHour().setCopy(0).secondOfMinute().setCopy(0).millisOfSecond().setCopy(0);
        to = to.hourOfDay().setCopy(23).minuteOfHour().setCopy(0).secondOfMinute().setCopy(0).millisOfSecond().setCopy(0);

        return new Interval<DateTime>(from, to);
    }
}
