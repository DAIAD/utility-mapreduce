package eu.daiad.mapreduce.hbase.model;

public class Interval<T> {

    private T from;

    private T to;

    public Interval(T from, T to) {
        this.from = from;
        this.to = to;
    }

    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }

}
