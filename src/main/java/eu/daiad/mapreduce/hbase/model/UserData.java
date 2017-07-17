package eu.daiad.mapreduce.hbase.model;

public class UserData {

    private String key;

    private String username;

    public UserData(String key, String username) {
        this.key = key;
        this.username = username;
    }

    public String getKey() {
        return key;
    }

    public String getUsername() {
        return username;
    }

}
