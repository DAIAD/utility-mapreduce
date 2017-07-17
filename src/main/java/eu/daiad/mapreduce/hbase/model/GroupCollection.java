package eu.daiad.mapreduce.hbase.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTimeZone;

public class GroupCollection {

    private Map<String, Group> groups = new HashMap<String, Group>();

    public void add(EnumGroupType type, String groupKey, String areaKey, String serial, byte[] serialHash, DateTimeZone timezone) {
        String key = groupKey + areaKey;

        Group group = groups.get(key);
        if (group == null) {
            group = new Group(type, key, timezone);
            groups.put(key, group);
        }
        group.add(serial, serialHash);
    }

    public Collection<Group> getValues() {
        return groups.values();
    }

    public int size() {
        return groups.size();
    }

}
