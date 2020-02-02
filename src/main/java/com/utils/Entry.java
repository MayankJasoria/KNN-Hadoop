package com.utils;

public class Entry implements Comparable<Entry> {
    private String key;
    private String value;

    public Entry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }


    @Override
    public int compareTo(Entry entry) {
        return this.key.compareTo(entry.getKey());
    }
}
