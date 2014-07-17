package com.brandwatch;

import java.util.Date;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Domain object for a Signals PGQ event. This mirrors the PGQ table(s) in the database.
 */
public class PGQEvent {
    private long id;
    private long time;
    private long txid;
    private int retry;
    private String type;
    private ImmutableMap<String, String> data;
    private String extra1;
    private String extra2;
    private String extra3;
    private String extra4;

    public PGQEvent(long id, Date time, long txid, int retry, String type, Map<String, String> data, String extra1,
            String extra2, String extra3, String extra4) {
        super();
        this.id = id;
        this.time = time.getTime();
        this.txid = txid;
        this.retry = retry;
        this.type = type;
        this.data = ImmutableMap.copyOf(data);
        this.extra1 = extra1;
        this.extra2 = extra2;
        this.extra3 = extra3;
        this.extra4 = extra4;
    }

    public long getId() {
        return id;
    }

    public Date getTime() {
        return new Date(time);
    }

    public long getTxid() {
        return txid;
    }

    public int getRetry() {
        return retry;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getData() {
        return data;
    }

    public String getExtra1() {
        return extra1;
    }

    public String getExtra2() {
        return extra2;
    }

    public String getExtra3() {
        return extra3;
    }

    public String getExtra4() {
        return extra4;
    }

    @Override
    public String toString() {
        return "PGQEvent [id=" + id + ", time=" + time + ", txid=" + txid + ", retry=" + retry
                + ", type=" + type + ", data=" + data + ", extra1=" + extra1 + ", extra2=" + extra2
                + ", extra3=" + extra3 + ", extra4=" + extra4 + "]";
    }
}
