package com.brandwatch.pgqconsumer;


import static com.google.common.collect.Maps.newHashMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.springframework.jdbc.core.RowMapper;

import com.google.common.base.Splitter;

/**
 * RowMapper implementation for creating PGQ Event objects.
 */
public class PGQEventRowMapper implements RowMapper<PGQEvent> {
    private final Splitter ampSplitter = Splitter.on('&');
    private final Splitter equalsSplitter = Splitter.on('=');

    public PGQEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        long id = rs.getLong("ev_id");
        Date time = rs.getDate("ev_time");
        long txid = rs.getLong("ev_txid");
        int retry = rs.getInt("ev_retry");
        String type = rs.getString("ev_type");
        String data = rs.getString("ev_data");
        String extra1 = rs.getString("ev_extra1");
        String extra2 = rs.getString("ev_extra2");
        String extra3 = rs.getString("ev_extra3");
        String extra4 = rs.getString("ev_extra4");

        Map<String, String> dataMap = processData(data);

        return new PGQEvent(id, time, txid, retry, type, dataMap, extra1, extra2, extra3, extra4);
    }

    private URLCodec urlCodec = new URLCodec("UTF-8");

    /**
     * Process the PGQ event by splitting the key-value map String into a Map object.
     *
     * @param data The String stored in the PGQ event's data column.
     * @return The Map representation of the input.
     */
    private Map<String, String> processData(String data) {
        Iterable<String> keyValues = ampSplitter.split(data);

        Map<String, String> result = newHashMap();
        for (String keyValue : keyValues) {
            Iterator<String> parts = equalsSplitter.split(keyValue).iterator();

            String key = decode(parts.next());

            String value;
            if(parts.hasNext()) {
                value = decode(parts.next());
            } else {
                value = null;
            }

            result.put(key, value);
        }

        return result;
    }

    /**
     * Utility function to URL decode a key.
     *
     * @param encodedKey The key to decode.
     * @return The decoded key.
     */
    private String decode(String encodedKey) {
        String key;
        try {
            key = urlCodec.decode(encodedKey);
        } catch (DecoderException e) {
            key = encodedKey;
        }
        return key;
    }
}
