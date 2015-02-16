package com.brandwatch.pgqconsumer;

import static com.google.common.collect.Maps.newHashMap;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

public class EventDataProcessor {

    private final Splitter ampSplitter = Splitter.on('&');
    private final Splitter equalsSplitter = Splitter.on('=');

    private URLCodec urlCodec = new URLCodec("UTF-8");

    /**
     * Process the PGQ event by splitting the key-value map String into a Map object.
     *
     * @param data The String stored in the PGQ event's data column.
     * @return The Map representation of the input.
     */
    public Map<String, String> processData(String data) {
        Preconditions.checkNotNull(data);
        Map<String, String> result = newHashMap();

        if (!data.contains("=")) {
            return result;
        }

        Iterable<String> keyValues = ampSplitter.split(data);

        for (String keyValue : keyValues) {
            Iterator<String> parts = equalsSplitter.split(keyValue).iterator();

            String key = decode(parts.next());

            String value;
            if (parts.hasNext()) {
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
