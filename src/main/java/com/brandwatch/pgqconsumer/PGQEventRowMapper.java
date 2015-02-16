package com.brandwatch.pgqconsumer;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

/**
 * RowMapper implementation for creating PGQ Event objects.
 */
public class PGQEventRowMapper implements RowMapper<PGQEvent> {

    private EventDataProcessor eventDataProcessor = new EventDataProcessor();

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

        Map<String, String> dataMap = eventDataProcessor.processData(data);

        return new PGQEvent(id, time, txid, retry, type, dataMap, extra1, extra2, extra3, extra4);
    }
}
