package com.brandwatch.pgqconsumer;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class PGQEventHandlerTest {

    private PGQEventRowMapper eventRowMapper;

    @Before
    public void setup() {
        eventRowMapper = new PGQEventRowMapper();
    }

    @Test(expected = NullPointerException.class)
    public void whenGivenANullResultSet_mapRow_throwsANullPointerException() throws SQLException {
        eventRowMapper.mapRow(null, 0);
    }

}
