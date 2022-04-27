package com.brandwatch.pgqconsumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.sql.DataSource;

@RunWith(MockitoJUnitRunner.class)
public class PGQConsumerTest {
    private PrintingEventHandler handler;

    @Mock
    private DataSource ds;

    @Before
    public void setup(){
        handler = new PrintingEventHandler();
    }


    @Test(expected = IllegalArgumentException.class)
    public void whenConsumerIsCreated_withLowInterval_exceptionIsThrown() {
        PGQConsumer pgq = new PGQConsumer("q1","testConsumer",ds,this.handler,40);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenConsumerIsCreated_withHighInterval_exceptionIsThrown() {
        PGQConsumer pgq = new PGQConsumer("q1","testConsumer",ds,this.handler,86400000);
    }

    @Test()
    public void consumerIsCreatedCleanly_withValidInterval() {
        PGQConsumer pgq = new PGQConsumer("q1","testConsumer",ds,this.handler,3600000);
    }

    @After
    public void teardown() {

    }
}
