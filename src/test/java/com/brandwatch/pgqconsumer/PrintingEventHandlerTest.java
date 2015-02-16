package com.brandwatch.pgqconsumer;

import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PrintingEventHandlerTest {

    private PrintingEventHandler handler;

    @Mock
    private PrintStream mockedOut;
    private PrintStream originalOut;

    @Before
    public void setup() {
        handler = new PrintingEventHandler();
        originalOut = System.out;
        System.setOut(mockedOut);
    }

    @After
    public void teardown() {
        System.setOut(originalOut);
    }

    @Test(expected = NullPointerException.class)
    public void givenANullEvent_whenHandleIsCalled_aNullPointerExceptionIsThrown() throws Exception {
        handler.handle(null);
    }

    @Test
    public void givenAnEvent_whenHandleIsCalled_theEventIdIsPrinted() throws Exception {
        PGQEvent event = Mockito.mock(PGQEvent.class);
        Mockito.when(event.getId()).thenReturn(1L);
        handler.handle(event);
        Mockito.verify(mockedOut).println(1L);
    }

}
