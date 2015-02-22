package com.brandwatch.pgqconsumer;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EventDataProcessorTest {

    private EventDataProcessor eventDataProcessor;

    @Before
    public void setup() {
        eventDataProcessor = new EventDataProcessor();
    }

    @Test(expected = NullPointerException.class)
    public void givenANullDataString_processData_throwsANullPointerException() {
        eventDataProcessor.processData(null);
    }

    @Test
    public void givenAnEmptyString_processData_returnsAnEmptyMap() {
        Map<String, String> processedData = eventDataProcessor.processData("");
        Assert.assertTrue(processedData.isEmpty());
    }

    @Test
    public void givenAStringWithNoKeyValues_processData_returnsAnEmptyMap() {
        Map<String, String> processedData = eventDataProcessor.processData("a b");
        Assert.assertTrue(processedData.isEmpty());
    }

    @Test
    public void givenAStringWithOneKeyValue_processData_returnsAMapWithOneKeyValue() {
        Map<String, String> processedData = eventDataProcessor.processData("a=b");
        Assert.assertEquals("b", processedData.get("a"));
    }

    @Test
    public void givenAStringWithTwoKeyValues_processData_returnsAMapWithTwoKeyValues() {
        Map<String, String> processedData = eventDataProcessor.processData("a=b&c=d");
        Assert.assertEquals("b", processedData.get("a"));
        Assert.assertEquals("d", processedData.get("c"));
    }

    @Test
    public void givenAStringWithOneEncodedKeyValue_processData_returnsOneDecodedKeyValue() {
        Map<String, String> processedData = eventDataProcessor.processData("a%20a=b");
        Assert.assertEquals("b", processedData.get("a a"));
    }
}
