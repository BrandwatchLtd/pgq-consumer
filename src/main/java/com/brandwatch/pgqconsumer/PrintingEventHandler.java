package com.brandwatch.pgqconsumer;

public class PrintingEventHandler implements PGQEventHandler {

    public void handle(PGQEvent event) throws Exception {
        System.out.println(event.getId());
    }
}
