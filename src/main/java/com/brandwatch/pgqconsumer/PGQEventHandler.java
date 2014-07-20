package com.brandwatch.pgqconsumer;

/**
 * Interface for any PGQ event handlers.
 */
public interface PGQEventHandler {
    void handle(PGQEvent event) throws Exception;
}
