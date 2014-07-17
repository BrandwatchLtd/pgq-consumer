package com.brandwatch;

/**
 * Interface for any PGQ event handlers.
 */
public interface PGQEventHandler {
    void handle(PGQEvent event) throws Exception;
}
