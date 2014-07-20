package com.brandwatch;

import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Consumes events from the PGQ event queue. Follows the recipe on the following URL:
 * http://wiki.postgresql.org/wiki/PGQ_Tutorial#Writing_a_PGQ_consumer
 *
 * @author jamess
 */
public class PGQConsumer implements Runnable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final int REGISTER_SUCCESS = 1;
    private static final long BACKOFF_MILLIS = 100;
    private static final int EVENT_RETRY_SECONDS = 10;

    private final String queueName;
    private final String consumerName;
    private final JdbcTemplate jdbcTemplate;
    private final PGQEventHandler eventHandler;

    public PGQConsumer(String queueName, String consumerName, DataSource dataSource, PGQEventHandler eventHandler) {
        this.queueName = queueName;
        this.consumerName = consumerName;
        jdbcTemplate = new JdbcTemplate(dataSource);
        this.eventHandler = eventHandler;
    }

    /**
     * Polls PGQ for events and processes them when found.
     */
    public void run() {
        log.info("Starting consumer");
        registerIfNeeded();
        log.debug("Polling for a batch every {}ms", BACKOFF_MILLIS);
        while (!Thread.currentThread().isInterrupted()) {
            Long batchId = getNextBatchId();
            if (batchId != null) {
                List<PGQEvent> events = getNextBatch(batchId);
                for (PGQEvent event : events) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    try {
                        eventHandler.handle(event);
                    } catch (Throwable t) {
                        log.error("Got exception processing event", t);
                        retryEvent(batchId, event.getId(), EVENT_RETRY_SECONDS);
                    }
                }
                finishBatch(batchId);
            } else if (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(BACKOFF_MILLIS);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                    // This seems counter intuitive but Thread.isInterrupted() goes back to false when InterruptedException is thrown.
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void registerIfNeeded() {
        boolean registerSuccess = register();
        if (registerSuccess) {
            log.info("PGQ consumer registered successfully for the first time.");
        } else {
            log.info("PGQ consumer was already registered.");
        }
    }

    private void retryEvent(long batchId, long eventId, int eventRetryTime) {
        String sql = "SELECT * FROM pgq.event_retry(?, ?, ?)";
        Integer result = jdbcTemplate.queryForObject(sql, Integer.class, batchId, eventId,
                eventRetryTime);

        if (result != 1) {
            log.error("Error scheduling event {} of batch {} for retry", eventId, batchId);
        }
    }

    /**
     * Get the next batch of events.
     *
     * @param batchId Batch of events to retrieve.
     * @return List of Event objects that were in that batch.
     */
    private List<PGQEvent> getNextBatch(long batchId) {
        log.debug("Getting next events batch for ID {}", batchId);

        String sql = "select * from pgq.get_batch_events(?);";
        return jdbcTemplate.query(sql, new PGQEventRowMapper(), batchId);
    }

    /**
     * Finish the given batch.
     *
     * @param batchId The batch to finish.
     * @return 1 if batch was found, 0 otherwise.
     */
    private int finishBatch(long batchId) {
        log.debug("Finishing batch ID {}", batchId);

        String sql = "select pgq.finish_batch(?)";
        return jdbcTemplate.queryForObject(sql, Integer.class, batchId);
    }

    /**
     * Get the ID of the next batch to be processed.
     *
     * @return The next batch ID to process, or null if there are no more events available.
     */
    private Long getNextBatchId() {
        String sql = "select pgq.next_batch(?, ?)";
        return jdbcTemplate.queryForObject(sql, Long.class, queueName, consumerName);
    }

    /**
     * @return The result of the registration. 1 means success, 0 means already registered.
     */
    public boolean register() {
        log.info("Registering consumer");

        String sql = "select pgq.register_consumer(?, ?)";
        Integer result = jdbcTemplate.queryForObject(sql, Integer.class, queueName, consumerName);

        return REGISTER_SUCCESS == result;
    }
}
