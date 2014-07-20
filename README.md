pgq-consumer
============

A PGQ consumer written in Java, using Spring.

What's PGQ?
-----------

[PGQ](https://wiki.postgresql.org/wiki/PGQ_Tutorial) is the queueing solution from [Skytools](https://wiki.postgresql.org/wiki/Skytools), which was written by [Skype](http://www.skype.com/en/). It's a neat way of writing database triggers that send events to an event queue in [PostgreSQL](http://www.postgresql.org/), which you can then poll with the PGQ API. An implementation of this polling is available in this library. 

A good presentation on PGQ is [available on SlideShare](http://www.slideshare.net/adorepump/skytools-pgq-queues-and-applications).

How do I use it?
----------------

As mentioned before, this code originated from a Spring application, so it assumes two things:

1. That you've set up PGQ in your PostgreSQL database
2. That your application has a DataSource pointing to that database

Once you have those, create a ```PGQConsumer```:
  
    String queueName = "myQueue";                         // What you called your queue in pgq.create_queue()
    String consumerName = "myConsumer";                   // A name unique to this application
    DataSource dataSource = ...                           // Initialised and pointing at your database
    PGQEventHandler eventHandler = new MyEventHandler();  // Your callback for each event
    PGQConsumer pgqConsumer = new PGQConsumer(queueName, consumerName, dataSource, eventHandler);

The ```PGQConsumer``` is a ```Runnable```, so put it into a pool for continuous execution, and away you go.
