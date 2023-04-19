# amqp-proton-demo

These files accompany a presentation on the AMQP protocol, and how to use 
Qpid Proton to carry out AMQP messaging operations.

Version 0.1, Kevin Boone, April 2023

## Prerequisites

To run these examples you will need:

- GCC with C++ support and template library
- Qpid Proton development files (e.g., `dnf install qpid-proton-cpp-devel`)
- GNU make (optional -- these examples are easy to compile manually)
- `tcpdump` or WireShark, to trace the communication at the wire level
- A message broker that supports AMQP (e.g., Apache Artemis). Some examples
  require two brokers in a failover group
- A tool for producing messages to, or consuming them from, a broker 
  (although the sample programs here will do, at a pinch)
- `valgrind`, if you want to run the memory leak example

## Building and running

All the examples are completely self-contained and can be built 
individually at the command line. For example:

    $ g++ -o send_lots send_lots.cpp -lqpid-proton-cpp

None of the examples take any command-line arguments. To change basic
configuration like host and port, see the top of the `main()` method
in each file.

## Examples

`send_lots` -- send a sequence of messages to the same address on a broker

`receive_lots` -- receive a sequence of messages from the same address on a
broker

`receive_lots_multiple_consumers` -- receive a sequence of messages from the 
same address on a broker, using two different consumers (links) in the same
session. The example also shows how a Proton application might request
'topic-like' or 'queue-like' behaviour from the message broker. 

`server` -- a direct receiver. Listens for incoming connections, and accepts
messages on address `foo`. Shows some basic error handling

`container_per_thread` -- demonstrates how to consume messages from a broker on
multiple concurrent connections, where Proton itself is not thread-safe (as it
was not in builds before C++-11).

`receive_client_ack` -- demonstrates client acknowledgement. The program 
consumes from a message broker, but some messages can be rejected. What
happens at that point depends on the message broker -- probably the 
message will go to a dead-letter queue. This program also shows how to
trap errors arising from message payload format conversion

`send_receive_failover` -- sends and receives messages from a pair of
brokers, and illustrates failover between them

`receive_selector` -- consumes messages filtered by a selector. Demonstrates,
as a side-effect, how to use proton::codec to format custom data types

`send_lots_tls` -- sends messages over a TLS-encrypted connection. Demonstrates
how to enable TLS and specify a trusted certificate.

`send_lots_leaky` -- like `send_lots.cpp`, but with a deliberate memory 
leak. Try to find it using `valgrind`.

`send_across_threads` -- demonstrates how to run a Proton container in a
background thread, and use it to send messages in a (hopefully) thread-safe
way.


