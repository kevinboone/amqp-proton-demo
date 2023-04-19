/*
  receive_lots_multiple_consumers.cpp

  This example demonstrates how to consume from a broker address using
  two consumers (links, in AMQP terminology). Because there are two
  consumers, this example can illustrate how to use queue-like and
  topic-like consumption in Proton. If you define the TOPIC macro, 
  topic-like consumption will be enabled.  

  The difference is visible in the logging -- with queue-like consumption
  the two consumers will get a message turn-by-turn. With topic-like
  consumption, both consumers will get copies of the same message. So,
  by placing a single message on the address, and watching how the
  consumers are invoked, we can see the difference in behaviour.

  This example can also demonstrate how to consume from multiple, different
  addresses on the same broker. 
  
  Be aware that Proton does not know what a 'queue' is, nor a 'topic'. 
  It just asks the broker to behave in a specific way. The Proton
  code will not _create_ a topic on the broker, if a queue already 
  exists with the same name. In that situation, you might get this
  failure:

    amqp:illegal-state: Address foo is not configured for topic support

  The solution is to define the topic administrative. Alternatively,
  if auto-creation is enabled, attach a Java client to the topic with
  the required name.

*/ 

#include <unistd.h>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/target_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/source_options.hpp>
#include <proton/delivery.hpp>
#include <proton/tracker.hpp>
#include <iostream>

/* Define TOPIC to use topic-style (multicast) messaging. Proton does not
   know what this means -- it just requests a specific capability from
   the broker. */

#define TOPIC

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"
#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

/* 
 * LoggingHandler is a subclass of proton::messaging_handler
 */
class LoggingHandler : public proton::messaging_handler 
  {
  protected:
    std::string host_and_port;
    std::string address;
    std::string user;
    std::string password;
    int received;
    int number_to_receive;
    bool closed;

  public:
    LoggingHandler (const std::string &host_and_port, 
            const std::string &address, const std::string &user,  
            const std::string &password, int number_to_receive)
      {
      this->number_to_receive = number_to_receive;
      this->received = 0;
      this->closed = false;
      this->address = address;
      this->user = user;
      this->password = password;
      this->host_and_port = host_and_port;
      }

  protected:
    // Note: in principle, we should invoke the base class method in
    //   all these on_xxx methods. I happen to know that they don't
    //   do anything in this example, but this isn't good practice.
    void on_tracker_reject (proton::tracker &t) override { LOG_FUNC; }
    void on_tracker_settle (proton::tracker &t) override { LOG_FUNC; }
    void on_transport_open (proton::transport &t) override { LOG_FUNC; }
    void on_session_close (proton::session &s) override { LOG_FUNC; }
    void on_session_open (proton::session &s) override { LOG_FUNC; }
    void on_sender_open (proton::sender &s) override { LOG_FUNC; }
    void on_sender_close (proton::sender &s) override { LOG_FUNC; }
    void on_sender_detach (proton::sender &s) override { LOG_FUNC; }
    void on_receiver_open (proton::receiver &r) override { LOG_FUNC; }
    void on_receiver_close (proton::receiver &c) override { LOG_FUNC; }
    void on_delivery_settle (proton::delivery &d) override { LOG_FUNC; }
    void on_connection_close (proton::connection &c) override { LOG_FUNC; }
    void on_sender_drain_start (proton::sender &s) override { LOG_FUNC; }
    void on_tracker_accept (proton::tracker &t) override { LOG_FUNC; }

    /** on_connection_open() is overridden to create two receivers. This
        will cause on_session_open() to be called implicitly. We could
        create the receivers in on_session_open(), but we'd have to
        cause the session to be created -- opening a connection does not
        have this effect, because a connection can exist with no session. */
    void on_connection_open (proton::connection &c) override 
      { 
      LOG_FUNC;
#ifdef TOPIC
      // Request the 'topic' capability of the broker. Other possibilities
      //   are 'shared' and 'global'
      std::vector<proton::symbol> caps { "topic" };
      proton::source_options source_options;
      source_options.capabilities (caps);
#endif

      proton::receiver_options recv_options;
#ifdef TOPIC
      recv_options.source (source_options);
#endif

      // Create two receivers
      std::cout << "creating receivers" << std::endl;
      c.open_receiver (address, recv_options);
      std::cout << "created receiver 1" << std::endl;
      c.open_receiver (address, recv_options);
      std::cout << "created receiver 2" << std::endl;
      LOG_FUNC; 
      }

    /** on_container_start: create a connection (only). */
    void on_container_start (proton::container &c) override 
      {
      LOG_FUNC;
      proton::connection_options conn_options;
      conn_options.user (user);
      conn_options.password (password);
      // Oddly, even if we set the allowed mechanisms to be PLAIN and
      //   nothing else, the container will still attempt an anonymous
      //   connection before it sends the authentication credentials
      conn_options.sasl_allowed_mechs ("PLAIN");
      // Need to allow insecure authentication if we will be sending
      //   credentials over a non-TLS connection. 
      conn_options.sasl_allow_insecure_mechs (true);
      c.connect (host_and_port, conn_options);
      // If an exception is thrown here, the container will stop
      }

    /** Messages from both consumers will end up in the same on_message()
        method */
    void on_message (proton::delivery &d, proton::message &m) override 
      {
      LOG_FUNC;
      received++;
      std::cout << "received by " << d.receiver() << std::endl;
      std::cout << "total messages " << received << std::endl;
      if (received == number_to_receive)
        {
        std::cout << "Closing connection" << std::endl;
        d.container().stop();
        }
      }
  };

int main(int argc, char **argv) 
  {
  try 
    {
    std::string host_and_port = "127.0.0.1:5672";
    std::string address = "foo";
    std::string user = "admin";
    std::string password = "admin";
    int total = 10;

    LoggingHandler h (host_and_port, address, user, password, total);
    proton::container container (h);
    container.run();
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    }

  return 0;
  }

