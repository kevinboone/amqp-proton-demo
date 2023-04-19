/*
  receive_selector.cpp

  This example demonstrates how to regulate message consumption using a
  selector. Although the AMQP 'link' performative makes allowance for a
  filter specification, and Proton provides a way to set it, Proton gives
  very little help in formatting the filter.

  In AMQP, a filter specification is a 'defined string', that is, a custom,
  annotated data type. We have to use the low-level proton::codec API to
  generate the filter string in the proper format.

  The filter is set on the _source_ of the proton::receiver. That is, the
  filter is a request to the remote system (broker) to behave in a particular
  way. We could filter messages in an application, of course, but that would
  leave the problem of deciding what to do with the messages we aren't going
  to process. A filter is applied at the broker (hence 'source') before any
  messages are actually sent.
*/ 

#include <unistd.h>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/target_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/source_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/delivery.hpp>
#include <proton/tracker.hpp>
#include <iostream>

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"

#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

/* 
 * LoggingHandler is a subclass of proton::messaging_handler
 */
class LoggingHandler : public proton::messaging_handler 
  {
  protected:
    std::string address;
    std::string user;
    std::string password;
    std::string selector;
    int received;
    int number_to_receive;
    bool closed;

  public:
    LoggingHandler (const std::string &address, 
            const std::string &user,  const std::string &password, 
            int number_to_receive, const std::string &selector)
      {
      this->number_to_receive = number_to_receive;
      this->received = 0;
      this->closed = false;
      this->address = address;
      this->user = user;
      this->password = password;
      this->selector = selector;
      }
  protected:
    // Note: in principle, we should invoke the base class method in
    //   all these on_xxx methods. I happen to know that they don't
    //   do anything in this example, but this isn't good practice.
    void on_tracker_reject (proton::tracker &t) override { LOG_FUNC; }
    void on_tracker_settle (proton::tracker &t) override { LOG_FUNC; }
    void on_transport_open (proton::transport &t) override { LOG_FUNC; }
    void on_session_open (proton::session &s) override { LOG_FUNC; }
    void on_session_close (proton::session &s) override { LOG_FUNC; }
    void on_sender_open (proton::sender &s) override { LOG_FUNC; }
    void on_sender_close (proton::sender &s) override { LOG_FUNC; }
    void on_sender_detach (proton::sender &s) override { LOG_FUNC; }
    void on_receiver_open (proton::receiver &r) override { LOG_FUNC; }
    void on_receiver_close (proton::receiver &c) override { LOG_FUNC; }
    void on_delivery_settle (proton::delivery &d) override { LOG_FUNC; }
    void on_connection_open (proton::connection &c) override { LOG_FUNC; }
    void on_connection_close (proton::connection &c) override { LOG_FUNC; }
    void on_sender_drain_start (proton::sender &s) override { LOG_FUNC; }
    void on_tracker_accept (proton::tracker &t) override { LOG_FUNC; }

    /* set_filter_on_source_opts() is a helper function to conceal the
         ugliness of AMQP filter setting. It takes a filter expression
         -- which might be broker-specific, and a source_options object.
         Note that setting the 'source options' on a receiver amounts to making
         requests on what the sender (the broker, here) should do.
         That is, the 'source' is this receiver's source of messages. */
    void set_filter_on_source_opts (proton::source_options &opts, 
        const std::string& selector_str) 
      {
      // A filter_map is a map from aN arbitrary key to a filter expression. 
      // I guess it has to be a map, because multiple filters can be applied. 
      proton::source::filter_map map;
      proton::symbol filter_key ("my_selector"); // Arbitrary name

      // The value stored along with the key is a filter expression as an
      //   AMQP 'described type'. This is a primitive value annotated with
      //   a specific descriptor, making it a kind of custom type. We need
      //   to use proton::codec to write the described type in the 
      //   appropriate format. Although AMQP provides a way for the 
      //   attach performative to specify the filter as a described type,
      //   AMQP says nothing about what the filter expression should
      //   actually be. There is some loose consensus, but no specification.
      proton::value filter_value;
      proton::codec::encoder enc (filter_value);
      enc << proton::codec::start::described()
	      << proton::symbol("apache.org:selector-filter:string")
	      << selector_str
	      << proton::codec::finish();

      // Put the filter spec (arbirary name and expression) into the map
      map.put (filter_key, filter_value);
      // Apply the filters in the map to the source options
      opts.filters (map);
      }

    /** on_container_start: create a receiver. Set the receiver's
        source options to define a selector. */
    void on_container_start (proton::container &c) override 
      {
      LOG_FUNC;
      proton::receiver_options recv_options;
      proton::source_options source_opts;
      set_filter_on_source_opts (source_opts, selector);
      recv_options.source (source_opts);
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
      std::cout << "creating receiver" << std::endl;
      c.open_receiver (address, recv_options, conn_options);
      std::cout << "created receiver" << std::endl;
      // If an exception is thrown here, the container will stop
      }

    void on_message (proton::delivery &d, proton::message &m) override 
      {
      LOG_FUNC;
      received++;
      std::cout << "Received: " << m.body() << std::endl;
      if (received == number_to_receive)
        {
        // This will only shut down the container if there is only one
        //   active connection (as there is in this example)
        d.connection().close();
        }
      }
  };

int main(int argc, char **argv) 
  {
  try 
    {
    std::string address = "127.0.0.1:5672/foo";
    std::string user = "admin";
    std::string password = "admin";
    std::string selector = "foo='bar'";

    LoggingHandler h (address, user, password, 10, selector);
    proton::container container (h);
    container.run();
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    }

  return 0;
  }

