/*
  receive_lots.cpp

  A simple proton receiver with lots of logging. Running this with
    the PNM_TRACE_FRAME environment variable set can yield some insight
    into how Proton interacts with the AMQP wire protocol. 

  Broker settings are in main(), at the end.
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
    int received;
    int number_to_receive;
    bool closed;

  public:
    LoggingHandler (const std::string &address, 
            const std::string &user,  const std::string &password, 
            int number_to_receive)
      {
      this->number_to_receive = number_to_receive;
      this->received = 0;
      this->closed = false;
      this->address = address;
      this->user = user;
      this->password = password;
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

    /** on_container_start: create a receiver. This will initate various
        steps at the AMQP level.
        1. Do SASL authentication
        2. Send, and receive, the "open", "begin", and "attach" frames
        3. Receive a "flow" frame that states the remaining link credit
           allowed by the receiver (1000, by default) 
         The "begin" frame indicates that a session is to be created, 
           while "attach" indicates that a link has been assigned to
           the session. However, the on_transport_open() and  
           on_session_start() callbacks will not be invoked until
           this method completes. */ 
    void on_container_start (proton::container &c) override 
      {
      LOG_FUNC;
      proton::receiver_options recv_options;
      // By setting the number of credits to 3, and receiving (say)
      //   10 messages, we will see the messages transfers in groups of
      //   3, with a disposition after each one. on_message() will be
      //   called in groups of 3. All this is only visible if we look at
      //   the wire protocol.
      recv_options.credit_window (0x3);
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
    int count = 10;

    LoggingHandler h (address, user, password, count);
    proton::container container (h);
    container.run();
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    }

  return 0;
  }

