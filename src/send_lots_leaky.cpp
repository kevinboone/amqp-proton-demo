/*
  send_lots_leaky.cpp
  
  This is a version of send_lots.cpp that has a deliberate memory leak.
  It is a demonstration of how to use valgrind to detect this kind of 
  problem. After compiling, run the example under valgrind:

  $ valgrind --leak-check=full ./bin/send_lots_leaky.

  The more messages it sends, the worse the leak gets.

  Broker settings are in main(), at the end.
 */ 

#include <unistd.h>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/target_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
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
  std::string address;
  std::string user;
  std::string password;

  public:
    LoggingHandler (const std::string &address, 
            const std::string &user,  const std::string &password, 
            int number_to_send)
      {
      this->number_to_send = number_to_send;
      this->sent = 0;
      this->closed = false;
      this->address = address;
      this->user = user;
      this->password = password;
      }

  protected:
    int sent;
    int number_to_send;
    bool closed;

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
    void on_receiver_close (proton::receiver &r) override { LOG_FUNC; }
    void on_delivery_settle (proton::delivery &d) override { LOG_FUNC; }
    void on_connection_open (proton::connection &c) override { LOG_FUNC; }
    void on_connection_close (proton::connection &c) override { LOG_FUNC; }
    void on_sender_drain_start (proton::sender &s) override { LOG_FUNC; }

    /** on_tracker_accept() is called when the application recieves a
          "disposition" frame with status=accepted. The receiving application
          is allowed to send a 'rejected' disposition. */
    void on_tracker_accept (proton::tracker &t) override 
      {
      LOG_FUNC;
      if (sent == number_to_send && !closed)
        {
        std::cout << "Closing connection" << std::endl;
        /** If the selected number of messages has been sent, we will 
              close the connection, while will cause the container to
              stop. However, at the point we call close(), there are still
              unsettled messages. The container will not stop until these
              have all been processed. */
        t.connection().close();
        closed = true;
        }
      }

    /** on_container_start: create a sender. This will initate various
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
      std::cout << "creating sender" << std::endl;
      c.open_sender (address, conn_options);
      std::cout << "created sender" << std::endl;
      // If an exception is thrown here, the container will stop
      }

    /** on_sendable() is called when the sender has been set up. It 
          indicates that the link is ready for a send operation (if
          there is sufficient link credit). */
    void on_sendable (proton::sender &s) override 
      {
      LOG_FUNC;
      std::cout << "my link credit is now " << s.credit() << std::endl;
      // Keep sending whilst the link has credit to send, or
      //   we reach the planned number of messages
      // We don't have to use all our credit here -- the container
      //   will call on_sendable() again whenever the receiver allows
      //   us to send.
      // If we have sufficient credit on entry to this method, and if the
      //   messages are small enough, the entire set of messages to send
      //   may fit into a single TCP frame
      // The messages are not actually sent until this method finishes,
      //   whether there is credit or not
      while (s.credit() && (sent < number_to_send))
        {
        std::cout << "Sending message" << std::endl;
        proton::message *msg = new proton::message ("Hello, world");
        // Set the message ID (for all messages) to 'foo'.
        // What does the broker do if you don't set a message ID at all?
        // Does it depend on protocol?
        s.send (*msg);
        //delete (msg); // LEAK!
        sent++;
        std::cout << "sent messages = " << sent << std::endl;
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

