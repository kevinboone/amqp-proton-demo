/*
  server.cpp

  A simple proton direct receiver with lots of logging. Running this with
    the PNM_TRACE_FRAME environment variable set can yield some insight
    into how Proton interacts with the AMQP wire protocol. 

  This example opens a listening connection on the port specified in
    main(), at the end of the code. It expects connections to address
    "foo" (see on_receiver_open). If the sender tries to attach to
    any other address, or connect as a consumer, an error will be
    raised, which should be communicated to the client. 
 */ 

#include <unistd.h>
#include <stdexcept>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/target_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/listen_handler.hpp>
#include <proton/tracker.hpp>
#include <proton/listener.hpp>
#include <proton/delivery.hpp>
#include <iostream>

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"

#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

/* 
 * A basic message receiver, with lots of logging
 */
class ReceiveHandler : public proton::messaging_handler 
  {
  protected:
    std::string address;

  public:
    ReceiveHandler (const std::string &address)
      {
      this->address = address;
      }

  protected:
    void on_tracker_reject (proton::tracker &t) override 
      { LOG_FUNC; proton::messaging_handler::on_tracker_reject (t); }

    void on_tracker_settle (proton::tracker &t) override 
      { LOG_FUNC; proton::messaging_handler::on_tracker_settle (t); }

    void on_tracker_accept (proton::tracker &t) override 
      { LOG_FUNC; proton::messaging_handler::on_tracker_accept (t); }

    void on_transport_open (proton::transport &t) override 
      { LOG_FUNC;  proton::messaging_handler::on_transport_open (t); }

    void on_session_open (proton::session &s) override 
      { LOG_FUNC;  proton::messaging_handler::on_session_open (s); }

    void on_session_close (proton::session &s) override 
      { LOG_FUNC;  proton::messaging_handler::on_session_close (s); }

    void on_sender_open (proton::sender &s) override 
      { 
      LOG_FUNC;  
      proton::messaging_handler::on_sender_open (s); 
      //s.close (proton::error_condition ("Receiving not allowed")); // Fails
      //s.connection().close (proton::error_condition ("Receiving not allowed")); 
      }

    void on_sender_close (proton::sender &s) override 
      { LOG_FUNC;  proton::messaging_handler::on_sender_close (s); }

    void on_sender_detach (proton::sender &s) override 
      { LOG_FUNC;  proton::messaging_handler::on_sender_detach (s); }

    void on_receiver_open (proton::receiver &r) override 
      { 
      LOG_FUNC;  proton::messaging_handler::on_receiver_open (r);
      std::string address = r.target().address();
      std::cout << "target address: " << address << std::endl;
      if (address != "foo")
        {
        //r.close (proton::error_condition ("Invalid address1")); // Fails
        //r.session().close (proton::error_condition ("Invalid address")); // Fails 
        r.connection().close (proton::error_condition ("Invalid address")); 
        }
      }

    void on_receiver_close (proton::receiver &c) override 
      { LOG_FUNC;  proton::messaging_handler::on_receiver_close (c); }

    void on_delivery_settle (proton::delivery &d) override 
      { LOG_FUNC;  proton::messaging_handler::on_delivery_settle (d); }

    void on_connection_open (proton::connection &c) override 
     { LOG_FUNC;  proton::messaging_handler::on_connection_open (c); }

    void on_connection_close (proton::connection &c) override 
      { LOG_FUNC; proton::messaging_handler::on_connection_close (c); }

    void on_sender_drain_start (proton::sender &s) override 
      { LOG_FUNC;  proton::messaging_handler::on_sender_drain_start (s); }

    void on_message (proton::delivery &d, proton::message &msg) override 
      {
      LOG_FUNC;
      std::cout << msg.body() << std::endl;
      //d.reject();
      }

    void on_container_start (proton::container &c) override 
      {
      LOG_FUNC;
      c.listen (address);
      }
  };


int main(int argc, char **argv) 
  {
  try 
    {
    std::string address ("0.0.0.0:5672");

    ReceiveHandler h (address);
    proton::container container (h);
    container.run();
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    }

  return 0;
  }

