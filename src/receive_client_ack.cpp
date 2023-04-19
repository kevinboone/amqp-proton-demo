/** 

  receive_client_ack.cpp

  This example demonstrates how to do 'client acknowledgement' in Proton.
  This applies to messages consumed from a broker -- the consuming client will
  either accept (acknowledge) them, or reject them. In this example, we
  accept all text messages, and reject anything else.

  When on_message() is called, the default response by Proton is to
  accept the delivery. The application can, instead, call one of the
  proton::delivery messages to change that outcome. If a message is
  rejected, the broker will usually not send it again, and it will end
  up on a dead-letter queue (but I imagine this behaviour is configurable).
*/ 

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/message.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/delivery.hpp>

#include <iostream>
#include <unistd.h>

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"
#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;


class MyHandler : public proton::messaging_handler 
  {
  private:
    std::string url;
    std::string user;
    std::string password;

  public:
    MyHandler(const std::string &_url, 
          const std::string &_user, const std::string &_password) :
        url (_url), user (_user), password (_password)
      { LOG_FUNC; }

    /** on_container_start -- create a receiver (which also creates a
          connection and a session. */
    void on_container_start (proton::container &c) 
      {
      LOG_FUNC;
      proton::connection_options co;
      co.user(user);
      co.password(password);
      co.sasl_allowed_mechs ("PLAIN");
      co.sasl_allow_insecure_mechs (true);
      co.reconnect (proton::reconnect_options());
      std::cout << "Creating consumer for address " << url << std::endl;
      c.open_receiver(url, co);
      }

    /** on_message -- incoming messages end up here */
    void on_message (proton::delivery& dlv, proton::message& msg) 
      {
      LOG_FUNC;
      int delivery_count = msg.delivery_count(); 
      std::cout << "Delivery count is " << delivery_count << std::endl;
      try
         {
         // If we can't convert the incoming message to a string, an
         //   exception will be raised.
         std::string s = proton::get<std::string>(msg.body());
         std::cout << "Accept message from " << url << std::endl;
         dlv.accept();
         }
      catch (std::exception &e)
        {
        // Exception raised -- reject message
        std::cout << "Reject message from " << url << std::endl;
        std::cout << "Check DLQ!" << std::endl;
        dlv.reject();
        // Try these alternative error responses:
        //dlv.release();
        //dlv.modify();
	}
      }
  };


int main (int argc, char **argv) 
  {
  std::string address = "127.0.0.1:5672/foo";
  std::string user = "admin";
  std::string password = "admin";

  try 
    {
    MyHandler connect (address, user, password);
    proton::container (connect).run();
    return 0;
    } 
  catch (const std::exception &e) 
    {
    std::cerr << e.what() << std::endl;
    return 1;
    }
  }

