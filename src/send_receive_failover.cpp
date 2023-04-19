/* 
  receive_failover.cpp

  This example demonstrates Proton's built-in failover support. It requires 
  a mesh of message brokers (e.g., Artemis), listening on different ports
  or hosts (or both).  

  The program just sends and receives messages, to the same queue, as
  fast as it can. As it does so, it logs the number of messages sent and
  received. It also logs the error messages that will be generated during
  a failover.

  One of the brokers is considered to be 'primary', and has a hostname
  and port supplied as the usual first argument to connection::open_sender()
  and connection::open_receiver(). The 'backup' broker is supplied to
  the proton::connection_options. In principle, multiple backup brokers
  can be supplied this way. I guess this way of doing setting up failover
  follows from the fact that the Proton sender/receiver objects only have
  one 'address' attribute.

  There is, in fact, no significant distinction between the 
  'primary' and 'backup' brokers -- they are just alternatives. The only
  difference seems to be that the 'primary' will be used first, if it is
  up. If the primary isn't up when the program starts, it will happily
  connect to the backup.

  Note that sending and receiving concurrently in what is, in effect, 
  a single-threaded application is not particularly effective. Ideally,
  the sender and the receiver need to have the same link credit settings,
  or one will swamp the other. The receiver's link credit is set in
  this program, but the sender's link credit will depend on the broker. 
 */

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/message.hpp>
#include <proton/delivery.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/receiver_options.hpp>

#include <unistd.h>
#include <iostream>

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"
#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

class MyHandler : public proton::messaging_handler 
  {
  protected:
    std::string address;
    std::string user;
    std::string password;
    std::string backup;
    int sent;
    int received;

  public:
    MyHandler (const std::string &_address, const std::string &_backup, 
          const std::string &_user, const std::string &_password) :
        address (_address), user (_user), password (_password), 
        backup (_backup), sent (0), received (0)
      {}

    /** Just log that this method was called. */
    void on_connection_open (proton::connection &c) override 
      { 
      proton::messaging_handler::on_connection_open (c);
      // Note that in Proton 0.37.0, virtual_host() seems to be broken,
      //   and always shows an empty string
      std::string host = c.virtual_host();
      std::cout << "Connected to '" << host << "'" << std::endl;
      LOG_FUNC; 
      }

    /** Just log that this method was called. */
    void on_connection_close (proton::connection &c) override 
      { 
      LOG_FUNC; 
      proton::messaging_handler::on_connection_close (c);
      }

    /** Just log that this method was called. */
    void on_connection_error (proton::connection &c) override 
      { 
      LOG_FUNC; 
      proton::messaging_handler::on_connection_error (c);
      }

    /** Just log the error. */
    void on_error (const proton::error_condition &ec) override 
      { 
      LOG_FUNC; 
      std::cout << "error: " << ec << std::endl;
      proton::messaging_handler::on_error (ec);
      }

    void on_container_start (proton::container &c) 
      {
      LOG_FUNC; 
      std::vector<std::string> failovers {backup};
      proton::connection_options conn_options;
      conn_options.user(user);
      conn_options.password(password);
      conn_options.sasl_allowed_mechs ("PLAIN");
      conn_options.sasl_allow_insecure_mechs (true);
      conn_options.failover_urls (failovers);
      proton::reconnect_options reconnect_options;
      conn_options.reconnect (reconnect_options);
      proton::receiver_options receiver_options;
      receiver_options.credit_window (1000);
      c.open_receiver (address, receiver_options, conn_options);
      c.open_sender (address, conn_options);
      }

    /** on_message -- just report that we received a message */
    void on_message (proton::delivery& dlv, proton::message& msg) 
      {
      received++;
      if (received % 1000 == 0)
        {
        std::cout << "Received " << received << " messages" << std::endl;
        }
      }

    /** on_sendable -- send a message whenever we are allowed to do so */
    void on_sendable (proton::sender &s) override 
      {
      //LOG_FUNC;
      proton::message msg ("Hello, world");
      s.send (msg);
      sent++;
      if (sent % 1000 == 0)
        {
        std::cout << "Sent " << sent << " messages" << std::endl;
        }
      }
  };


int main(int argc, char **argv) 
  {
  std::string address = "127.0.0.1:5672/foo";
  std::string backup = "127.0.0.1:5673";
  std::string user = "admin";
  std::string password = "admin";

  try 
    {
    MyHandler connect (address, backup, user, password);
    proton::container(connect).run();
    return 0;
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    return 1;
    }
  }
