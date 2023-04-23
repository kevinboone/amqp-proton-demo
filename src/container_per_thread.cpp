/* 
 Test for receiving on multiple concurrent threads, with each thread getting 
 its own proton::container instance

 This is probably the only safe to consume using multiple, concurrent 
 connections with C++ versions earlier than 11 -- at least, according to
 the Proton docs. After C++-11, it seems that Proton has better internal
 thread safety. 
 
 In this example, we create a new instance of messaging_handler and a
 new instance of container for each thread that is to consume messages.
 That gives us one connection per thread. The containers and handlers
 have absolutely nothing in common, so there should not be any 
 concurrency problems. The handlers pass messages to a common handle_message()
 method, but by the time this method is called, the Proton message has been
 converted to an ordinary std::sttring, allocated on the stack. So, so
 long as handle_message() itself does not get into any trouble with
 threading, this scheme should be thread-safe.

 With later C++ versions, most of the Proton artefacts are thread-safe,
 because the container maintains an internal thread pool for connections.

 Kevin Boone, April 2022 
*/

#include <unistd.h>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/message.hpp>
#include <proton/receiver_options.hpp>
#include <proton/delivery.hpp>
#include <proton/value.hpp>

#include <iostream>
#include <thread>

#define URL "localhost:5672/foo"
#define USER "admin"
#define PASSWORD "admin"
// Number of threads (and thus connections) on which to consume
#define THREADS 10

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"
#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

/** handle_message() is the terminal point for the handling of all messages.
    There are no inherent thread-safety issues, because no Proton artefacts 
    are passed to this method, and the string argument is itself 
    stack-based. */
int handle_message (const std::string &msg)
  {
  std::cout << "Handling message " << msg << std::endl; 
  return 1; // 1 == OK
  }

/** MyHandler -- the usual Proton interaction handler. There will be one
    instance per consuming thread. */
class MyHandler : public proton::messaging_handler 
  {
  int count; // Number of msgs received in this handler 
  int my_num; // Human-readable ID number for this handler instance
  std::string cont_id; // Container ID, for logging purposes

  public:
    MyHandler (int _my_num) : count (0), my_num (_my_num) { LOG_FUNC; }

    // For each container, start a receiver
    void on_container_start (proton::container &c) 
      {
      LOG_FUNC;
      // Open a receiver, etc
      cont_id = c.id(); // Store the ID, for logging
      proton::connection_options conn_options;
      conn_options.user (USER);
      conn_options.password (PASSWORD);
      conn_options.sasl_allowed_mechs ("PLAIN");
      // Need to allow insecure authentication if we will be sending
      //   credentials over a non-TLS connection. 
      conn_options.sasl_allow_insecure_mechs (true);
      proton::receiver_options ro;
      ro.credit_window (10); // 10 is actually the default
      ro.auto_accept (false); // Lets decide whether to ack message ourselves
      c.open_receiver (URL, ro, conn_options);
      }

    void on_message (proton::delivery& dlv, proton::message& msg) 
      {
      LOG_FUNC;
      count++;
      std::cout << "Received " << count << " in handler " 
            << my_num << std::endl; 
      proton::value v = msg.body();
      std::string m = proton::coerce<std::string> (v);
      if (handle_message (m))
        dlv.accept();
      else
        dlv.reject();
      }
  };

/* Handler function for container thread. Just runs the container (and
 * so, never exits except on error)
 */
void run_container (proton::container* cont)
  {
  LOG_FUNC;

  std::cout << "Container thread " << cont->id() << " started" << std::endl;
  try
    {
    cont->run();
    }
  catch (const std::exception& e) 
    {
    std::cerr << "container::run failed: " << e.what() << std::endl;
    }
  };


int main(int argc, char **argv) 
  {
  try 
    {
    // Instantiate a number of containers, each with its own 
    //   proton::messaging_handler
    for (int i = 0; i < THREADS; i++)
      {
      // We must allocate the handler and container using new, so they
      // survives after this for loop finishes.
      MyHandler *handler = new MyHandler (i);
      proton::container *container = new proton::container (*handler);
      new std::thread (run_container, container);
      }

    while (1) { sleep (1); }
    // Don't need to clean up, as we never exit
    
    return 0;
    } 
  catch (const std::exception& e) 
    {
    std::cerr << e.what() << std::endl;
    return 1;
    }
  }

