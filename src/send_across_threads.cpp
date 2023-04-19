/*
  send_across_threads.cpp

  This example demonstrates how to run a Proton container in a background
    thread, and then invoke it to send messages from a different, 'foreground'
    thread. When all the messages have been sent, the thread and the
    container should shut down cleanly -- this is by no means straightforward
    to organize. The foreground thread sends messages at one-second intervals,
    so long as there is sufficient link credit. Otherwise it will block
    until credit is available.

  The implementation provides a thread-safe send() message that can be
    called from a non-Proton thread. This method can be called in an
    imperative style, that is, not from a Proton callback. It's surprisingly
    difficult to make this work.

  The same technique demonstrated here should also work for receiving 
    messages; however, using a reactive programming style to receive is
    less awkward than when sending.

  In the interests of full disclosure, I should point out that the 
    send operation is asynchronous; that is, it will complete before the
    receiving broker acknowledges. A truly synchronous send is very 
    difficult to implement using Proton.  

  Thread synchronization in this example requires both a condition variable
    and a mutex.  Please see the detailed comments in the source for how these 
    are used.

  The whole implementation depends on the notion of a Proton 'work_queue'.
    The sender object has a work queue which can be populated with 
    function calls (I guess they are strictly 'functors', rather than
    functions). When the container is idle, it will pop these calls
    off the queue and action them. This is, so far as I know, the only
    documented way for other threads to do things on a container thread
    safely. Note that, although this code populates the work_queue, the
    dequeuing of calls is entirely handled by the container.

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
#include <proton/work_queue.hpp>
#include <iostream>
#include <condition_variable>
#include <thread>
#include <mutex>

#define BOLD_ON "\x1B[1m"
#define BOLD_OFF "\x1B[0m"
#define LOG_FUNC std::cout << BOLD_ON << __FUNCTION__ << BOLD_OFF << std::endl;

/* 
 * MyHandler is a subclass of proton::messaging_handler. However, it provides
 *   and additional public send() method that should be thread-safe, even
 *   when the container is running on a different thread from the container.
 */
class MyHandler : public proton::messaging_handler 
  {
  protected:
    std::string address;
    std::string user;
    std::string password;
    std::condition_variable ready_to_send; 
    proton::sender sender;
    proton::work_queue *work_queue;
    std::mutex lock;

    // We record the amount of link credit that is reported in 
    //   the on_sendable() method, so we don't try to send in
    //   no-credit situations.
    int current_sender_credit;

    // error_raised will be set to true if ever on_error() is called.
    // We will use this to abort the send() method early in error
    //   conditions (e.g., broker cannot be contacted).
    bool error_raised;

  public:
    MyHandler (const std::string &address, 
            const std::string &user,  const std::string &password)
      {
      this->work_queue = 0;
      this->address = address;
      this->user = user;
      this->password = password;
      this->current_sender_credit = 0;
      this->error_raised = false;
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
    void on_sender_close (proton::sender &s) override { LOG_FUNC; }
    void on_sender_detach (proton::sender &s) override { LOG_FUNC; }
    void on_receiver_open (proton::receiver &r) override { LOG_FUNC; }
    void on_receiver_close (proton::receiver &r) override { LOG_FUNC; }
    void on_delivery_settle (proton::delivery &d) override { LOG_FUNC; }
    void on_connection_open (proton::connection &c) override { LOG_FUNC; }
    void on_connection_close (proton::connection &c) override { LOG_FUNC; }
    void on_sender_drain_start (proton::sender &s) override { LOG_FUNC; }
    void on_tracker_accept (proton::tracker &t) override { LOG_FUNC; }

    /** on_sender_open() is called when the link to the broker has
          been established. Here we can store the sender object, and
          retrieve its work_queue. The use of std::lock_guard here is
          to avoid a situation where a thread is interrupted between
          storing the sender and the work queue -- however, this is a
          conservative measure, and I'm not certain it's necessary */
    void on_sender_open (proton::sender &s) override 
      { 
      LOG_FUNC; 
      std::lock_guard<std::mutex> dummy(lock);
      sender = s;
      work_queue = &s.work_queue(); // Get the sender's work queue
      }

    /** on_container_start: set up connection and create the sender. */ 
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

    /** on_sendable() is called when the sender has been set up, and 
          there is reason to think that the sender has link credit from
          the receiver. In a reactive programming style we would send
          messages in this callback. Here, though, we just notify 
          other threads that are waiting to send. */ 
    void on_sendable (proton::sender &s) override 
      {
      LOG_FUNC;
      std::cout << "my link credit is now " << s.credit() << std::endl;
      std::lock_guard<std::mutex> l(lock);
      // The next two operations must be atomic (I think). We don't 
      //   want other threads that are waiting to send to get woken
      //   up before we have stored the current credit. 
      ready_to_send.notify_all();
      current_sender_credit = s.credit();
      }

    /** In any error condition, set error_raised to true. Note that
	  it isn't unusual to see errors here when tearing down the container.
          At this point -- teardown -- we don't need to worry about 
          implications of setting the error flag. However, there's something 
          be said for supressing the display of _anticipated_ error messages, 
          but this simple implementation does not attempt to do so. */
    void on_error (const proton::error_condition& e) override 
      {
      std::cerr << "unexpected error raised: "
            << e.description() << std::endl;
      error_raised = true;
      // Notify senders that are waiting to send. They won't be able
      //   to send because of the error (most likely); but we don't
      //   want them blocked for ever.
      ready_to_send.notify_all();
      }
 
  public:
    /** send() sends a text message. This is the only method on this
          class that can safely be called from outside a Proton thread. */
    void send (const std::string &s)
       {
       LOG_FUNC;

       {
       // Start of atomic section
       std::unique_lock<std::mutex> l(lock);
       // We need to wait until the work_queue exists, which won't be 
       //   until the link to the broker has been established. We can't
       //   prevent send() being called before this happens, so we need
       //   to wait. We also need to wait until there is sufficient credit,
       //   or an error has been raised.
       while (!work_queue && (current_sender_credit == 0 && !error_raised)) 
	 { 
	 std::cout << "Waiting for sender's work queue to be ready" 
	    << std::endl;
	 ready_to_send.wait (l);
	 }
       // End of atomic section
       }

       // If an error was raised whilst waiting to be able to send, 
       //    throw an exception. Otherwise, put a call to sender.send()
       //    in the work_queue for later processing.
       if (error_raised)
         {
         throw std::runtime_error 
           ("Tried to send in a error condition");
         }
       else
         {
         proton::message msg (s);
         work_queue->add([=]() { this->sender.send (msg); });
         }
       } 

  }; // End of MyHandler

/**
   main() -- start here
*/

int main(int argc, char **argv) 
  {
  try 
    {
    std::string address = "127.0.0.1:5672/foo";
    std::string user = "admin";
    std::string password = "admin";
    int count = 3;

    // Set up the handler and the container in the usual way.
    // But don't run the container here...
    MyHandler h (address, user, password);
    proton::container container (h);

    // Run the container in a separate thread.
    std::thread container_thread = std::thread([&]() 
       { 
       container.run(); 
       std::cout << "container.run() finished" << std::endl; 
       });
    
    // Be aware that execution will reach this point long before
    //   the AMQP link has been set up. We can't assume that anything
    //   is ready, or that no error occurred, just because we get here.

    /* We need a separate a try-catch for MyHandler::send(). This method
         will raised an exception if, for example, the broker is not
         is not available. Merely creating a MyHandler won't raised an
         error, however broken everything else. Eventually, after
         container::run() has been called, MyHandler::on_error will get
         called. But we could be well into MyHandler::send() before
         then. If we don't catch the exception, then the outer catch
         will be invoked. But the handler for the outer catch doesn't
         join() the container thread, so the container will be destructed
         before its scaffolding has been removed. We can't can't join()
         the container thread in the outer exception handler, because
         it might never have been created. Such are the hazards of 
         trying to program in an imperative style with a reactive 
         framework. */ 
    try 
      {
      for (int i = 0; i < count; i++)
        {
        std::cout << "Sending a message\n";
        h.send ("Hello, World\n");
        sleep (1);
        }
      }
    catch (const std::exception& e) 
      {
      std::cerr << e.what() << std::endl;
      }

    container.stop();
    
    // We must call join() to wait for the container thread to finish.
    // Allowing the container to be destroyed by going out of scope 
    // before it has been shut down properly scope), will lead to 
    //   a program abort.
    std::cout << "wait for container thread to finish" << std::endl;
    container_thread.join();
    std::cout << "done" << std::endl;
    } 
  catch (const std::exception& e) 
    {
    // Because the container.run() operation is in a different thread, and
    //   the message send operation has its own try-catch, I think that
    //   only grevious, internal errors will end up in this catch.
    std::cerr << e.what() << std::endl;
    }

  return 0;
  }

