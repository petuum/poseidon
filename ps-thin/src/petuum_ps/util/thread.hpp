#pragma once

#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>

namespace petuum {
class Thread {
public:
  Thread() {}

  virtual ~Thread() {}

  virtual void *operator()() { return 0; }

  int Start() {
    InitWhenStart();
    // From the web: The pthread_create() function is used to create a new
    // thread,
    // with attributes specified by attr, within a process. If attr is NULL, the
    // default attributes are used. If the attributes specified by attr are
    // modified
    // later, the thread's attributes are not affected. Upon successful
    // completion,
    // pthread_create() stores the ID of the created thread in the location
    // referenced
    // by thread.  The thread is created executing start_routine with arg as its
    // sole
    // argument.
    return pthread_create(&thr_, NULL, InternalStart, this);
  }

  void Join() { pthread_join(thr_, NULL); }

protected:
  virtual void InitWhenStart() {}

private:
  static void *InternalStart(void *thread) {
    Thread *t = reinterpret_cast<Thread *>(thread);
    return t->operator()();
  }

  pthread_t thr_;
};
}
