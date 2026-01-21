# Task Engine

Task Engine is a multithreaded application for job scheduling developed in C++. 
It takes care of the user's jobs simultaneously by a constant number of workers who are threads and guarantees strong failure handling, retries, timeouts, and logging.

The focus of this project is creating dependable concurrency mechanisms and job management in life cycle, like those used in today’s task schedulers and backend systems.

---

## Features

- Concurrent job execution using a custom thread pool
- Thread-safe blocking job queue
- Job lifecycle tracking: 
  *(Pending → Running → Retrying → Success / Failed / TimedOut)*
- Retry support with configurable policies:
  - Fixed backoff
  - Exponential backoff
- Timeout handling (records timeouts without unsafe thread termination)
- Structured logging:
  - timestamp, log level, thread ID, optional job ID
- Execution metrics:
  - jobs submitted / succeeded / failed
  - retries performed
  - timeouts recorded
  - average runtime
- Shutdown with no deadlocks or hanging threads


