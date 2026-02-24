# Task Engine

Task Engine is a production-quality multithreaded job scheduling engine implemented in C++.
It runs user submitted jobs using a fixed-size worker pool with strong concurrency control, retries, timeouts, and structured logging.


-------

## Architecture

 - The engine separates responsibilities across components:
 - Thread Pool – Executes jobs using a fixed number of worker threads.
 - Dispatcher Thread – Controls when jobs are released to workers.
 - Priority Queues – High / Normal / Low priority scheduling.
 - Concurrency Gate – Enforces maximum concurrent running jobs.
 - Rate Limiter (Token Bucket) – Controls dispatch throughput.
 - Metrics & Logging Layer – Provides structured observability.


## Features

Concurrent Execution
 - Custom thread pool with condition variables and atomic synchronization.
 - Thread safe job submission and state changes.

Priority Scheduling
 - Three level priority scheduling:
    - High
    - Normal
    - Low
 - High priority jobs can preempt queued lower-priority jobs.

Retry Policies
- Configurable retry counts.
- Fixed backoff support.
- Exponential backoff with bounded growth.
- Retries handled by dispatcher (no worker thread blocking).

Timeout Enforcement
 - Detects execution timeouts.
 - Tracks timeouts without unsafe thread interruption.
 - Jobs marked as TimedOut with complete lifecycle support.

Cancellation Support
 - Safe cancellation before execution.
 - Prevents additional retries after cancellation request.
 - Correct terminal state accounting.

Concurrency & Rate Limiting
 - Configurable maximum concurrent running jobs.
 - Token bucket dispatch rate limiting.
 - Burst control support.

## Job Lifecycle
Each job transitions through well-defined states:
Pending → Running → Retrying → Success
                               Failed
                               TimedOut
                               Cancelled

## Structured JSON Logging
 - The engine supports JSON logging for observability.
 - Each log entry includes:
  - Timestamp
  - Sequence number
  - Log level
  - Thread ID
  - Optional Job ID
  - Structured event fields

## Execution Metrics
The scheduler aggregates runtime statistics:
 - Jobs submitted
 - Jobs succeeded
 - Jobs failed
 - Retries performed
 - Timeouts recorded
 - Average runtime (ms)

