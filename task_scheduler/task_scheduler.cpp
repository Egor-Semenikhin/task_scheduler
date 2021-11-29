#include "task_scheduler.h"

#include <atomic>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cassert>

class task_scheduler::task_queue final
{
public:
	enum class state : uint32_t
	{
		normal,
		worker_sleeping
	};

private:
	std::atomic<uint32_t> _flag = { 0 };

	alignas(TASK_QUEUE_ALIGNMENT)
	std::queue<std::unique_ptr<task_wrapper_base>> _tasks;
	state _state = state::worker_sleeping;

public:
	bool try_capture() noexcept;
	void release() noexcept;
	void add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper);
	bool is_empty() const noexcept;

	void set_state(state value) noexcept;
	state get_state() const noexcept;

	std::unique_ptr<task_wrapper_base> pop_task() noexcept;
};

class task_scheduler::worker_thread final
{
private:
	enum class state : uint32_t
	{
		ready,
		stop,
		sleeping
	};

private:
	std::thread _thread;
	std::mutex _mutex;
	std::condition_variable _conditional;
	task_scheduler* _scheduler = nullptr;
	uint32_t _queueIndex = 0;
	std::atomic<state> _state = state::sleeping;

public:
	worker_thread();
	~worker_thread();

	void start(task_scheduler& scheduler, uint32_t queueIndex);
	void wake_up();

private:
	void thread_func();
	bool wait_for_initial_wake_up();
	bool try_to_do_task();
	bool try_go_to_sleep();
};

class task_scheduler::task_queue_holder final
{
private:
	task_queue& _queue;

public:
	explicit task_queue_holder(task_queue& queue) noexcept;
	~task_queue_holder() noexcept;
};

task_scheduler::task_scheduler(uint32_t threadsCount)
	: _queues(new task_queue[threadsCount, TASK_QUEUE_ALIGNMENT])
	, _workers(new worker_thread[threadsCount])
	, _threadsCount(threadsCount)
{
	for (uint32_t i = 0; i < threadsCount; ++i)
	{
		_workers[i].start(*this, i);
	}
}

task_scheduler::~task_scheduler()
{
}

void task_scheduler::add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper)
{
	thread_local uint32_t lastQueueIndex = 0;

	while (true)
	{
		for (uint32_t i = 0; i < _threadsCount; ++i)
		{
			const uint32_t queueIndex = thread_index(lastQueueIndex + i);
			task_queue& queue = _queues[queueIndex];

			if (!queue.try_capture())
				[[unlikely]]
			{
				continue;
			}

			lastQueueIndex = queueIndex + 1;

			{
				const task_queue_holder queueHolder(queue);

				queue.add_task(std::move(taskWrapper));

				if (queue.get_state() == task_queue::state::worker_sleeping)
				{
					queue.set_state(task_queue::state::normal);
				}
				else
				{
					return;
				}
			}

			_workers[queueIndex].wake_up();
			return;
		}
	}
}

uint32_t task_scheduler::thread_index(uint32_t index) const
{
	if (index >= _threadsCount)
		[[unlikely]]
	{
		index -= _threadsCount;
	}
	return index;
}

task_scheduler::task_queue_holder::task_queue_holder(task_queue& queue) noexcept
	: _queue(queue)
{
}

task_scheduler::task_queue_holder::~task_queue_holder() noexcept
{
	_queue.release();
}

bool task_scheduler::task_queue::try_capture() noexcept
{
	uint32_t expected = 0;
	return _flag.compare_exchange_strong(expected, 1);
}

void task_scheduler::task_queue::release() noexcept
{
	_flag.store(0);
}

void task_scheduler::task_queue::add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper)
{
	_tasks.emplace(std::move(taskWrapper));
}

bool task_scheduler::task_queue::is_empty() const noexcept
{
	return _tasks.empty();
}

std::unique_ptr<task_scheduler::task_wrapper_base> task_scheduler::task_queue::pop_task() noexcept
{
	std::unique_ptr<task_wrapper_base> result = std::move(_tasks.front());
	_tasks.pop();
	return result;
}

void task_scheduler::task_queue::set_state(state value) noexcept
{
	_state = value;
}

task_scheduler::task_queue::state task_scheduler::task_queue::get_state() const noexcept
{
	return _state;
}

task_scheduler::worker_thread::worker_thread()
	: _thread([this]() { thread_func(); })
{
}

task_scheduler::worker_thread::~worker_thread()
{
	// Notify thread about stopping
	{
		std::lock_guard<std::mutex> lock(_mutex);

		switch (_state)
		{
		case state::sleeping:
			_state = state::stop;
			_conditional.notify_one();
			break;

		case state::ready:
			_state = state::stop;
			break;

		case state::stop:
			assert(false);
			break;
		}
	}

	_thread.join();
}

void task_scheduler::worker_thread::start(task_scheduler& scheduler, uint32_t queueIndex)
{
	_scheduler = &scheduler;
	_queueIndex = queueIndex;
}

void task_scheduler::worker_thread::wake_up()
{
	std::lock_guard<std::mutex> lock(_mutex);

	if (_state == state::sleeping)
	{
		_state = state::ready;
	}
	else
	{
		assert(_state == state::stop);
	}

	_conditional.notify_one();
}

void task_scheduler::worker_thread::thread_func()
{
	if (!wait_for_initial_wake_up())
	{
		return;
	}

	while (true)
	{
		const bool next = try_to_do_task();

		if (_state == state::stop)
			[[unlikely]]
		{
			return;
		}

		if (next)
			[[likely]]
		{
			continue;
		}

		if (!try_go_to_sleep())
		{
			return;
		}
	}
}

bool task_scheduler::worker_thread::wait_for_initial_wake_up()
{
	std::unique_lock<std::mutex> lock(_mutex);

	state currentState;

	_conditional.wait(
		lock,
		[this, &currentState]()
			{
				currentState = _state;
				return currentState != state::sleeping;
			}
		);

	if (currentState == state::stop)
		[[unlikely]]
	{
		return false;
	}
	else
	{
		assert(currentState == state::ready);
		return true;
	}
}

bool task_scheduler::worker_thread::try_to_do_task()
{
	bool failedToCapture = false;

	for (uint32_t i = 0; i < _scheduler->_threadsCount; ++i)
	{
		task_queue& queue = _scheduler->_queues[_scheduler->thread_index(_queueIndex + i)];

		if (!queue.try_capture())
			[[unlikely]]
		{
			failedToCapture = true;
			continue;
		}

		std::unique_ptr<task_scheduler::task_wrapper_base> task;

		{
			const task_queue_holder queueHolder(queue);

			if (queue.is_empty())
				[[unlikely]]
			{
				continue;
			}

			task = queue.pop_task();
		}

		try
		{
			task->do_work();
		}
		catch (...)
		{
			// No error tracking for now
		}

		return true;
	}

	return failedToCapture;
}

bool task_scheduler::worker_thread::try_go_to_sleep()
{
	task_queue& queue = _scheduler->_queues[_queueIndex];

	if (!queue.try_capture())
		[[unlikely]]
	{
		return true;
	}

	if (!queue.is_empty())
		[[unlikely]]
	{
		queue.release();
		return true;
	}

	std::unique_lock<std::mutex> lock(_mutex);

	if (_state == state::stop)
	{
		return false;
	}

	assert(_state == state::ready);

	_state = state::sleeping;
	queue.set_state(task_queue::state::worker_sleeping);

	queue.release();

	_conditional.wait(
		lock,
		[this]() { return _state != state::sleeping; }
		);

	return true;
}
