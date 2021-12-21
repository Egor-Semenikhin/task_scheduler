#include "task_scheduler.h"

#include <atomic>
#include <queue>
#include <thread>
#include <condition_variable>
#include <cassert>

static constexpr size_t CACHE_LINE_SIZE = 2 * 64;

class alignas(CACHE_LINE_SIZE) task_scheduler::task_queue final
{
private:
	std::atomic<uint32_t> _flag = 0;

	alignas(CACHE_LINE_SIZE)
	std::queue<std::unique_ptr<task_wrapper_base>> _tasks;

public:
	bool try_capture() noexcept;
	void release() noexcept;
	void add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper);
	bool is_empty() const noexcept;

	std::unique_ptr<task_wrapper_base> pop_task() noexcept;
};

class alignas(CACHE_LINE_SIZE) task_scheduler::worker_thread final
{
private:
	enum class state : uint32_t
	{
		working,
		sleeping,
		suspended,
		stop
	};

private:
	task_scheduler* _scheduler = nullptr;
	volatile state _state = state::sleeping;
	uint32_t _queueIndex = 0;
	std::mutex _mutex;
	std::condition_variable _conditional;
	std::thread _thread;

public:
	worker_thread();
	~worker_thread();

	void init(task_scheduler& scheduler, uint32_t queueIndex) noexcept;
	void wake_up();
	void suspend();
	void resume();

private:
	void thread_func();
	bool wait_for_initial_wake_up();
	bool try_to_do_task();
	bool try_go_to_sleep();
	bool try_to_suspend();
	bool wait(std::unique_lock<std::mutex>& lock);
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
	: _queues(new task_queue[threadsCount])
	, _workers(new worker_thread[threadsCount])
	, _threadsCount(threadsCount)
{
	for (uint32_t i = 0; i < threadsCount; ++i)
	{
		_workers[i].init(*this, i);
	}
}

task_scheduler::~task_scheduler()
{
	// We don't need to do anything specific here because
	// by standard it is guaranteed that destructor will be called for _workers first
	// and then for _queues. So we don't have any potential race conditions.
}

uint32_t task_scheduler::threads_count() const
{
	return _threadsCount;
}

void task_scheduler::add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper)
{
	thread_local uint32_t lastQueueIndex = 0;

	const uint32_t firstQueueIndex = lastQueueIndex % _threadsCount;

	while (true)
	{
		for (uint32_t i = 0; i < _threadsCount; ++i)
		{
			const uint32_t queueIndex = thread_index(firstQueueIndex + i);
			task_queue& queue = _queues[queueIndex];

			if (!queue.try_capture())
				[[unlikely]]
			{
				continue;
			}

			lastQueueIndex = lastQueueIndex + i + 1;

			bool wasEmpty;

			{
				const task_queue_holder queueHolder(queue);
				wasEmpty = queue.is_empty();
				queue.add_task(std::move(taskWrapper));
			}

			if (wasEmpty)
			{
				_workers[queueIndex].wake_up();
			}

			return;
		}
	}
}

void task_scheduler::suspend_all_tasks()
{
	const std::lock_guard<std::mutex> lock(_mutexSuspendResume);

	if (_isSuspended)
	{
		return;
	}

	_isSuspended = true;

	for (uint32_t i = 0; i < _threadsCount; ++i)
	{
		_workers[i].suspend();
	}
}

void task_scheduler::resume_all_tasks()
{
	const std::lock_guard<std::mutex> lock(_mutexSuspendResume);

	if (!_isSuspended)
	{
		return;
	}

	_isSuspended = false;

	for (uint32_t i = 0; i < _threadsCount; ++i)
	{
		_workers[i].resume();
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

task_scheduler::worker_thread::worker_thread()
	: _thread([this]() { thread_func(); })
{
}

task_scheduler::worker_thread::~worker_thread()
{
	// Notify thread about stopping
	{
		std::unique_lock<std::mutex> lock(_mutex);

		switch (_state)
		{
		case state::sleeping:
		case state::suspended:
			_state = state::stop;
			lock.unlock();
			_conditional.notify_one();
			break;

		case state::working:
			_state = state::stop;
			break;

		case state::stop:
			assert(false);
			break;
		}
	}

	_thread.join();
}

void task_scheduler::worker_thread::init(task_scheduler& scheduler, uint32_t queueIndex) noexcept
{
	_scheduler = &scheduler;
	_queueIndex = queueIndex;
}

void task_scheduler::worker_thread::wake_up()
{
	std::unique_lock<std::mutex> lock(_mutex);

	switch (_state)
	{
	[[likely]]
	case state::sleeping:
		_state = state::working;
		lock.unlock();
		_conditional.notify_one();
		break;

	[[unlikely]]
	case state::stop:
		assert(false);	// Very bad situation when some other thread accesses scheduler with already called destructor
		return;

	[[unlikely]]
	case state::suspended:
	[[unlikely]]
	case state::working:
		break;
	}
}

void task_scheduler::worker_thread::suspend()
{
	const std::lock_guard<std::mutex> lock(_mutex);

	switch (_state)
	{
	case state::working:
		_state = state::suspended;
		break;

	case state::sleeping:
		_state = state::suspended;
		break;

	case state::stop:
		assert(false);
		break;

	case state::suspended:
		assert(false);
		break;
	}
}

void task_scheduler::worker_thread::resume()
{
	std::unique_lock<std::mutex> lock(_mutex);

	assert(_state == state::suspended);

	_state = state::working;

	lock.unlock();
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
		const bool goNext = try_to_do_task();
		const state currentState = _state;

		if (currentState == state::stop)
			[[unlikely]]
		{
			return;
		}

		if (currentState == state::suspended)
			[[unlikely]]
		{
			if (!try_to_suspend())
				[[unlikely]]
			{
				return;
			}
			else
			{
				continue;
			}
		}

		if (goNext)
			[[likely]]
		{
			continue;
		}

		if (!try_go_to_sleep())
			[[unlikely]]
		{
			return;
		}
	}
}

bool task_scheduler::worker_thread::wait_for_initial_wake_up()
{
	std::unique_lock<std::mutex> lock(_mutex);
	return wait(lock);
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
			task->do_work(_queueIndex);
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

	std::unique_lock<std::mutex> lock(_mutex);

	{
		const task_queue_holder queueHolder(queue);

		if (!queue.is_empty())
			[[unlikely]]
		{
			return true;
		}

		switch (_state)
		{
		[[unlikely]] 
		case state::stop:
			return false;

		[[likely]]
		case state::working:
			_state = state::sleeping;
			break;

		[[unlikely]]
		case state::suspended:
			break;

		[[unlikely]]
		case state::sleeping:
			assert(false);
			break;
		}
	}

	return wait(lock);
}

bool task_scheduler::worker_thread::try_to_suspend()
{
	std::unique_lock<std::mutex> lock(_mutex);

	switch (_state)
	{
	[[unlikely]]
	case state::stop:
		return false;

	[[unlikely]]
	case state::working:
		return true;

	[[likely]]
	case state::suspended:
		break;

	[[unlikely]]
	case state::sleeping:
		assert(false);
		break;
	}

	return wait(lock);
}

bool task_scheduler::worker_thread::wait(std::unique_lock<std::mutex>& lock)
{
	state currentState;
	_conditional.wait(
		lock,
		[this, &currentState]()
			{
				currentState = _state;
				return currentState == state::working || currentState == state::stop;
			}
		);
	return currentState == state::working;
}
