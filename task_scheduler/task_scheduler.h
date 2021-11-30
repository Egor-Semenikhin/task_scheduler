#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

class task_scheduler final
{
private:
	class task_wrapper_base;
	class task_queue;
	class worker_thread;
	class task_queue_holder;

	template <typename TTaskFunction>
	class task_wrapper;

private:
	std::unique_ptr<task_queue[]> _queues;
	std::unique_ptr<worker_thread[]> _workers;
	std::mutex _mutexSuspendResume;
	uint32_t _threadsCount;
	bool _isSuspended = false;

	static constexpr size_t TASK_QUEUE_ALIGNMENT = 128;

public:
	explicit task_scheduler(uint32_t threadsCount);
	~task_scheduler();

	template <typename TTaskFunction>
	void schedule_task(TTaskFunction taskFunction);

	void suspend_all_tasks();
	void resume_all_tasks();

private:
	void add_task(std::unique_ptr<task_wrapper_base>&& taskWrapper);
	uint32_t thread_index(uint32_t index) const;
};

class task_scheduler::task_wrapper_base
{
public:
	virtual ~task_wrapper_base() = default;
	virtual void do_work() = 0;
};

template <typename TTaskFunction>
class task_scheduler::task_wrapper final : public task_wrapper_base
{
private:
	TTaskFunction _taskFunction;

public:
	explicit task_wrapper(TTaskFunction&& taskFunction) noexcept;
	void do_work() override;
};

template <typename TTaskFunction>
void task_scheduler::schedule_task(TTaskFunction taskFunction)
{
	add_task(std::make_unique<task_wrapper<TTaskFunction>>(std::move(taskFunction)));
}

template <typename TTaskFunction>
task_scheduler::task_wrapper<TTaskFunction>::task_wrapper(TTaskFunction&& taskFunction) noexcept
	: _taskFunction(std::move(taskFunction))
{
}

template <typename TTaskFunction>
void task_scheduler::task_wrapper<TTaskFunction>::do_work()
{
	_taskFunction();
}
