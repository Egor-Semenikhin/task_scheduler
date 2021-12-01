#include <iostream>
#include <atomic>
#include <thread>
#include <chrono>
#include <vector>

#include "task_scheduler.h"
//
//int main()
//{
//	task_scheduler scheduler(15);
//
//	scheduler.suspend_all_tasks();
//
//	std::atomic<size_t> counter = 0;
//	constexpr size_t Max = 100000000;
//
//	for (size_t i = 0; i < Max; ++i)
//	{
//		scheduler.schedule_task([&counter]() { ++counter; });
//	}
//
//	scheduler.resume_all_tasks();
//
//	while (true)
//	{
//		if (counter == Max)
//		{
//			break;
//		}
//		std::this_thread::sleep_for(std::chrono::milliseconds(1));
//	}
//
//	std::cout << counter << std::endl;
//}

std::atomic<size_t> counter = 0;

class task final
{
private:
	task_scheduler& _scheduler;
	int _counter = 0;

public:
	task(task_scheduler& scheduler)
		: _scheduler (scheduler)
	{
	}

	void update()
	{
		++counter;
		++_counter;

		if (_counter < 10)
		{
			_scheduler.schedule_task([this]() { update(); });
		}
	}
};

int main()
{
	task_scheduler scheduler(15);

	scheduler.suspend_all_tasks();

	constexpr size_t Max = 1000000;

	std::vector<task> tasks(Max, task(scheduler));

	for (size_t i = 0; i < Max; ++i)
	{
		scheduler.schedule_task([&t = tasks[i]]() { t.update(); });
	}

	scheduler.resume_all_tasks();

	while (true)
	{
		if (counter == Max * 10)
		{
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		//scheduler.suspend_all_tasks();
		//std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		//scheduler.resume_all_tasks();
	}

	std::cout << counter << std::endl;
}
