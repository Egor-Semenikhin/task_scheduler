#include <iostream>
#include <atomic>
#include <thread>
#include <chrono>

#include "task_scheduler.h"

int main()
{
	task_scheduler scheduler(15);

	std::atomic<size_t> counter = 0;

	constexpr size_t Max = 100000000;

	for (size_t i = 0; i < Max; ++i)
	{
		scheduler.schedule_task([&counter]() { ++counter; });
	}

	while (true)
	{
		if (counter == Max)
		{
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}
