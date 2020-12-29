"""
depends on
https://github.com/cameron314/concurrentqueue
"""
#ifndef CPP_THREADPOOL_HPP
#define CPP_THREADPOOL_HPP

#include <vector>
#include "trd_lib/concurrentqueue/blockingconcurrentqueue.h"

#define  THREADPOOL_MAX_NUM 48
typedef std::function<void()> Task;

namespace mylib {


    class ThreadPoolNew {
    public:
        moodycamel::BlockingConcurrentQueue<Task> q;
        std::vector<std::thread> pool;
        std::atomic<bool> is_run{true};
        std::atomic<int> idle_thread_num{0};
        ulong queue_size_limit;

    public:
        explicit ThreadPoolNew(unsigned long size = 4, ulong pqueue_size_limit = 102400) {
            queue_size_limit = pqueue_size_limit;
            pool.reserve(size);
            add_thread(size);
        }

        template<class Func, class Ins, class... Args>
        forceinline static Task create_task(Func func, Ins *ins, Args &&... args) {
            return Task([func, ins, args...]() {
                (ins->*func)(args...);
            });
        }

        void wait_finished() {
            is_run = false;
            for (auto &thread : pool) {
                if (thread.joinable())
                    thread.join(); // 等待任务结束， 前提：线程一定会执行完
            }
        }

        inline bool add_task(Task &&task) {
            if (!is_run) {
                std::cerr << "enqueue false: thread pool is not running" << std::endl;\
                throw MyException("enqueue false: thread pool is not running");
            }

            if (!q.enqueue(task)) {
                std::cerr << "enqueue false unkown reason" << std::endl;
                throw MyException("enqueue false unkown reason");
            }
            return true;
        }

        [[nodiscard]] forceinline bool queue_full() const {
            return q.size_approx() >= queue_size_limit;
        }

        void add_thread(unsigned long size) {
            for (; pool.size() < THREADPOOL_MAX_NUM && size > 0; --size) {   //增加线程数量,但不超过 预定义数量 THREADPOOL_MAX_NUM
                pool.emplace_back([this] { //工作线程函数
                    bool is_q_empty = false;
                    while (is_run or !is_q_empty) {

                        Task task;

                        if (q.wait_dequeue_timed(task, std::chrono::milliseconds(100))) {
                            idle_thread_num--;
                            task();//执行任务
                            idle_thread_num++;
                        } else if (!is_run) {
                            is_q_empty = true;
                        }
                    }
                }); // end for adding thread

                idle_thread_num++;
            }
        }

    };

}


#endif //CPP_THREADPOOL_HPP
