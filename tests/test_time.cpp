#include <sodium/sodium.h>
#include <sodium/time.h>
#include <queue>
#include <iostream>
#include <assert.h>

struct test_impl : sodium::timer_system_impl<int>
{
    test_impl() : next_seq(0), now_(0) {}

    struct entry {
        entry(int t_, std::function<void()> callback_, long long seq_)
        : t(t_), callback(callback_), seq(seq_) {}
        int t;
        std::function<void()> callback;
        long long seq;
        bool operator < (const entry& other) const {
            if (t < other.t) return true;
            if (t > other.t) return false;
            return seq < other.seq;
        }
        bool operator == (const entry& other) const {
            return seq == other.seq;
        }
    };

    std::mutex lock;
    sodium::impl::thread_safe_priority_queue<entry> entries;
    long long next_seq;
    int now_;

    /*!
     * Set a timer that will execute the specified callback at the specified time.
     * This function MUST be thread safe.
     *
     * @return A function that can be used to cancel the timer. This function MUST
     *     be thread safe. It does NOT need to guarantee that the callback won't
     *     be called after it has returned.
     */
    virtual std::function<void()> set_timer(int t, std::function<void()> callback)
    {
        lock.lock();
        entry e(t, callback, ++next_seq);
        entries.push(e);
        lock.unlock();
        return [this, e] () {
            lock.lock();
            entries.remove(e);
            lock.unlock();
        };
    }

    /**
     * Return the current clock time.
     */
    virtual int now() {
        return now_;
    }

    void set_time(int t) {
        now_ = t;
        while (true) {
            lock.lock();
            boost::optional<entry> oe = entries.pop_if([t] (const entry& e) { return e.t <= t; });
            lock.unlock();
            if (oe)
                oe.get().callback();
            else
                break;
        }
    }
};

int main(int argc, char* argv[])
{
    std::shared_ptr<test_impl> impl(new test_impl);
    sodium::timer_system<int> ts(impl);
    sodium::cell_sink<boost::optional<int>> period(boost::optional<int>(500));
    sodium::stream<int> timer1 = sodium::periodic_timer(ts, period);
    sodium::stream<int> timer2 = sodium::periodic_timer<int>(ts, boost::optional<int>(1429));
    std::vector<std::string> out;
    auto kill1 = timer1.listen([&out] (int t) {
        char buf[128];
        sprintf(buf, "%5d one", t);
        out.push_back(buf);
    });
    auto kill2 = timer2.listen([&out] (int t) {
        char buf[128];
        sprintf(buf, "%5d two", t);
        out.push_back(buf);
    });
    sodium::stream_sink<sodium::unit> sAskCurrentTime;
    sodium::stream<int> sCurrentTime = sAskCurrentTime.snapshot(ts.time);
    auto kill3 = sCurrentTime.listen([&out] (int t) {
        char buf[128];
        sprintf(buf, "%5d ---", t);
        out.push_back(buf);
    });
    for (int t = 0; t <= 10656; t += 666) {
        if (t >= 4000)
            period.send(boost::optional<int>());
        if (t >= 5000)
            period.send(boost::optional<int>(2000));
        impl->set_time(t);
        sAskCurrentTime.send(sodium::unit());
    }
    kill1();
    kill2();
    kill3();
    for (auto it = out.begin(); it != out.end(); ++it)
        std::cout << *it << std::endl;
    bool success = out == std::vector<std::string>({
        "    0 ---",
        "  500 one",
        "  666 ---",
        " 1000 one",
        " 1332 ---",
        " 1429 two",
        " 1500 one",
        " 1998 ---",
        " 2000 one",
        " 2500 one",
        " 2664 ---",
        " 2858 two",
        " 3000 one",
        " 3330 ---",
        " 3500 one",
        " 3996 ---",
        " 4287 two",
        " 4662 ---",
        " 5328 ---",
        " 5500 one",
        " 5716 two",
        " 5994 ---",
        " 6660 ---",
        " 7145 two",
        " 7326 ---",
        " 7500 one",
        " 7992 ---",
        " 8574 two",
        " 8658 ---",
        " 9324 ---",
        " 9500 one",
        " 9990 ---",
        "10003 two",
        "10656 ---"
    });
    std::cout << (success ? "PASS" : "FAIL") << std::endl;
    return success ? 0 : 1;
}
