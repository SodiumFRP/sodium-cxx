/**
 * Copyright (c) 2012-2016, Stephen Blackheath and Anthony Jones
 * Released under a BSD3 licence.
 *
 * C++ implementation courtesy of International Telematics Ltd.
 */

#include "test_sodium.h"
#include <sodium/sodium.h>
#include <sodium/router.h>
#include <boost/optional.hpp>

#include <cppunit/ui/text/TestRunner.h>
#include <stdio.h>
#include <ctype.h>
#include <iostream>

using namespace std;
using namespace sodium;
using namespace boost;


void test_sodium::tearDown()
{
#if defined(SODIUM_V2)
    sodium::collect_cycles();
#endif
}

void test_sodium::stream1()
{
    stream_sink<int> ev;
    std::shared_ptr<string> out = std::make_shared<string>();
    ev.send('?');
    function<void()> unlisten;
    {
        transaction trans;
        ev.send('h');
        unlisten = ev.listen([out] (int ch) {
            *out = *out + (char)ch;
        });
        ev.send('e');
    };
    {
        transaction trans;
        ev.send('l');
        ev.send('l');
        ev.send('o');
    }
    unlisten();
    ev.send('!');
    CPPUNIT_ASSERT_EQUAL(string("eo"), *out);
}

void test_sodium::map()
{
    stream_sink<int> e;
    auto m = e.map([] (const int& x) {
        char buf[128];
        sprintf(buf, "%d", x);
        return string(buf);
    });
    std::shared_ptr<vector<string> > out = std::make_shared<vector<string> >();
    auto unlisten = m.listen([out] (const string& x) { out->push_back(x); });
    e.send(5);
    unlisten();
    vector<string> shouldBe = { string("5") };
    CPPUNIT_ASSERT(shouldBe == *out);
}

void test_sodium::map_optional()
{
    stream_sink<int> e;
    stream<string> m = e.map_optional([] (const int& x) {
        if ((x & 1) == 1) {  // If odd
            char buf[128];
            sprintf(buf, "%d", x);
            return make_optional(string(buf));
        }
        else
            return optional<string>();
    });
    std::shared_ptr<vector<string> > out = std::make_shared<vector<string> >();
    auto unlisten = m.listen([out] (const string& x) { out->push_back(x); });
    e.send(5);
    e.send(6);
    e.send(7);
    e.send(8);
    e.send(9);
    unlisten();
    vector<string> shouldBe = { string("5"), string("7"), string("9") };
    CPPUNIT_ASSERT(shouldBe == *out);
}

void test_sodium::merge_non_simultaneous()
{
    stream_sink<int> e1;
    stream_sink<int> e2;
    std::shared_ptr<vector<int> > out = std::make_shared<vector<int> >();
    auto unlisten = e2.or_else(e1).listen([out] (const int& x) { out->push_back(x); });
    e1.send(7);
    e2.send(9);
    e1.send(8);
    unlisten();
    vector<int> shouldBe = {7,9,8};
    CPPUNIT_ASSERT(shouldBe == *out);
}

void test_sodium::filter()
{
    stream_sink<char> e;
    auto out = std::make_shared<string>();
    auto unlisten = e.filter([] (const char& c) { return isupper(c); })
                     .listen([out] (const char& c) { (*out) += c; });
    e.send('H');
    e.send('o');
    e.send('I');
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("HI"), *out);
}

void test_sodium::filter_optional1()
{
    stream_sink<boost::optional<string>> e;
    auto out = std::make_shared<vector<string>>();
    auto unlisten = filter_optional(e).listen([out] (const string& s) {
        out->push_back(s);
    });
    e.send(boost::optional<string>("tomato"));
    e.send(boost::optional<string>());
    e.send(boost::optional<string>("peach"));
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("tomato"), string("peach") }) == *out);
}

// Sodium v2 now fixes the memory leak in this code.  
void test_sodium::loop_stream1()
{
    stream_sink<int> ea;
    transaction trans;
    stream_loop<int> eb;
    eb.loop(ea);
    trans.close();
    auto out = std::make_shared<vector<int>>();
    auto unlisten = eb.listen([out] (const int& x) { out->push_back(x); });
    ea.send(2);
    ea.send(52);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 2, 52 }) == *out);
}

// Sodium v2 now fixes the memory leak in this code.  
void test_sodium::loop_stream2()
{
    stream_sink<int> ea;
    stream<int> ec;
    {
        transaction trans;
        stream_loop<int> eb;
        ec = ea.map([] (const int& x) { return x % 10; })
                    .merge(eb, [] (const int& x, const int& y) { return x+y; });
        auto eb_out = ea.map([] (const int& x) { return x / 10; })
                        .filter([] (const int& x) { return x != 0; });
        eb.loop(eb_out);
    }
    auto out = std::make_shared<vector<int>>();
    auto unlisten = ec.listen([out] (const int& x) { out->push_back(x); });
    ea.send(2);
    ea.send(52);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 2, 7 }) == *out);
}

void test_sodium::gate1()
{
    stream_sink<char> ec;
    cell_sink<bool> pred(true);
    auto out = std::make_shared<string>();
    auto unlisten = ec.gate(pred).listen([out] (const char& c) { *out += c; });
    ec.send('H');
    pred.send(false);
    ec.send('O');
    pred.send(true);
    ec.send('I');
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("HI"), *out);
}

void test_sodium::once1()
{
    stream_sink<char> e;
    auto out = std::make_shared<string>();
    auto unlisten = e.once().listen([out] (const char& c) { *out += c; });
    e.send('A');
    e.send('B');
    e.send('C');
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("A"), *out);
}

void test_sodium::hold1()
{
    stream_sink<int> e;
    cell<int> b = e.hold(0);
    auto out = std::make_shared<vector<int>>();
    auto unlisten = b.updates().listen([out] (const int& x) { out->push_back(x); });
    e.send(2);
    e.send(9);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 2, 9 }) == *out);
}

void test_sodium::snapshot1()
{
    cell_sink<int> b(0);
    stream_sink<long> trigger;
    auto out = std::make_shared<vector<string>>();
    auto unlisten = trigger.snapshot(b, [out] (const long& x, const int& y) -> string {
        char buf[129];
        sprintf(buf, "%ld %d", x, y);
        return buf;
    }).listen([out] (const string& s) {
        out->push_back(s);
    });
    trigger.send(100l);
    b.send(2);
    trigger.send(200l);
    b.send(9);
    b.send(1);
    trigger.send(300l);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("100 0"), string("200 2"), string("300 1") }) == *out);
}

void test_sodium::snapshot2()
{
    cell_sink<int> b(0);
    cell_sink<int> c(5);
    stream_sink<long> trigger;
    auto out = std::make_shared<vector<string>>();
    auto unlisten = trigger.snapshot(b, c, [out] (const long& x, const int& y, const int& z) -> string {
        char buf[129];
        sprintf(buf, "%ld %d %d", x, y, z);
        return buf;
    }).listen([out] (const string& s) {
        out->push_back(s);
    });
    trigger.send(100l);
    b.send(2);
    trigger.send(200l);
    b.send(9);
    b.send(1);
    trigger.send(300l);
    c.send(3);
    trigger.send(400l);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("100 0 5"), string("200 2 5"), string("300 1 5"), string("400 1 3") }) == *out);
}

/*!
 * When the cell changes, give deltas between old and new values. The first delta
 * is between a specified initial value and the cell's first value. The first
 * value of a hold is defined as any event held in the first transaction, or,
 * failing that, the default value passed to hold.
 *
 * The function f receives the arguments (update value, previous value).
 */
template <typename A, typename Fn>
sodium::stream<typename std::result_of<Fn(A,A)>::type> deltas_with_initial(const sodium::cell<A>& ca, const A& a,
    const Fn& f)
{
    using namespace sodium;
    transaction trans;
    stream<A> sValue = ca.value();
    cell<A> previous = sValue.hold(a);
    auto sOut = sValue.snapshot(previous, f);
    trans.close();
    return sOut;
}

void test_sodium::snapshot_initial_value()
{
    sodium::transaction trans;
    stream_sink<int> sA;
    stream_sink<sodium::unit> sB;
    cell<int> a = sA.hold(5);
    sA.send(10);
    auto out = std::make_shared<vector<int>>();
    auto unlisten = deltas_with_initial(a, 0, [] (int neu, int old) {
        return neu - old;
    }).listen([out] (const int& i) {
        out->push_back(i);
    });
    trans.close();
    sA.send(12);
    sA.send(30);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 10, 2, 18 }) == *out);
}

void test_sodium::value1()
{
    cell_sink<int> b(9);
    transaction trans;
    auto out = std::make_shared<vector<int>>();
    auto unlisten = b.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(2);
    b.send(7);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9, 2, 7 }) == *out);
}

void test_sodium::value_const()
{
    cell<int> b(9);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9 }) == *out);
}

void test_sodium::constant_cell()
{
    cell_sink<int> b(12);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 12 }) == *out);
}

void test_sodium::value_then_map()
{
    cell_sink<int> b(9);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().map([] (const int& x) { return x + 100; })
        .listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(2);
    b.send(7);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 109, 102, 107 }) == *out);
}

/*
 * This is used for tests where value() produces a single initial value on listen,
 * and then we double that up by causing that single initial stream to be repeated.
 * This needs testing separately, because the code must be done carefully to achieve
 * this.
 */
template <class A>
stream<A> doubleUp(const stream<A>& ea)
{
    return ea.merge(ea);
}

void test_sodium::value_then_snapshot()
{
    cell_sink<int> bi(9);
    cell_sink<char> bc('a');
    auto out = std::make_shared<string>();
    transaction trans;
    auto unlisten = bi.value().snapshot(bc).listen([out] (const char& c) { *out += c; });
    trans.close();
    bc.send('b');
    bi.send(2);
    bc.send('c');
    bi.send(7);
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("abc"), *out);
}

void test_sodium::value_then_merge()
{
    cell_sink<int> bi(9);
    cell_sink<int> bj(2);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = bi.value().merge(bj.value(), [] (const int& x, const int& y) -> int { return x+y; })
        .listen([out] (const int& z) { out->push_back(z); });
    trans.close();
    bi.send(1);
    bj.send(4);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 11, 1, 4 }) == *out);
}

void test_sodium::value_then_filter1()
{
    cell_sink<int> b(9);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().filter([] (const int& x) { return true; })
        .listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(2);
    b.send(7);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9, 2, 7 }) == *out);
}

void test_sodium::value_then_filter2a()
{
    cell_sink<optional<int>> b = cell_sink<optional<int>>(optional<int>(9));
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = filter_optional(b.value())
        .listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(optional<int>());
    b.send(optional<int>(7));
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9, 7 }) == *out);
}

void test_sodium::value_then_filter2b()
{
    cell_sink<optional<int>> b = cell_sink<optional<int>>(optional<int>());
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = filter_optional(b.value())
        .listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(optional<int>());
    b.send(optional<int>(7));
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 7 }) == *out);
}

void test_sodium::value_then_once()
{
    cell_sink<int> b(9);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().once()
        .listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(2);
    b.send(7);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9 }) == *out);
}

void test_sodium::value_late_listen()
{
    cell_sink<int> b(9);
    b.send(8);
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    auto unlisten = b.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b.send(2);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 8, 2 }) == *out);
}

void test_sodium::value_then_switch()
{
    transaction trans;
    cell_sink<int> b1(9);
    cell_sink<int> b2(11);
    auto out = std::make_shared<vector<int>>();
    cell_sink<stream<int>> be(b1.value());
    auto unlisten = switch_s(be).listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    b1.send(10);
    // This is an odd sort of case. We want
    // 1. value() is supposed to simulate a cell as an stream, firing the current
    //   value once in the transaction when we listen to it.
    // 2. when we switch to a new stream in switch_s(), any firing of the new stream
    //   that happens in that transaction should be ignored, because cells are
    //   delayed. The switch should take place after the transaction.
    // So we might think that it's sensible for switch_s() to fire out the value of
    // the new cell upon switching, in that same transaction. But this breaks
    // 2., so in this case we can't maintain the "cell as an stream" fiction.
    be.send(b2.value());  // This does NOT fire 11 for the reasons given above.
    b2.send(12);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 9, 10, 12 }) == *out);
}

void test_sodium::mapB1()
{
    cell_sink<int> b(6);
    auto out = std::make_shared<vector<string>>();
    transaction trans;
    auto unlisten = b.map([] (const int& x) {
        char buf[128];
        sprintf(buf, "%d", x);
        return string(buf);
    }).value().listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    b.send(8);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("6"), string("8") }) == *out);
}

void test_sodium::mapB_late_listen()
{
    cell_sink<int> b(6);
    auto out = std::make_shared<vector<string>>();
    b.send(2);
    transaction trans;
    auto unlisten = b.map([] (const int& x) {
        char buf[128];
        sprintf(buf, "%d", x);
        return string(buf);
    }).value().listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    b.send(8);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("2"), string("8") }) == *out);
}

static string fmtInt(const int& x) {
    char buf[128];
    sprintf(buf, "%d", x);
    return string(buf);
}

void test_sodium::apply1()
{
    cell_sink<function<string(const int&)>> bf([] (const int& b) {
        return string("1 ")+fmtInt(b);
    });
    cell_sink<int> ba(5);
    auto out = std::make_shared<vector<string>>();
    transaction trans;
    auto unlisten = apply<int, string>(bf, ba).value().listen([out] (const string& x) {
        out->push_back(x);
    });
    trans.close();
    bf.send([] (const int& b) { return string("12 ")+fmtInt(b); });
    ba.send(6);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("1 5"), string("12 5"), string("12 6") }) == *out);
}

void test_sodium::lift1()
{
    cell_sink<int> a(1);
    cell_sink<int> b(5);
    auto out = std::make_shared<vector<string>>();
    transaction trans;
    auto unlisten = a.lift(b, [] (const int& a_, const int& b_) {
        return fmtInt(a_)+" "+fmtInt(b_);
    }).value().listen([out] (const string& x) {
        out->push_back(x);
    });
    trans.close();
    a.send(12);
    b.send(6);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("1 5"), string("12 5"), string("12 6") }) == *out);
}

void test_sodium::lift_glitch()
{
    transaction trans;
    cell_sink<int> a(1);
    cell<int> a3 = a.map([] (const int& x) { return x * 3; });
    cell<int> a5 = a.map([] (const int& x) { return x * 5; });
    cell<string> b = a3.lift(a5, [] (const int& x, const int& y) {
        return fmtInt(x)+" "+fmtInt(y);
    });
    auto out = std::make_shared<vector<string>>();
    auto unlisten = b.value().listen([out] (const string& s) { out->push_back(s); });
    trans.close();
    a.send(2);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("3 5"), string("6 10") }) == *out);
}

void test_sodium::hold_is_delayed()
{
    stream_sink<int> e;
    cell<int> h = e.hold(0);
    stream<string> pair = e.snapshot(h, [] (const int& a, const int& b) { return fmtInt(a) + " " + fmtInt(b); });
    auto out = std::make_shared<vector<string>>();
    auto unlisten = pair.listen([out] (const string& s) { out->push_back(s); });
    e.send(2);
    e.send(3);
    unlisten();
    CPPUNIT_ASSERT(vector<string>({string("2 0"), string("3 2")}) == *out);
}

struct SB
{
    SB(optional<char> oa_, optional<char> ob_, optional<cell<char>> osw_) : oa(oa_), ob(ob_), osw(osw_) {}
    optional<char> oa;
    optional<char> ob;
    optional<cell<char>> osw;
};

void test_sodium::switch_c1()
{
    transaction trans;
    stream_sink<SB> esb;
    // Split each field out of SB so we can update multiple behaviours in a
    // single transaction.
    cell<char> ba = filter_optional(esb.map([] (const SB& s) { return s.oa; })).hold('A');
    cell<char> bb = filter_optional(esb.map([] (const SB& s) { return s.ob; })).hold('a');
    cell<cell<char>> bsw = filter_optional(esb.map([] (const SB& s) { return s.osw; })).hold(ba);
    cell<char> bo = switch_c(bsw);
    auto out = std::make_shared<string>();
    auto unlisten = bo.value().listen([out] (const char& c) { *out += c; });
    trans.close();
    esb.send(SB(optional<char>('B'),optional<char>('b'),optional<cell<char>>()));
    esb.send(SB(optional<char>('C'),optional<char>('c'),optional<cell<char>>(bb)));
    esb.send(SB(optional<char>('D'),optional<char>('d'),optional<cell<char>>()));
    esb.send(SB(optional<char>('E'),optional<char>('e'),optional<cell<char>>(ba)));
    esb.send(SB(optional<char>('F'),optional<char>('f'),optional<cell<char>>()));
    esb.send(SB(optional<char>(),   optional<char>(),   optional<cell<char>>(bb)));
    esb.send(SB(optional<char>(),   optional<char>(),   optional<cell<char>>(ba)));
    esb.send(SB(optional<char>('G'),optional<char>('g'),optional<cell<char>>(bb)));
    esb.send(SB(optional<char>('H'),optional<char>('h'),optional<cell<char>>(ba)));
    esb.send(SB(optional<char>('I'),optional<char>('i'),optional<cell<char>>(ba)));
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("ABcdEFfFgHI"), *out);
}

struct SE
{
    SE(optional<char> oa_, optional<char> ob_, optional<stream<char>> osw_) : oa(oa_), ob(ob_), osw(osw_) {}
    optional<char> oa;
    optional<char> ob;
    optional<stream<char>> osw;
};

void test_sodium::switch_s1()
{
    stream_sink<SE> ese;
    stream<char> ea = filter_optional(ese.map([] (const SE& s) { return s.oa; }));
    stream<char> eb = filter_optional(ese.map([] (const SE& s) { return s.ob; }));
    cell<stream<char>> bsw = filter_optional(ese.map([] (const SE& s) { return s.osw; }))
        .hold(ea);
    stream<char> eo = switch_s(bsw);
    auto out = std::make_shared<string>();
    auto unlisten = eo.listen([out] (const char& c) { *out += c; });
    ese.send(SE(optional<char>('A'),optional<char>('a'),optional<stream<char>>()));
    ese.send(SE(optional<char>('B'),optional<char>('b'),optional<stream<char>>()));
    ese.send(SE(optional<char>('C'),optional<char>('c'),optional<stream<char>>(eb)));
    ese.send(SE(optional<char>('D'),optional<char>('d'),optional<stream<char>>()));
    ese.send(SE(optional<char>('E'),optional<char>('e'),optional<stream<char>>(ea)));
    ese.send(SE(optional<char>('F'),optional<char>('f'),optional<stream<char>>()));
    ese.send(SE(optional<char>('G'),optional<char>('g'),optional<stream<char>>(eb)));
    ese.send(SE(optional<char>('H'),optional<char>('h'),optional<stream<char>>(ea)));
    ese.send(SE(optional<char>('I'),optional<char>('i'),optional<stream<char>>(ea)));
    unlisten();
    CPPUNIT_ASSERT_EQUAL(string("ABCdeFGhI"), *out);
}

// NOTE! Currently this leaks memory.
void test_sodium::loop_cell()
{
    stream_sink<int> ea;
    transaction trans;
    cell_loop<int> sum;
    sum.loop(ea.snapshot(sum, [] (const int& x, const int& y) { return x+y; }).hold(0));
    auto out = std::make_shared<vector<int>>();
    auto unlisten = sum.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();

    ea.send(2);
    ea.send(3);
    ea.send(1);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 0, 2, 5, 6 }) == *out);
    CPPUNIT_ASSERT(sum.sample() == 6);
}

void test_sodium::collect1()
{
    stream_sink<int> ea;
    auto out = std::make_shared<vector<int>>();
    stream<int> sum = ea.collect<int>(100, [] (const int& a, const int& s) -> tuple<int, int> {
        return tuple<int, int>(a+s, a+s);
    });
    auto unlisten = sum.listen([out] (const int& x) { out->push_back(x); });
    ea.send(5);
    ea.send(7);
    ea.send(1);
    ea.send(2);
    ea.send(3);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 105, 112, 113, 115, 118 }) == *out);
}

void test_sodium::collect2()
{
    stream_sink<int> ea;
    auto out = std::make_shared<vector<int>>();
    transaction trans;
    cell<int> sum = ea.hold(100).collect<int>(0, [] (const int& a, const int& s) {
        return tuple<int, int>(a+s, a+s);
    });
    auto unlisten = sum.value().listen([out] (const int& x) { out->push_back(x); });
    trans.close();
    ea.send(5);
    ea.send(7);
    ea.send(1);
    ea.send(2);
    ea.send(3);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 100, 105, 112, 113, 115, 118 }) == *out);
}

void test_sodium::accum1()
{
    stream_sink<int> ea;
    auto out = std::make_shared<vector<int>>();
    cell<int> sum = ea.accum<int>(100, [] (const int& a, const int& s) -> int {
        return a+s;
    });
    auto unlisten = sum.updates().listen([out] (const int& x) { out->push_back(x); });
    ea.send(5);
    ea.send(7);
    ea.send(1);
    ea.send(2);
    ea.send(3);
    unlisten();
    CPPUNIT_ASSERT(vector<int>({ 105, 112, 113, 115, 118 }) == *out);
}

void test_sodium::split1()
{
    stream_sink<string> ea;
    auto out = std::make_shared<vector<string>>();
    stream<string> eo = split(ea.map([] (const string& text0) -> list<string> {
        size_t p;
        string text = text0;
        list<string> tokens;
        while ((p = text.find(' ')) != string::npos) {
            tokens.push_back(text.substr(0, p));
            text = text.substr(p+1);
        }
        if (text.length() != 0)
            tokens.push_back(text);
        return tokens;
    }))
    // coalesce so we'll fail if split didn't put each string into its own transaction
    /* .coalesce([] (const string& a, const string& b) { return b; }) */;
    auto unlisten = eo.listen([out] (const string& x) { out->push_back(x); });
    ea.send("the common cormorant");
    ea.send("or shag");
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("the"), string("common"), string("cormorant"),
                                    string("or"), string("shag") }) == *out);
}

// TO DO: split2 from Haskell implementation

void test_sodium::add_cleanup1()
{
    auto out = std::make_shared<vector<string>>();
    {
        stream_sink<string> ea;
        std::function<void()> unlisten;
        {
            stream<string> eb = ea.map([] (const string& x) { return x + "!"; })
                                 .add_cleanup([out] {out->push_back("<cleanup>");});
            unlisten = eb.listen([out] (const string& x) { out->push_back(x); });
            ea.send("custard apple");
        }
        ea.send("persimmon");
        unlisten();
        out->push_back("date");
    }
    /*
    for (auto it = out->begin(); it != out->end(); ++it)
        printf("%s\n", (*it).c_str());
        */
    CPPUNIT_ASSERT(vector<string>({ string("custard apple!"), string("persimmon!"), string("<cleanup>"),
                                    string("date") }) == *out);
}

void test_sodium::add_cleanup2()
{
    auto out = std::make_shared<vector<string>>();
    {
        stream_sink<string> ea;
        std::function<void()> unlisten;
        {
            stream<string> eb = ea.filter([] (const string& x) { return x != "ignore"; })
                                 .add_cleanup([out] {out->push_back("<cleanup>");});
            unlisten = eb.listen([out] (const string& x) { out->push_back(x); });
            ea.send("custard apple");
            ea.send("ignore");
        }
        ea.send("persimmon");
        unlisten();
        out->push_back("date");
    }
    /*
    for (auto it = out->begin(); it != out->end(); ++it)
        printf("%s\n", (*it).c_str());
        */
    CPPUNIT_ASSERT(vector<string>({ string("custard apple"), string("persimmon"), string("<cleanup>"),
                                    string("date") }) == *out);
}

void test_sodium::constant_value()
{
    auto out = std::make_shared<vector<string>>();
    transaction trans; 
    cell<string> a("cheese");
    auto eValue = a.value();
    eValue.listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    CPPUNIT_ASSERT(vector<string>({ string("cheese") }) == *out);
}

void test_sodium::loop_value()
{
    auto out = std::make_shared<vector<string>>();
    transaction trans; 
    cell_loop<string> a;
    auto eValue = a.value();
    a.loop(cell<string>("cheese"));
    eValue.listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    CPPUNIT_ASSERT(vector<string>({ string("cheese") }) == *out);
}

void test_sodium::loop_value_snapshot()
{
    auto out = std::make_shared<vector<string>>();
    cell<string> a("lettuce");
    transaction trans; 
    cell_loop<string> b;
    auto eSnap = a.value().snapshot(b, [] (const string& a_, const string& b_) {
        return a_ + " " + b_;
    });
    b.loop(cell<string>("cheese"));
    auto unlisten = eSnap.listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("lettuce cheese") }) == *out);
}

void test_sodium::loop_value_hold()
{
    auto out = std::make_shared<vector<string>>();
    transaction trans; 
    cell_loop<string> a;
    cell<string> value = a.value().hold("onion");
    stream_sink<unit> eTick;
    a.loop(cell<string>("cheese"));
    trans.close();
    auto unlisten = eTick.snapshot(value).listen([out] (const string& x) { out->push_back(x); });
    eTick.send(unit());
    unlisten();
    CPPUNIT_ASSERT(vector<string>({ string("cheese") }) == *out);
}

void test_sodium::lift_loop()
{
    auto out = std::make_shared<vector<string>>();
    transaction trans;
    cell_loop<string> a;
    cell_sink<string> b("kettle");
    auto c = a.lift(b, [] (const string& a_, const string& b_) {
        return a_+" "+b_;
    });
    a.loop(cell<string>("tea"));
    auto unlisten = c.value().listen([out] (const string& x) { out->push_back(x); });
    trans.close();
    b.send("caddy");
    CPPUNIT_ASSERT(vector<string>({ string("tea kettle"), string("tea caddy") }) == *out);
}

void test_sodium::loop_switch_s()
{
    auto out = std::make_shared<vector<string>>();
    transaction trans;
    stream_sink<string> e1;
    cell_loop<stream<string>> b_lp;
    stream<string> e = switch_s(b_lp);
    e1.send("banana");
    auto unlisten = e.listen([out] (const string& x) { out->push_back(x); });
    cell_sink<stream<string>> b(e1);
    b_lp.loop(b);
    trans.close();
    stream_sink<string> e2;
    e2.send("pear");
    b.send(e2);
    e2.send("apple");
    CPPUNIT_ASSERT(vector<string>({ string("banana"), string("apple") }) == *out);
}

void test_sodium::detach_sink()
{
    // Check that holding the sink doesn't prstream a cleanup added to an stream
    // from working.
    stream_sink<int>* esnk = new stream_sink<int>;
    std::shared_ptr<bool> cleanedUp(new bool(false));
    stream<int>* e(new stream<int>(esnk->add_cleanup([cleanedUp] () {
        *cleanedUp = true;
    })));
    CPPUNIT_ASSERT(*cleanedUp == false);
    delete e;
    CPPUNIT_ASSERT(*cleanedUp == true);
    delete esnk;
}

void test_sodium::move_semantics()
{
    cell<unique_ptr<int>> pointer(unique_ptr<int>(new int(625)));
    auto value = pointer.map([&](const unique_ptr<int>& pInt) {
        return pInt ? *pInt : 0;
    });
    CPPUNIT_ASSERT(value.sample() == 625);
}

void test_sodium::move_semantics_hold()
{
    stream<unique_ptr<int>> e;
    auto b = e.hold(unique_ptr<int>(new int(345)));
    auto val = b.map([](const unique_ptr<int>& pInt) {
        return pInt ? *pInt : 0;
    });
    CPPUNIT_ASSERT(val.sample() == 345);
}

void test_sodium::lift_from_simultaneous()
{
    transaction trans;
    cell_sink<int> b1(3);
    cell_sink<int> b2(5);
    cell<int> sum = b1.lift(b2,
        [] (const int& a, const int& b) { return a + b; });
    auto out = std::make_shared<vector<int>>();
    auto kill = sum.value().listen([out] (const int& sum_) {
        out->push_back(sum_);
    });
    b2.send(7);
    trans.close();
    kill();
    CPPUNIT_ASSERT(vector<int>({ 10 }) == *out);
}

void test_sodium::stream_sink_combining()
{
    stream_sink<int> s([] (int a, int b) { return a + b; });
    auto out = std::make_shared<vector<int>>();
    auto kill = s.listen([out] (const int& a) {
        out->push_back(a);
    });
    s.send(99);
    {
        transaction trans;
        s.send(18);
        s.send(100);
        s.send(2001);
    }
    {
        transaction trans;
        s.send(5);
        s.send(10);
    }
    kill();
    CPPUNIT_ASSERT(vector<int>({ 99, 2119, 15 }) == *out);
}

void test_sodium::cant_send_in_handler()
{
    stream_sink<int> sa;
    stream_sink<int> sb;
    auto kill = sa.listen([sb] (const int& i) {
        sb.send(i);
    });
    try {
        sa.send(5);
        CPPUNIT_FAIL("exception expected");
    }
    catch (const std::runtime_error&) {
        kill();  // Pass!
    }
}

struct Packet {
    Packet(int address_, std::string payload_)
    : address(address_),
      payload(payload_)
    {
    }
    int address;
    std::string payload;
};

void test_sodium::router1()
{
    stream_sink<Packet> s;
    router<Packet, int> r(s, [] (const Packet& pkt) { return pkt.address; });

    stream<Packet> one = r.filter_equals(1);
    auto out_one = std::make_shared<vector<std::string>>();
    auto kill_one = one.listen([out_one] (const Packet& p) {
            out_one->push_back(p.payload);
        });

    stream<Packet> two = r.filter_equals(2);
    auto out_two = std::make_shared<vector<std::string>>();
    auto kill_two = two.listen([out_two] (const Packet& p) {
            out_two->push_back(p.payload);
        });

    stream<Packet> three = r.filter_equals(3);
    auto out_three = std::make_shared<vector<std::string>>();
    auto kill_three = three.listen([out_three] (const Packet& p) {
            out_three->push_back(p.payload);
        });

    s.send(Packet(1, "dog"));
    s.send(Packet(3, "manuka"));
    s.send(Packet(2, "square"));
    s.send(Packet(3, "tawa"));
    s.send(Packet(2, "circle"));
    s.send(Packet(1, "otter"));
    s.send(Packet(1, "lion"));
    s.send(Packet(2, "rectangle"));
    s.send(Packet(3, "rata"));
    s.send(Packet(4, "kauri"));

    kill_one();
    kill_two();
    kill_three();

    CPPUNIT_ASSERT(vector<string>({ "dog", "otter", "lion" }) == *out_one);
    CPPUNIT_ASSERT(vector<string>({ "square", "circle", "rectangle" }) == *out_two);
    CPPUNIT_ASSERT(vector<string>({ "manuka", "tawa", "rata" }) == *out_three);
}

void test_sodium::router2()
{
    stream_sink<Packet> s;
    router<Packet, int> r(s, [] (const Packet& pkt) { return pkt.address; });

    stream<Packet> one = r.filter_equals(1);
    auto out_one = std::make_shared<vector<std::string>>();
    auto kill_one = one.listen([out_one] (const Packet& p) {
            out_one->push_back(p.payload);
        });

    // Test filtering twice with the same value.
    stream<Packet> two = r.filter_equals(1);
    auto out_two = std::make_shared<vector<std::string>>();
    auto kill_two = two.listen([out_two] (const Packet& p) {
            out_two->push_back(p.payload);
        });

    stream<Packet> three = r.filter_equals(3);
    auto out_three = std::make_shared<vector<std::string>>();
    auto kill_three = three.listen([out_three] (const Packet& p) {
            out_three->push_back(p.payload);
        });

    s.send(Packet(1, "dog"));
    s.send(Packet(3, "manuka"));
    s.send(Packet(2, "square"));
    s.send(Packet(3, "tawa"));
    s.send(Packet(2, "circle"));
    s.send(Packet(1, "otter"));
    s.send(Packet(1, "lion"));
    s.send(Packet(2, "rectangle"));
    s.send(Packet(3, "rata"));
    s.send(Packet(4, "kauri"));

    kill_one();
    kill_two();
    kill_three();

    CPPUNIT_ASSERT(vector<string>({ "dog", "otter", "lion" }) == *out_one);
    CPPUNIT_ASSERT(vector<string>({ "dog", "otter", "lion" }) == *out_two);
    CPPUNIT_ASSERT(vector<string>({ "manuka", "tawa", "rata" }) == *out_three);
}

void test_sodium::router_loop1()
{
    router_loop<Packet, int> r;

    stream<Packet> one = r.filter_equals(1);
    auto out_one = std::make_shared<vector<std::string>>();
    auto kill_one = one.listen([out_one] (const Packet& p) {
            out_one->push_back(p.payload);
        });

    stream_sink<Packet> s;
    router<Packet, int> r0(s, [] (const Packet& pkt) { return pkt.address; });
    r.loop(r0);

    stream<Packet> two = r.filter_equals(2);
    auto out_two = std::make_shared<vector<std::string>>();
    auto kill_two = two.listen([out_two] (const Packet& p) {
            out_two->push_back(p.payload);
        });

    stream<Packet> three = r.filter_equals(3);
    auto out_three = std::make_shared<vector<std::string>>();
    auto kill_three = three.listen([out_three] (const Packet& p) {
            out_three->push_back(p.payload);
        });

    s.send(Packet(1, "dog"));
    s.send(Packet(3, "manuka"));
    s.send(Packet(2, "square"));
    s.send(Packet(3, "tawa"));
    s.send(Packet(2, "circle"));
    s.send(Packet(1, "otter"));
    s.send(Packet(1, "lion"));
    s.send(Packet(2, "rectangle"));
    s.send(Packet(3, "rata"));
    s.send(Packet(4, "kauri"));

    kill_one();
    kill_two();
    kill_three();

    CPPUNIT_ASSERT(vector<string>({ "dog", "otter", "lion" }) == *out_one);
    CPPUNIT_ASSERT(vector<string>({ "square", "circle", "rectangle" }) == *out_two);
    CPPUNIT_ASSERT(vector<string>({ "manuka", "tawa", "rata" }) == *out_three);
}

int main(int argc, char* argv[])
{
    CppUnit::TextUi::TestRunner runner;
    runner.addTest( test_sodium::suite() );
    bool success = runner.run();
    return success ? 0 : 1;
}

