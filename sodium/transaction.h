/**
 * Copyright (c) 2012-2014, Stephen Blackheath and Anthony Jones
 * Released under a BSD3 licence.
 *
 * C++ implementation courtesy of International Telematics Ltd.
 */
#ifndef _SODIUM_TRANSACTION_H_
#define _SODIUM_TRANSACTION_H_

#include <sodium/config.h>
#include <sodium/count_set.h>
#include <sodium/light_ptr.h>
#include <sodium/lock_pool.h>
#include <boost/optional.hpp>
#include <boost/intrusive_ptr.hpp>
#include <sodium/unit.h>
#include <map>
#include <set>
#include <list>
#include <memory>
#include <mutex>
#include <forward_list>
#include <tuple>

namespace sodium {

    class transaction_impl;

    struct partition {
        partition();
        ~partition();
#if !defined(SODIUM_SINGLE_THREADED)
        std::recursive_mutex mx;
#endif
        int depth;

        bool processing_post;
        std::list<std::function<void()>> postQ;
        void post(std::function<void()> action);
        void process_post();
        std::list<std::function<void()>> on_start_hooks;
        bool processing_on_start_hooks;
        void on_start(std::function<void()> action);
        bool shutting_down;
    };

    namespace impl {
        struct transaction_impl;

        typedef unsigned long rank_t;
        #define SODIUM_IMPL_RANK_T_MAX ULONG_MAX

        class holder;

        class node;
        template <typename Allocator>
        struct listen_impl_func {
            typedef std::function<std::function<void()>*(
                transaction_impl*,
                const std::shared_ptr<impl::node>&,
                const SODIUM_SHARED_PTR<holder>&,
                bool)> closure;
            listen_impl_func(closure* func_)
                : func(func_) {}
            ~listen_impl_func()
            {
                assert(cleanups.begin() == cleanups.end() && func == NULL);
            }
            count_set counts;
            closure* func;
            SODIUM_FORWARD_LIST<std::function<void()>*> cleanups;
            inline void update_and_unlock(spin_lock* l) {
                if (func && !counts.active()) {
                    counts.inc_strong();
                    l->unlock();
                    for (auto it = cleanups.begin(); it != cleanups.end(); ++it) {
                        (**it)();
                        delete *it;
                    }
                    cleanups.clear();
                    delete func;
                    func = NULL;
                    l->lock();
                    counts.dec_strong();
                }
                if (!counts.alive()) {
                    l->unlock();
                    delete this;
                }
                else
                    l->unlock();
            }
        };

        class holder {
            public:
                holder(
                    std::function<void(const std::shared_ptr<impl::node>&, transaction_impl*, const light_ptr&)>* handler_
                ) : handler(handler_) {}
                ~holder() {
                    delete handler;
                }
                void handle(const SODIUM_SHARED_PTR<node>& target, transaction_impl* trans, const light_ptr& value) const;

            private:
                std::function<void(const std::shared_ptr<impl::node>&, transaction_impl*, const light_ptr&)>* handler;
        };

        struct H_STREAM {};
        struct H_STRONG {};
        struct H_NODE {};

        void intrusive_ptr_add_ref(sodium::impl::listen_impl_func<sodium::impl::H_STREAM>* p);
        void intrusive_ptr_release(sodium::impl::listen_impl_func<sodium::impl::H_STREAM>* p);
        void intrusive_ptr_add_ref(sodium::impl::listen_impl_func<sodium::impl::H_STRONG>* p);
        void intrusive_ptr_release(sodium::impl::listen_impl_func<sodium::impl::H_STRONG>* p);
        void intrusive_ptr_add_ref(sodium::impl::listen_impl_func<sodium::impl::H_NODE>* p);
        void intrusive_ptr_release(sodium::impl::listen_impl_func<sodium::impl::H_NODE>* p);

        inline bool alive(const boost::intrusive_ptr<listen_impl_func<H_STRONG> >& li) {
            return li && li->func != NULL;
        }

        inline bool alive(const boost::intrusive_ptr<listen_impl_func<H_STREAM> >& li) {
            return li && li->func != NULL;
        }

        class node
        {
            public:
                struct target {
                    target(
                        void* h_,
                        const SODIUM_SHARED_PTR<node>& n_
                    ) : h(h_),
                        n(n_) {}

                    void* h;
                    SODIUM_SHARED_PTR<node> n;
                };

            public:
                node();
                node(rank_t rank);
                ~node();

                rank_t rank;
                SODIUM_FORWARD_LIST<node::target> targets;
                SODIUM_FORWARD_LIST<light_ptr> firings;
                SODIUM_FORWARD_LIST<boost::intrusive_ptr<listen_impl_func<H_STREAM> > > sources;
                boost::intrusive_ptr<listen_impl_func<H_NODE> > listen_impl;

                bool link(void* holder, const SODIUM_SHARED_PTR<node>& target);
                void unlink(void* holder);
                void unlink_by_target(const SODIUM_SHARED_PTR<node>& target);

            private:
                bool ensure_bigger_than(std::set<node*>& visited, rank_t limit);
        };
    }
}

namespace sodium {
    namespace impl {

        template <typename A>
        struct ordered_value {
            ordered_value() : tid(-1) {}
            long long tid;
            boost::optional<A> oa;
        };

        struct entryID {
            entryID() : id(0) {}
            entryID(rank_t id_) : id(id_) {}
            rank_t id;
            entryID succ() const { return entryID(id+1); }
            inline bool operator < (const entryID& other) const { return id < other.id; }
        };

        rank_t rankOf(const SODIUM_SHARED_PTR<node>& target);

        struct prioritized_entry {
            prioritized_entry(SODIUM_SHARED_PTR<node> target_,
                              std::function<void(transaction_impl*)> action_)
                : target(std::move(target_)), action(std::move(action_))
            {
            }
            SODIUM_SHARED_PTR<node> target;
            std::function<void(transaction_impl*)> action;
        };

        struct transaction_impl {
            transaction_impl();
            ~transaction_impl();
            static partition* part;
            entryID next_entry_id;
            std::map<entryID, prioritized_entry> entries;
            std::multiset<std::pair<rank_t, entryID>> prioritizedQ;
            std::list<std::function<void()>> lastQ;
            bool to_regen;
            int inCallback;

            void prioritized(SODIUM_SHARED_PTR<impl::node> target,
                             std::function<void(impl::transaction_impl*)> action);
            void last(const std::function<void()>& action);

            void check_regen();
            void process_transactional();
        };

        class transaction_ {
        private:
            transaction_impl* impl_;
            transaction_(const transaction_&) {}
            transaction_& operator = (const transaction_&) { return *this; };
        public:
            transaction_();
            ~transaction_();
            impl::transaction_impl* impl() const { return impl_; }
        protected:
            void close();
            static transaction_impl* current_transaction();
        };
    };

    class transaction : public impl::transaction_
    {
        private:
            // Disallow copying
            transaction(const transaction&) {}
            // Disallow copying
            transaction& operator = (const transaction&) { return *this; };
        public:
            transaction() {}
            /*!
             * The destructor will close the transaction, so normally close() isn't needed.
             * But, in some cases you might want to close it earlier, and close() will do this for you.
             */
            inline void close() { impl::transaction_::close(); }

            void prioritized(SODIUM_SHARED_PTR<impl::node> target,
                             std::function<void(impl::transaction_impl*)> action)
            {
                impl()->prioritized(std::move(target), std::move(action));
            }

            void post(std::function<void()> f) {
                impl()->part->post(std::move(f));
            }

            static void on_start(std::function<void()> f) {
                transaction trans;
                trans.impl()->part->on_start(std::move(f));
            }
    };
}  // end namespace sodium

#endif
