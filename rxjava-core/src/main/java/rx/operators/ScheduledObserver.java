/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rx.operators;

import rx.Notification;
import rx.Observer;
import rx.Scheduler;
import rx.util.functions.Action0;

import java.util.concurrent.atomic.AtomicReference;

/* package */class ScheduledObserver<T> implements Observer<T> {
    private final Observer<T> underlying;
    private final Scheduler scheduler;

    private final Queue<Notification<T>> queue = new Queue<Notification<T>>();

    public ScheduledObserver(Observer<T> underlying, Scheduler scheduler) {
        this.underlying = underlying;
        this.scheduler = scheduler;
    }

    @Override
    public void onCompleted() {
        enqueue(new Notification<T>());
    }

    @Override
    public void onError(final Exception e) {
        enqueue(new Notification<T>(e));
    }

    @Override
    public void onNext(final T args) {
        enqueue(new Notification<T>(args));
    }

    private void enqueue(Notification<T> notification) {
        boolean enq = queue.enqueue(notification);
        if (enq) {
            scheduler.schedule(new Action0() {
                @Override
                public void call() {
                    try {
                        while (true) {
                            Notification<T> deq = queue.dequeue();

                            if (deq.isOnNext()) {
                                underlying.onNext(deq.getValue());
                            } else if (deq.isOnError()) {
                                underlying.onError(deq.getException());
                            } else if (deq.isOnCompleted()) {
                                underlying.onCompleted();
                            }
                        }
                    } catch (EmptyException e) {
                        // queue is empty, we are fine
                    }
                }
            });
        }
    }


    private static class Queue<T> {
        private static final Node EMPTY = new Node(null);

        private static class Node<T> {
            public final T value;
            public final AtomicReference<Node<T>> next;

            public Node(T value) {
                this.value = value;
                this.next = new AtomicReference<Node<T>>(EMPTY);
            }
        }

        private static final Node DUMMY = new Node(null);

        private final AtomicReference<Node<T>> head = new AtomicReference<Node<T>>(DUMMY);
        private final AtomicReference<Node<T>> tail = new AtomicReference<Node<T>>(DUMMY);

        public boolean enqueue(T value) {
            Node<T> node = new Node<T>(value);
            while (true) {
                Node<T> last = tail.get();
                Node<T> next = last.next.get();
                if (last == tail.get()) {
                    if (next == EMPTY) {
                        if (last.next.compareAndSet(next, node)) {
                            tail.compareAndSet(last, node);
                            return last == DUMMY;
                        }
                    } else {
                        tail.compareAndSet(last, next);
                    }
                }
            }
        }

        public T dequeue() throws EmptyException {
            while (true) {
                Node<T> first = head.get();
                Node<T> last = tail.get();
                Node<T> next = first.next.get();
                if (first == head.get()) {
                    if (first == last) {
                        if (next == DUMMY)
                            throw new EmptyException();
                        tail.compareAndSet(last, next);
                    } else {
                        T value = next.value;
                        if (head.compareAndSet(first, next))
                            return value;
                    }
                }
            }
        }
    }


    private static class EmptyException extends Exception {
    }
}
