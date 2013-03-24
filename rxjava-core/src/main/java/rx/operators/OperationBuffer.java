package rx.operators;

import org.junit.Test;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.util.functions.Func1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public final class OperationBuffer {

    public static <T> Func1<Observer<List<T>>, Subscription> buffer(Observable<T> source, int count) {
        return buffer(source, count, 0);
    }

    public static <T> Func1<Observer<List<T>>, Subscription> buffer(Observable<T> source, int count, int skip) {
        if (count <= 0) {
            throw new IllegalArgumentException("count should be > 0");
        }
        return new CountingBufferedObservable<T>(source, count, skip);
    }

    private static class CountingBufferedObservable<T> implements Func1<Observer<List<T>>, Subscription> {
        private final Observable<T> source;
        private final int emitStart;
        private final int emitEnd;

        private final AtomicInteger counter = new AtomicInteger(0);
        private final ConcurrentLinkedQueue<T> buf = new ConcurrentLinkedQueue<T>();

        private CountingBufferedObservable(Observable<T> source, int count, int skip) {
            this.source = source;
            this.emitStart = skip;
            this.emitEnd = skip + count;
        }

        @Override
        public Subscription call(final Observer<List<T>> observer) {
            return source.subscribe(new Observer<T>() {
                @Override
                public void onCompleted() {
                    List<T> chunk = getChunk();

                    if (!chunk.isEmpty()) {
                        observer.onNext(chunk);
                    }

                    observer.onCompleted();
                }

                @Override
                public void onError(Exception e) {
                    observer.onError(e);
                }

                @Override
                public void onNext(T args) {

                    int i = counter.incrementAndGet();
                    boolean skipItem = (i + emitStart) % emitEnd <= emitStart;
                    boolean emitItem = (i + emitStart) % emitEnd == 0;

                    if (!skipItem || emitItem) {
                        buf.add(args);
                    }

                    if (emitItem) {
                        List<T> chunk = getChunk();
                        observer.onNext(chunk);
                    }

                }

                private List<T> getChunk() {
                    List<T> result = new ArrayList<T>();

                    int i = 0;
                    int size = Math.min(buf.size(), emitEnd);
                    while (i < size) {
                        result.add(buf.poll());
                        i++;
                    }

                    return result;
                }
            });
        }
    }

    public static class UnitTest {

        @Test
        public void testEmpty() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>empty(), 10));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

        @Test
        public void testExactCount() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>from("one", "two", "three"), 3));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onNext(Arrays.asList("one", "two", "three"));
            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

        @Test
        public void testEqualChunks() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>from("one", "two", "three", "four"), 2));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onNext(Arrays.asList("one", "two"));
            verify(obs, times(1)).onNext(Arrays.asList("three", "four"));
            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

        @Test
        public void testUnequalChunks() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>from("one", "two", "three", "four"), 3));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onNext(Arrays.asList("one", "two", "three"));
            verify(obs, times(1)).onNext(Arrays.asList("four"));
            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

        @Test
        public void testOneSmallChunk() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>from("one", "two"), 1000));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onNext(Arrays.asList("one", "two"));
            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

        @Test
        public void testSkip() {
            Observable<List<String>> observable = Observable.create(buffer(Observable.<String>from("one", "two", "three", "four", "five"), 1, 2));

            Observer<List<String>> obs = mock(Observer.class);

            observable.subscribe(obs);

            verify(obs, times(1)).onNext(Arrays.asList("one"));
            verify(obs, times(1)).onNext(Arrays.asList("four"));
            verify(obs, times(1)).onCompleted();
            verifyNoMoreInteractions(obs);

        }

    }
}
