package com.bhcode.flare.flink.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

/**
 * A wrapper for AsyncFunction that catches exceptions and wraps results into AsyncResult.
 */
public class AsyncDirtyDataWrapper<IN, OUT> extends RichAsyncFunction<IN, AsyncResult<IN, OUT>> {

    private final AsyncFunction<IN, OUT> inner;

    public AsyncDirtyDataWrapper(AsyncFunction<IN, OUT> inner) {
        this.inner = inner;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (inner instanceof RichAsyncFunction) {
            ((RichAsyncFunction<?, ?>) inner).open(parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (inner instanceof RichAsyncFunction) {
            ((RichAsyncFunction<?, ?>) inner).close();
        }
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<AsyncResult<IN, OUT>> resultFuture) throws Exception {
        try {
            inner.asyncInvoke(input, new ResultFuture<OUT>() {
                @Override
                public void complete(java.util.Collection<OUT> result) {
                    if (result == null || result.isEmpty()) {
                        resultFuture.complete(Collections.singleton(AsyncResult.success(null, input)));
                    } else {
                        // For simplicity, we take the first result as our wrapper is 1-to-1
                        resultFuture.complete(Collections.singleton(AsyncResult.success(result.iterator().next(), input)));
                    }
                }

                @Override
                public void completeExceptionally(Throwable error) {
                    resultFuture.complete(Collections.singleton(AsyncResult.failure(error, input)));
                }
            });
        } catch (Exception e) {
            resultFuture.complete(Collections.singleton(AsyncResult.failure(e, input)));
        }
    }
}
