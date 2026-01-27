package com.bhcode.flare.flink.functions;

import lombok.Getter;
import java.io.Serializable;

/**
 * Wrapper for asynchronous lookup results to support dirty data handling.
 */
@Getter
public class AsyncResult<IN, OUT> implements Serializable {
    private final OUT data;
    private final IN origin;
    private final Throwable error;

    private AsyncResult(OUT data, IN origin, Throwable error) {
        this.data = data;
        this.origin = origin;
        this.error = error;
    }

    public static <IN, OUT> AsyncResult<IN, OUT> success(OUT data, IN origin) {
        return new AsyncResult<>(data, origin, null);
    }

    public static <IN, OUT> AsyncResult<IN, OUT> failure(Throwable error, IN origin) {
        return new AsyncResult<>(null, origin, error);
    }

    public boolean isSuccess() {
        return error == null;
    }
}
