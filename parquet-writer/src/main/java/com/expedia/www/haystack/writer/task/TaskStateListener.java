package com.expedia.www.haystack.writer.task;

public interface TaskStateListener {
    enum State {
        RUNNING,
        NOT_RUNNING,
        FAILED,
        CLOSED
    }

    void onChange(final State state);
}