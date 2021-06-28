package com.heima.common.callback;

@FunctionalInterface
public interface CallbackInterface {

    public void call(String messageId, String replayContent);
}
