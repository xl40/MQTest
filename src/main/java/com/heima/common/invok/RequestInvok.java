package com.heima.common.invok;

import java.util.concurrent.Callable;

public interface RequestInvok<T> {

    public Callable<T> invok();
}
