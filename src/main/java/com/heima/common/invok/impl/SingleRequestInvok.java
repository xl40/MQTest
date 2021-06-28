package com.heima.common.invok.impl;

import com.heima.common.RequestHandel;
import com.heima.common.callback.CallbackTask;
import com.heima.common.invok.RequestInvok;

import java.util.concurrent.Callable;

/**
 * 单请求调用
 */
public class SingleRequestInvok implements RequestInvok<String> {

    //直接进行调用
    @Override
    public Callable<String> invok() {
        long result = RequestHandel.purchase(1);
        String returnMsg = result >= 0 ? "下单成功..." : "下单失败...";
        return new CallbackTask(result);
    }
}
