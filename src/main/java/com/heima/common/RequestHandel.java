package com.heima.common;

import com.heima.common.invok.RequestInvok;
import com.heima.common.invok.impl.MergeRequestInvok;
import com.heima.common.invok.impl.SingleRequestInvok;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RequestHandel {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandel.class);

    private static final AtomicBoolean atomicBoolean = new AtomicBoolean(true);

    private static final AtomicLong atomicLong = new AtomicLong(10000000);

    /**
     * 具体计算方法
     *
     * @param num
     * @return
     */
    public static long purchase(long num) {
        logger.info("开始进行下单操作,数量:{}", num);
        //如果本地缓存状态是失败 直接返回
        if (!atomicBoolean.get()) {
            return -100000;
        }
        try {
            //模拟接口调用耗时
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long result = atomicLong.addAndGet(-num);
        //如果全部扣减完成 设置本地缓存为 false
        if (result <= 0) {
            atomicBoolean.set(false);
        }
        logger.info("完成进行下单操作,剩余数量:{}", result);
        return result;
    }


    public static RequestInvok getSingleRequestInvok() {
        return new SingleRequestInvok();
    }

    public static RequestInvok getMergeRequestInvok() {
        RequestInvok mergeRequestInvok = new MergeRequestInvok(10);
        return mergeRequestInvok;
    }

}
