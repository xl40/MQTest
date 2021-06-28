package com.heima.common.invok.impl;

import com.heima.common.RequestHandel;
import com.heima.common.callback.CallbackTask;
import com.heima.common.invok.RequestInvok;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 合并请求
 */
public class MergeRequestInvok implements RequestInvok<String> {
    /**
     * 创建回调操作队列
     */
    private static ArrayBlockingQueue<CallbackTask<String>> blockingQueue = new ArrayBlockingQueue<CallbackTask<String>>(10000);
    /**
     * 创建异步任务线程池
     */
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2 + 1);
    //设置如果队阻塞堆大线程耗时时间
    private static final long delayTime = 200;
    //设置最小执行批次数
    private static final int minBatchSize = 50;
    //设置最大执行批次数
    private static final int maxBatchSize = 200;

    /**
     * 构造芳芳
     *
     * @param taskSize
     */
    public MergeRequestInvok(int taskSize) {
        //初始化指定线程到线程池
        for (int i = 0; i < taskSize; i++) {
            //异步执行任务
            executorService.execute(() -> {
                asyncHandel();
            });
        }

    }

    /**
     * 浏览器调用请求
     *
     * @return
     */
    @Override
    public Callable<String> invok() {
        //创建回调对象
        CallbackTask<String> callbackTask = new CallbackTask<String>();
        try {
            //加入队列缓冲
            blockingQueue.put(callbackTask);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //以callable方式返回
        return callbackTask;
    }

    /**
     * 异步线程处理数据
     */
    public void asyncHandel() {
        //设置CallBack缓冲区
        List<CallbackTask<String>> cacheCallbackList = new LinkedList<>();
        //当前时间
        long currentTime = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            try {
                //获取一个回调对象
                CallbackTask<String> callbackTask = blockingQueue.poll(delayTime, TimeUnit.MILLISECONDS);
                if (null != callbackTask) {
                    //添加到缓存
                    cacheCallbackList.add(callbackTask);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //list为空 继续循环
            if (cacheCallbackList.isEmpty()) {
                continue;
            }
            //获取当前调用耗时
            long duration = (System.currentTimeMillis() - currentTime);

            //如果缓冲区lsit 小于最小批次数 并且没有 没有达到延时时间 继续循环
            if (cacheCallbackList.size() < minBatchSize && duration < delayTime) {
                continue;
            }
            //如果 list缓存数量>最大皮次数 或者 耗时超过200Ms 进行一次批量提交
            if (cacheCallbackList.size() >= maxBatchSize || duration >= delayTime) {
                //进行批量扣减
                long result = RequestHandel.purchase(cacheCallbackList.size());
                //设置回调值 并唤醒 响应线程
                setBatchResult(cacheCallbackList, result);
                //清空list缓存
                cacheCallbackList.clear();
                //重置基准时间
                currentTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * 批量设置结果值 并唤醒响应线程
     *
     * @param callbackTaskList
     * @param result
     */
    public void setBatchResult(List<CallbackTask<String>> callbackTaskList, long result) {
        //计算扣减成功的数量
        long successSize = result >= 0 ? callbackTaskList.size() : callbackTaskList.size() + result;
        for (int i = 0; i < callbackTaskList.size(); i++) {
            CallbackTask<String> callbackTask = callbackTaskList.get(i);
            if (i <= successSize) {
                callbackTask.setResult("下单成功...");
            } else {
                callbackTask.setResult("下单失败...");
            }
        }
    }
}
