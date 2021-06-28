package com.heima.rocketmq.order;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConcurrentlyConsumer {
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        //创建一个消息消费者，并设置一个消息消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocket_test_consumer_group");
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //订阅指定 Topic 下的所有消息
        consumer.subscribe("topicTest", "*");

        //注册消费的监听 这里注意顺序消费为MessageListenerOrderly 之前并发为ConsumeConcurrentlyContext
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                //默认 list 里只有一条消息，可以通过设置参数来批量接收消息
                if (list != null) {
                    for (MessageExt ext : list) {
                        try {
                            try {
                                //模拟业务逻辑处理中...
                                TimeUnit.SECONDS.sleep(random.nextInt(10));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            //重试次数
                            int retryTimes = ext.getReconsumeTimes();
                            //获取接收到的消息
                            String message = new String(ext.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            //获取队列ID
                            int queueId = context.getMessageQueue().getQueueId();
                            //打印消息
                            System.out.println("Consumer-线程名称=[" + Thread.currentThread().getId() + "],重试次数:[" + retryTimes + "],接收queueId:[" + queueId + "],接收时间:[" + new Date().getTime() + "],消息=[" + message + "]");

                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //暂停当前队列
                int num = random.nextInt(10);
                if (num % 3 == 0) {
                    System.out.println("系统出现异常，阻塞当前队列...");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();
        System.out.println("消息消费者已启动");
    }
}
