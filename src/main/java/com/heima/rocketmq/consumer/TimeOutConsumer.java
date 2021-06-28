package com.heima.rocketmq.consumer;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 超时异常测试
 */
public class TimeOutConsumer {
    public static void main(String[] args) throws Exception {
        //创建一个消息消费者，并设置一个消息消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocket_test_consumer_group");
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        //订阅指定 Topic 下的所有消息
        consumer.subscribe("topicTest", "*");

        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                if (list != null) {
                    for (MessageExt ext : list) {
                        //获取消息重试次数
                        int retryTimes = ext.getReconsumeTimes();
                        try {
                            String message = new String(ext.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            System.out.println("Consumer-线程名称=[" + Thread.currentThread().getId() + "],消息重试次数:[" + retryTimes + "],接收时间:[" + new Date().getTime() + "],消息=[" + message + "]");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //这里睡眠60秒
                try {
                    TimeUnit.SECONDS.sleep(60);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("休眠60秒 看还能不能走到这里...");
                //  业务方正常消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();
        System.out.println("消息消费者已启动");
    }
}
