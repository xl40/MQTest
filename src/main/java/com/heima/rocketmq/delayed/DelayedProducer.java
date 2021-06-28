package com.heima.rocketmq.delayed;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 延时消息发送
 */
public class DelayedProducer {

    public static void main(String[] args) throws Exception {
        //创建一个消息生产者，并设置一个消息生产者组
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");

        //指定 NameServer 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);

        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();

        //创建一条消息对象，指定其主题、标签和消息内容
        Message msg = new Message(
                /* 消息主题名 */
                "topicTest",
                /* 消息标签 */
                "TagA",
                /* 消息内容 */
                ("发送延时消息，消息发送时间" + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        //设置延迟level为5，对应延迟1分钟
        msg.setDelayTimeLevel(5);
        //发送消息并返回结果
        SendResult sendResult = producer.send(msg);

        System.out.printf("%s%n", sendResult);

        // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
        producer.shutdown();
    }
}
