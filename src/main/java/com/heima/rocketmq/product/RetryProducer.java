package com.heima.rocketmq.product;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class RetryProducer {
    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //创建一个消息生产者，并设置一个消息生产者组
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");

        //指定 NameServer 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置重试次数(默认2次）
        producer.setRetryTimesWhenSendFailed(1000);
        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();
        Message msg = new Message(
                /* 消息主题名 */
                "topicTest",
                /* 消息标签 */
                "TagA",
                /* 消息内容 */
                ("Hello Java demo RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送消息并返回结果,设置超时时间 5ms 所以每次都会发送失败
        SendResult sendResult = producer.send(msg, 5);

        System.out.printf("%s%n", sendResult);
        // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
        producer.shutdown();
    }
}
