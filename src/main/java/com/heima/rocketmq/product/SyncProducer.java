package com.heima.rocketmq.product;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");
        // 指定 name server 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        producer.setRetryTimesWhenSendFailed(2);
        producer.setRetryAnotherBrokerWhenNotStoreOK(true);
        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();
        for (int i = 0; i < 100; i++) {
            //创建一条消息对象，指定其主题、标签和消息内容
            Message msg = new Message(
                    /* 消息主题名 */
                    "topicTest",
                    /* 消息标签 */
                    "TagA",
                    /* 消息内容 */
                    ("Hello Java demo RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            //发送消息并返回结果
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
        producer.shutdown();
    }
}