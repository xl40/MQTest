package com.heima.rocketmq.product;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");
        // 指定 name server 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();
        try {
            for (int i = 0; i < 100; i++) {
                //创建一条消息对象，指定其主题、标签和消息内容
                Message msg = new Message(
                        /* 消息主题名 */
                        "topicTest",
                        /* 消息标签 */
                        "TagA",
                        /* 消息内容 */
                        ("Hello Java demo RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );

                //发送消息不关心返回
                producer.sendOneway(msg);
                System.out.println("发送消息" + i);
            }
        } finally {
            //关闭
            producer.shutdown();
        }
    }
}