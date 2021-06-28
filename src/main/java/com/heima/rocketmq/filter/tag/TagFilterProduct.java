package com.heima.rocketmq.filter.tag;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消息过滤生产者
 */
public class TagFilterProduct {

    public static void main(String[] args) throws Exception {
        //创建一个消息生产者，并设置一个消息生产者组
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");

        //指定 NameServer 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setRetryTimesWhenSendAsyncFailed(3);
        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();
        //Tag 数组
        String[] tagArray = {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 10; i++) {
            //获取一个Tag
            String tag = tagArray[i % tagArray.length];
            //创建一条消息对象，指定其主题、标签和消息内容
            Message msg = new Message(
                    /* 消息主题名 */
                    "topicTest",
                    /* 消息标签 */
                    tag,
                    /* 消息内容 */
                    ("Hello Java demo RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            //发送消息并返回结果
            SendResult sendResult = producer.send(msg);
            System.out.println("发送queueId:[" + sendResult.getMessageQueue().getQueueId() + "],偏移量offset:[" + sendResult.getQueueOffset() + "],发送Tag:[" + tag + "],发送状态:[" + sendResult.getSendStatus() + "]");
        }

        // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
        producer.shutdown();
    }
}
