package com.heima.rocketmq.consumer;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BatchPullConsumer {
    public static void main(String[] args) throws Exception {
        //创建一个消息消费者，并设置一个消息消费者组
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("rocket_test_consumer_group");
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //设置每批次消费10条消息
        consumer.setPullBatchSize(10);

        //订阅指定 Topic 下的所有消息
        consumer.subscribe("topicTest", "*");
        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();
        System.out.println("消息消费者已启动");
        try {
            while (true) {
                List<MessageExt> list = consumer.poll();
                //默认 list 里只有一条消息，可以通过设置参数来批量接收消息
                if (list != null) {
                    System.out.println("消费一批消息，消息长度：" + list.size());
                    for (MessageExt ext : list) {
                        try {
                            System.out.println(new String(ext.getBody(), RemotingHelper.DEFAULT_CHARSET));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //提交偏移量
                consumer.commitSync();
            }
        } finally {
            // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
            consumer.shutdown();
        }
    }
}
