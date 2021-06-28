package com.heima.rocketmq.consumer;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class LitePullConsumer {

    public static void main(String[] args) throws MQClientException {
        // 创建DefaultLitePullConsumer实例
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("rocket_test_consumer_group");
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //订阅指定 Topic 下的所有消息
        consumer.subscribe("topicTest", "*");
        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();

        try {
            //循环开始消费消息
            while (true) {
                //从consumer中获取消息
                List<MessageExt> list = consumer.poll();
                //消息处理
                if (list != null) {
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
            consumer.shutdown();
        }
    }
}
