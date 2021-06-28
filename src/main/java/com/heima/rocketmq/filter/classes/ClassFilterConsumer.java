package com.heima.rocketmq.filter.classes;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

/**
 * 消息过滤 消费者代码
 */
public class ClassFilterConsumer {

    public static void main(String[] args) throws Exception {
        //创建一个消息消费者，并设置一个消息消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(null, "rocket_test_consumer_group_c", null, new AllocateMessageQueueConsistentHash());
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //设置 Consumer 第一次启动时从队列头部开始消费还是队列尾部开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // 使用Java代码，在服务器做消息过滤
        String filterCode = MixAll.file2String("D:/workspace/MQTest/src/main/java/com/heima/rocketmq/filter/classes/CustomMessageFilter.java");

        //订阅指定 Topic 下 index 属性 >=5 的消息
        consumer.subscribe("topicTest",filterCode, filterCode);
        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                //默认 list 里只有一条消息，可以通过设置参数来批量接收消息
                if (list != null) {
                    for (MessageExt ext : list) {
                        try {
                            String message = new String(ext.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            String index = ext.getUserProperty("index");
                            //打印消息
                            System.out.println("接收queueId:[" + ext.getQueueId() + "],偏移量offset:[" + ext.getQueueOffset() + "],接收时间:[" + new Date().getTime() + "],消息=[" + message + "],index=[" + index + "]");

                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //出现异常会回到队列重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();
        System.out.println("消息消费者已启动");
    }

}
