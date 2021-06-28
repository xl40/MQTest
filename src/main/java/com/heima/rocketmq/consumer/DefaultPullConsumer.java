package com.heima.rocketmq.consumer;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultPullConsumer {

    // 记录每个队列的消费进度
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        //创建一个消息消费者，并设置一个消息消费者组
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("rocket_test_consumer_group");
        //指定 NameServer 地址
        consumer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        // 消费者对象在使用之前必须要调用 start 初始化
        consumer.start();

        // 获取Topic的所有队列
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("topicTest");

        //遍历所有队列
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    //拉取消息，arg1=消息队列，arg2=tag消息过滤，arg3=消息队列，arg4=一次最大拉去消息数量
                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(mq, "*", getMessageQueueOffset(mq), 32);
                    //从consumer中获取消息
                    List<MessageExt> list = pullResult.getMsgFoundList();

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
                    //将消息放入hash表中，存储该队列的消费进度
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:  // 找到消息，输出
                            System.out.println(pullResult.getMsgFoundList().get(0));
                            break;
                        case NO_MATCHED_MSG:  // 没有匹配tag的消息
                            System.out.println("无匹配消息");
                            break;
                        case NO_NEW_MSG:  // 该队列没有新消息，消费offset=最大offset
                            System.out.println("没有新消息");
                            break SINGLE_MQ;  // 跳出该队列遍历
                        case OFFSET_ILLEGAL:  // offset不合法
                            System.out.println("Offset不合法");
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //关闭Consumer
        consumer.shutdown();
    }

    /**
     * 从Hash表中获取当前队列的消费offset
     *
     * @param mq 消息队列
     * @return long类型 offset
     */
    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    /**
     * 将消费进度更新到Hash表
     *
     * @param mq     消息队列
     * @param offset offset
     */
    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}