package com.heima.rocketmq.order;

import com.heima.common.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

public class OrderProducer {
    private static final List<ProductOrder> orderList = new ArrayList<>();

    static {
        orderList.add(new ProductOrder("XXX001", "订单创建"));
        orderList.add(new ProductOrder("XXX001", "订单付款"));
        orderList.add(new ProductOrder("XXX001", "订单完成"));
        orderList.add(new ProductOrder("XXX002", "订单创建"));
        orderList.add(new ProductOrder("XXX002", "订单付款"));
        orderList.add(new ProductOrder("XXX002", "订单完成"));
        orderList.add(new ProductOrder("XXX003", "订单创建"));
        orderList.add(new ProductOrder("XXX003", "订单付款"));
        orderList.add(new ProductOrder("XXX003", "订单完成"));
    }

    public static void main(String[] args) throws Exception {
        //创建一个消息生产者，并设置一个消息生产者组
        DefaultMQProducer producer = new DefaultMQProducer("rocket_test_consumer_group");

        //指定 NameServer 地址
        producer.setNamesrvAddr(MQConstant.ROCKETMQ_NAMESERVER_ADDR);
        //初始化 Producer，整个应用生命周期内只需要初始化一次
        producer.start();

        for (int i = 0; i < orderList.size(); i++) {
            //获取当前order
            ProductOrder order = orderList.get(i);
            //创建一条消息对象，指定其主题、标签和消息内容
            Message message = new Message(
                    /* 消息主题名 */
                    "topicTest",
                    /* 消息标签 */
                    order.getOrderId(),
                    /* 消息内容 */
                    (order.toString()).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            //发送消息并返回结果 使用hash选择策略
            SendResult sendResult = producer.send(message, new SelectMessageQueueByHash(), order.getOrderId());

            System.out.println("product: 发送状态:" + sendResult.getSendStatus() + ",存储queue:" + sendResult.getMessageQueue().getQueueId() + ",orderID:" + order.getOrderId() + ",type:" + order.getType());
        }

        // 一旦生产者实例不再被使用则将其关闭，包括清理资源，关闭网络连接等
        producer.shutdown();
    }
}
