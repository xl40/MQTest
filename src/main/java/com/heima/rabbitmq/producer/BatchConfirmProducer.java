package com.heima.rabbitmq.producer;

import com.heima.common.MQConstant;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class BatchConfirmProducer {

    public final static String EXCHANGE_NAME = "direct_exchange";//direct交换器名称
    public final static Integer SEND_NUM = 10;//发送消息次数

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //创建连接工厂，连接RabbitMQ
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(MQConstant.RABBITMQ_HOST);//端口号、用户名、密码可以使用默认的
        connectionFactory.setUsername(MQConstant.RABBITMQ_USERNAME);
        connectionFactory.setPassword(MQConstant.RABBITMQ_PASSWORD);
        connectionFactory.setPort(MQConstant.RABBITMQ_PORT);
        //创建连接
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //启用发送者确认模式
        channel.confirmSelect();
        //在信道中设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //交换器和队列绑定放到消费者进行
        //自定义路由键
        String[] keys = new String[]{"key1", "key2", "key3"};
        //发送消息

        for (int i = 0; i < SEND_NUM; i++) {
            String key = keys[i % keys.length];
            String message = "hello 发送rabitmq消息" + i;
            //消息进行发送 并添加mandatory为true
            channel.basicPublish(EXCHANGE_NAME, key, false, null, message.getBytes("UTF-8"));
            //等待消息发送状态
            System.out.println("sendMessage:" + key + "===" + message);
        }
        try {
            channel.waitForConfirmsOrDie();
            System.out.println("批量确认消息成功");
        } catch (Exception e) {
            System.out.println("批量确认消息失败，errorMessage:" + e.getMessage());
        }
        //关闭信道
        channel.close();
        //关闭连接
        connection.close();
    }
}

