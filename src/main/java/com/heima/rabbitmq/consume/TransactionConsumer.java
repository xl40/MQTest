package com.heima.rabbitmq.consume;

import com.heima.common.MQConstant;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TransactionConsumer {
    public final static String EXCHANGE_NAME = "direct_exchange";//direct交换器名称
    public final static String DIRECT_QUEUE = "DIRECT_QUEUE";

    public static void main(String[] args) throws IOException, TimeoutException {
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
        //在信道中设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //声明队列
        channel.queueDeclare(DIRECT_QUEUE, false, false, false, null);

        //交换器和队列绑定
        channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "key1");
        channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "key2");
        channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "key3");
        //开始事务
        channel.txSelect();
        //绑定多个key
        //channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "key3");
        System.out.println("等待 message.....");
        //声明消费者
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                try {
                    System.out.println("Received:" + envelope.getRoutingKey() + "========" + message);

                    //手动确认消息 无效必须提交事务
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    //模拟异常
                    if (true) {
                        throw new RuntimeException("消费异常");
                    }

                    //提交事务
                    channel.txCommit();
                } catch (Exception e) {
                    //不进行手动确认
                    //channel.basicReject(envelope.getDeliveryTag(), false);
                    System.out.println("出现消费异常");
                    //回滚事务
                    channel.txRollback();
                }

            }
        };
        //消费者在指定的对队列上消费
        channel.basicConsume(DIRECT_QUEUE, false, consumer);
    }
}
