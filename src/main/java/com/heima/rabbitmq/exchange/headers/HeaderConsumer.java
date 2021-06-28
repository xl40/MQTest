package com.heima.rabbitmq.exchange.headers;

import com.heima.common.MQConstant;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HeaderConsumer {
    public final static String EXCHANGE_NAME = "header_exchange";//header交换器名称

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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
        // 声明一个临时队列
        String queueName = channel.queueDeclare().getQueue();
        //创建头Map
        Map<String, Object> headers = new HashMap<>();
        headers.put("routingKey", "key2");
        //交换器和队列绑定
        channel.queueBind(queueName, EXCHANGE_NAME, "", headers);
        //绑定多个key
        //channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "key3");
        System.out.println("等待 message.....");
        //声明消费者
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                System.out.println("Received:" + envelope.getRoutingKey() + "========" + message);

            }
        };
        //消费者在指定的对队列上消费
        channel.basicConsume(queueName, true, consumer);
    }
}