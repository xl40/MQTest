package com.heima.rabbitmq.consume;

import com.heima.common.MQConstant;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchAckConsumer {
    public final static String EXCHANGE_NAME = "direct_exchange";//direct交换器名称
    private static AtomicInteger messageCount = new AtomicInteger(0);


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
        //在信道中设置主交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 声明一个临时队列
        String queueName = channel.queueDeclare().getQueue();
        //交换器和队列绑定
        channel.queueBind(queueName, EXCHANGE_NAME, "key1");

        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                System.out.println("主交换器 Received:" + envelope.getRoutingKey() + "===" + message + ",ThreadId:" + Thread.currentThread().getId());
                //统计消息数量 每隔100条消息批量提交一次
                if (messageCount.get() % 100 == 0) {
                    //批量提交消息
                    channel.basicAck(envelope.getDeliveryTag(), true);
                } else if (message.equals("end")) {
                    //如果消息有以end结尾的则对后面的消息进行批量提交
                    this.getChannel().basicAck(envelope.getDeliveryTag(), true);
                }
            }
        };
        //消费者在指定的对队列上消费
        channel.basicConsume(queueName, false, consumer);
    }

}