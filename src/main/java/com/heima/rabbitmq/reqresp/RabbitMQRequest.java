package com.heima.rabbitmq.reqresp;

import com.heima.common.MQConstant;
import com.heima.common.callback.CallbackInterface;
import com.heima.server.servlet.merge.MergeRequestAsyncServlet;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQRequest {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQRequest.class);

    public final static String EXCHANGE_NAME = "REPLY_TO_EXCHANGE";//topic交换器名称

    private final static String requestQueueName = "requestQueue";

    private String responseQueueName = null;

    private Channel channel = null;


    public RabbitMQRequest() {
        try {
            //初始化rabbitMQ配置获取Channel
            channel = initConfig();
            //初始化临时相应队列
            responseQueueName = initTemporaryQueue(channel);
            //初始化监听响应队列
            initResponseConsumer(channel, responseQueueName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


    public RabbitMQRequest(CallbackInterface callbackInterface) {
        try {
            channel = initConfig();
            responseQueueName = initTemporaryQueue(channel);
            initResponseConsumer(channel, responseQueueName, callbackInterface);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    private Channel initConfig() throws IOException, TimeoutException {
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
        //声明request队列
        channel.queueDeclare(requestQueueName, false, false, false, null);
        //绑定交换器
        channel.queueBind(requestQueueName, EXCHANGE_NAME, "");
        return channel;
    }

    /**
     * 创建临时响应队列
     *
     * @param channel
     * @return
     * @throws IOException
     */
    private String initTemporaryQueue(Channel channel) throws IOException {
        String responseQueueName = channel.queueDeclare().getQueue();
        return responseQueueName;
    }

    /**
     * 普通消息发送
     *
     * @param message
     */
    public void sendMessage(String message) {
        //设置消息属性，并设置replyTo属性为响应队列名称
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().replyTo(responseQueueName).build();

        try {
            channel.basicPublish(EXCHANGE_NAME, "", properties, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 带消息ID的消息发送
     *
     * @param message
     * @param messageId
     */
    public void sendMessage(String messageId, String message) {
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().messageId(messageId).replyTo(responseQueueName).build();
        try {
            channel.basicPublish(EXCHANGE_NAME, "", properties, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化监听响应队列
     *
     * @param channel
     * @param queueName
     * @throws IOException
     */
    private static void initResponseConsumer(Channel channel, String queueName) throws IOException {
        logger.info("开始监听响应队列...");
        //启动消费者监听，并设置自动ACK
        channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                logger.info("接收到回复消息：{}", msg);
            }
        });
    }

    /**
     * 初始化临时队列监听
     *
     * @param channel
     * @param queueName
     * @param callbackInterface
     * @throws IOException
     */
    private static void initResponseConsumer(Channel channel, String queueName, CallbackInterface callbackInterface) throws IOException {
        System.out.println("开始监听响应响应...");
        channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                String messageId = properties.getMessageId();
                callbackInterface.call(messageId, msg);
                System.out.println("接收到回复消息：" + msg);
            }
        });

    }

}
