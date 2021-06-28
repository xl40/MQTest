package com.heima.rabbitmq.reqresp;

import com.heima.common.MQConstant;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQResponse {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQResponse.class);

    public final static String EXCHANGE_NAME = "REPLY_TO_EXCHANGE";//topic交换器名称

    private final static String requestQueueName = "requestQueue";


    public RabbitMQResponse() {
        Channel channel = null;
        try {
            //初始化RabbitMQ配置并获取Channel
            channel = initConfig();
            //初始化请求队列监听
            initRequestConsumer(channel, requestQueueName);
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
     * 监听请求队列并向响应队列发送数据
     *
     * @param channel
     * @param queueName
     */
    public void initRequestConsumer(Channel channel, String queueName) throws IOException {
        logger.info("开始监听请求队列...");
        //开始监听请求队列，并设置自动ACK
        channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                //处理消息
                handelMessage(msg);
                //使用默认交换器向响应队列发送应答消息
                channel.basicPublish("", properties.getReplyTo(), properties, "OK".getBytes());
            }
        });
    }


    private void handelMessage(String message) {
        logger.info("开始进行业务处理,message:{}", message);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("业务处理完成,message:{}", message);
    }


}
