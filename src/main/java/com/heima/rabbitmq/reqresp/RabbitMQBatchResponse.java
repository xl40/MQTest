package com.heima.rabbitmq.reqresp;

import com.heima.common.MQConstant;
import com.heima.common.RequestHandel;
import com.heima.common.invok.RequestInvok;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class RabbitMQBatchResponse {

    public final static String EXCHANGE_NAME = "REPLY_TO_EXCHANGE";//topic交换器名称

    private final static String requestQueueName = "requestQueue";


    //线程池 处理异步线程
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2 + 1);


    private static final RequestInvok<String> requestInvok = RequestHandel.getMergeRequestInvok();


    public RabbitMQBatchResponse() {
        Channel channel = null;
        try {
            channel = initConfig();
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
     * 开始响应结果
     *
     * @param channel
     * @param queueName
     */
    public void initRequestConsumer(Channel channel, String queueName) throws IOException {
        System.out.println("开始监听请求响应...");
        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                Callable<String> callable = requestInvok.invok();
                replayResponse(channel, envelope, properties, callable);
            }
        });
    }

    public void replayResponse(Channel channel, Envelope envelope, AMQP.BasicProperties properties, Callable<String> callable) {
        executorService.execute(() -> {
            String result = null;
            try {
                result = callable.call();
                channel.basicPublish("", properties.getReplyTo(), properties, result.getBytes());
                channel.basicAck(envelope.getDeliveryTag(), false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }


}
