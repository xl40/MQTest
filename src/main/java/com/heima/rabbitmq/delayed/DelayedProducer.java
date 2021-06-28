package com.heima.rabbitmq.delayed;

import com.heima.common.MQConstant;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 延时消费
 */
public class DelayedProducer {
    public final static String EXCHANGE_NAME = "delayed_exchange";//延时交换器
    public final static String DIRECT_QUEUE = "DELAYED_DIRECT_QUEUE";
    public final static String DLX_EXCHANGE_NAME = "dlx_topict_exchange"; //死信交换器
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
        //在信道中设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        //创建死信交换器
        Map<String, Object> dlxMap = new HashMap<>();
        // x-dead-letter-exchange    这里声明当前队列绑定的死信交换机
        dlxMap.put("x-dead-letter-exchange", DLX_EXCHANGE_NAME);
        //死信路由键，会替换消息原来的路由键
        dlxMap.put("x-dead-letter-routing-key", "dead_key");
        // x-message-ttl  声明队列的TTL
        dlxMap.put("x-message-ttl", 1000 * 10);
        //声明队列
        channel.queueDeclare(DIRECT_QUEUE, false, false, false, dlxMap);
        //交换器和队列绑定
        channel.queueBind(DIRECT_QUEUE, EXCHANGE_NAME, "#");

        //交换器和队列绑定放到消费者进行
        //自定义路由键
        String[] keys = new String[]{"key1", "key2", "key3"};
        //发送消息
        for (int i = 0; i < SEND_NUM; i++) {
            String key = keys[i % keys.length];
            String message = "hello 发送rabitmq消息" + i;
            //消息进行发送 并添加mandatory为true
            channel.basicPublish(EXCHANGE_NAME, key, false, null, message.getBytes("UTF-8"));

            System.out.println("sendMessage:" + key + "===" + message);

        }
        //关闭信道
        channel.close();
        //关闭连接
        connection.close();
    }
}
