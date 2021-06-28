package com.heima.rabbitmq.reqresp;

public class RabbitMQReqRespTest {
    public static void main(String[] args) throws InterruptedException {
        //创建请求客户端
        RabbitMQRequest request = new RabbitMQRequest();
        //创建响应服务器
        RabbitMQResponse response = new RabbitMQResponse();
        //发送请求消息
        request.sendMessage("xxxxxxxxx");
        //休眠等待响应
        Thread.sleep(10000);
    }
}
