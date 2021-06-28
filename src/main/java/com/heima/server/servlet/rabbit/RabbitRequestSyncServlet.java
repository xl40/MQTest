package com.heima.server.servlet.rabbit;

import com.heima.rabbitmq.reqresp.RabbitMQRequest;
import com.heima.rabbitmq.reqresp.RabbitMQResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RabbitRequestSyncServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(RabbitRequestSyncServlet.class);


    private RabbitMQRequest request;

    private RabbitMQResponse response;

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html;charset=UTF-8");
        String message = req.getParameter("message");
       // logger.info("同步Servlet请求，参数：{}", message);
        if (StringUtils.isEmpty(message)) {
            resp.getWriter().write("消息不能为空");
            return;
        }
        syncHandelMessage(message);
        resp.getWriter().write("发送消息成功,响应结果");
        //logger.info("异步Servlet请求完成.....");
    }

    public String syncHandelMessage(String message) {
        request.sendMessage(message);
        return null;
    }


    @Override
    public void init() throws ServletException {
        request = new RabbitMQRequest();
        response = new RabbitMQResponse();
    }
}
