package com.heima.server.servlet.merge;


import com.heima.common.IdWorker;
import com.heima.common.callback.CallbackTask;
import com.heima.rabbitmq.reqresp.RabbitMQRequest;
import com.heima.rabbitmq.reqresp.RabbitMQBatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@WebServlet(asyncSupported = true)
public class RabbitMergeRequestAsyncServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMergeRequestAsyncServlet.class);
    private static final Map<String, Callable<String>> futureMap = new HashMap<>();
    private static final IdWorker idWork = new IdWorker(1, 1, 0);

    private static final ExecutorService executorService = Executors.newFixedThreadPool(300);

    private RabbitMQRequest request;

    private RabbitMQBatchResponse response;

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) {
        String message = req.getParameter("message");
        // logger.info("异步Servlet请求，参数：{}", message);
        //开启异步,获取异步上下文
        final AsyncContext ctx = req.startAsync();
        Callable<String> callable = asyncHandelMessage(message);
        asyncHandel(ctx, callable);
        //  logger.info("异步Servlet请求完成.....");
    }


    /**
     * 异步处理
     *
     * @param ctx
     * @param callable
     */
    public void asyncHandel(AsyncContext ctx, Callable<String> callable) {
        executorService.execute(() -> {
            String result = null;
            try {
                result = callable.call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            ServletResponse response = ctx.getResponse();
            response.setContentType("text/html;charset=UTF-8");
            // logger.info("异步Servlet响应结果，result:{}", result);
            try {
                response.getWriter().write(result);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ctx.complete();
        });
    }

    public Callable<String> asyncHandelMessage(String message) {
        String messageId = String.valueOf(idWork.nextId());
        request.sendMessage(messageId, message);
        Callable<String> callable = new CallbackTask<>();
        futureMap.put(messageId, callable);
        return callable;
    }

    @Override
    public void init() {
        request = new RabbitMQRequest(((messageId, replayContent) -> {
            Callable<String> callable = futureMap.get(messageId);
            if (null != callable && callable instanceof CallbackTask) {
                ((CallbackTask<String>) callable).setResult(replayContent);
            }
        }));
        response = new RabbitMQBatchResponse();
    }
}
