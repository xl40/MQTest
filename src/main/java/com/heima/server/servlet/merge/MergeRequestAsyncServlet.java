package com.heima.server.servlet.merge;


import com.heima.common.RequestHandel;
import com.heima.common.invok.RequestInvok;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@WebServlet(asyncSupported = true)
public class MergeRequestAsyncServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(MergeRequestAsyncServlet.class);

    private static final ExecutorService executorService = Executors.newFixedThreadPool(300);

    private static final RequestInvok<String> requestInvok = RequestHandel.getMergeRequestInvok();


    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) {
        String message = req.getParameter("message");
        // logger.info("异步Servlet请求，参数：{}", message);
        //开启异步,获取异步上下文
        final AsyncContext ctx = req.startAsync();
        Callable<String> callable = requestInvok.invok();
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
}
