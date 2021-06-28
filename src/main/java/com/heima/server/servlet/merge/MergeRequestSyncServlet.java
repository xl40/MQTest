package com.heima.server.servlet.merge;


import com.heima.common.RequestHandel;
import com.heima.common.invok.RequestInvok;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.Callable;

public class MergeRequestSyncServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(MergeRequestSyncServlet.class);

    private static final RequestInvok<String> requestInvok = RequestHandel.getSingleRequestInvok();


    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html;charset=UTF-8");
        // logger.info("同步Servlet请求开始.....");
        Callable<String> callable = requestInvok.invok();
        String result = null;
        try {
            result = callable.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        resp.getWriter().write(result);
        //logger.info("同步Servlet请求完成.....");
    }


    @Override
    public void init() throws ServletException {

    }
}
