package com.heima.hello;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;

public class HelloTomcateServer {
    public static void main(String[] args) {
        Tomcat tomcat = new Tomcat();
        // 设置主机名称
        tomcat.setHostname("localhost");
        //设置访问端口号
        tomcat.setPort(8080);
        //设置路径
        tomcat.setBaseDir("D:/tmp/embedTomcat");
        String contextPath = "";

        StandardContext context = new StandardContext();
        // 设置资源路径
        context.setPath(contextPath);
        // 设置应用路径
        context.setPath(contextPath);
        context.addLifecycleListener(new Tomcat.FixContextListener());
        // 将context加入tomcat
        tomcat.getHost().addChild(context);
        // 在context中创建表示servlet的Wrapper并返回
        tomcat.addServlet(contextPath, "request", new HelloRequestServlet());
        context.addServletMappingDecoded("/request", "request");

        try {
            // 启动tomcat
            tomcat.start();
        } catch (LifecycleException e) {
            e.printStackTrace();
        }
        // 等待请求
        tomcat.getServer().await();
    }
}
