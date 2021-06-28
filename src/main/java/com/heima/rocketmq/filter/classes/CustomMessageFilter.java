package com.heima.rocketmq.filter.classes;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.FilterContext;
import org.apache.rocketmq.common.filter.MessageFilter;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 自定义消息过滤器
 */
public class CustomMessageFilter implements MessageFilter {
    /**
     * 需要实现的消息过滤方法
     *
     * @param messageExt
     * @param filterContext
     * @return
     */
    @Override
    public boolean match(MessageExt messageExt, FilterContext filterContext) {
        String indexStr = messageExt.getUserProperty("index");
        String tags = messageExt.getTags();
        if (StringUtils.isNoneEmpty(indexStr, tags)) {
            Integer index = Integer.parseInt(indexStr);
            //筛选出来 index <=5 并且Tag包含TagA的数据
            if (index >= 5 && tags.contains("TagA")) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        String filterCode = MixAll.file2String("D:/workspace/MQTest/src/main/java/com/heima/rocketmq/filter/classes/CustomMessageFilter.java");

        String pattern = "^package ([a-z]{1,}.){1,}";

        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(filterCode);
        System.out.println(m.replaceAll(""));
        //System.out.println(filterCode);
    }
}
