package com.example.gmall_api.util;

/**
 * @ClassName DateUtil
 * @Description: TODO
 * @Author zhuyuxiang
 * @Date 2020/9/18
 * @Version V1.0
 **/

public class DateUtil {
    public static Integer now() {
        long l = System.currentTimeMillis();
        return (int)l;
    }
}