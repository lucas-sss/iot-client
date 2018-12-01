package com.example.iotclienttest.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * @author liuwei liuwei@flksec.com
 * @version 1.0
 * @date 2018/7/4
 */
public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(Object obj) {

        if (obj == null) {
            return "";
        }
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
        }
        return null;
    }


    public static <T> T toObject(String json, Class<T> tClass) {
        if (StringUtils.isBlank(json) || tClass == null) {
            return null;
        }

        try {
            T t = objectMapper.readValue(json, tClass);
            return t;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
