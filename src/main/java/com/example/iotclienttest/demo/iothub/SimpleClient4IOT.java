/**
 * aliyun.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.example.iotclienttest.demo.iothub;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.iotclienttest.util.JsonUtil;
import com.example.iotclienttest.util.LogUtil;
import com.example.iotclienttest.util.SignUtil;
import com.flksec.keybox.msgbodymodel.business.BusinessMsg;
import com.flksec.keybox.msgbodymodel.commons.IotTopicFormatEnum;
import com.flksec.keybox.msgbodymodel.commons.MsgType;
import com.flksec.keybox.msgbodymodel.event.RegisteEvent;
import com.flksec.keybox.msgbodymodel.response.CipherSetupResp;
import com.flksec.keybox.msgbodymodel.response.KeyUpdateResp;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * IoT套件JAVA版设备接入demo
 */
public class SimpleClient4IOT {

    /******这里是客户端需要的参数*******/
    public static String deviceName = "lw1544165961000T";
    public static String productKey = "a1zQileBXVA";
    public static String secret = "pRGopFSqDqh2wceaKawioLRkDX7PnfTW";

    public static void main(String... strings) throws Exception {
        //客户端设备自己的一个标记，建议是MAC或SN，不能为空，32字符内
        String clientId = InetAddress.getLocalHost().getHostAddress();

        //设备认证
        Map<String, String> params = new HashMap<String, String>();
        //这个是对应用户在控制台注册的 设备productkey
        params.put("productKey", productKey);
        //这个是对应用户在控制台注册的 设备name
        params.put("deviceName", deviceName);
        params.put("clientId", clientId);
        String t = System.currentTimeMillis() + "";
        params.put("timestamp", t);

        //MQTT服务器地址，TLS连接使用ssl开头
        String targetServer = "ssl://" + productKey + ".iot-as-mqtt.cn-shanghai.aliyuncs.com:1883";
        //客户端ID格式，两个||之间的内容为设备端自定义的标记，字符范围[0-9][a-z][A-Z]
        String mqttclientId = clientId + "|securemode=2,signmethod=hmacsha1,timestamp=" + t + ",ext=1|";
        //mqtt用户名格式
        String mqttUsername = deviceName + "&" + productKey;
        //签名
        String mqttPassword = SignUtil.sign(params, secret, "hmacsha1");

        System.err.println("mqttclientId=" + mqttclientId);

        connectMqtt(targetServer, mqttclientId, mqttUsername, mqttPassword, deviceName);
    }

    public static void connectMqtt(String url, String clientId, String mqttUsername,
                                   String mqttPassword, final String deviceName) throws Exception {
        final ExecutorService executorService = new ThreadPoolExecutor(2,
                4, 600, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(100), new CallerRunsPolicy());

        MemoryPersistence persistence = new MemoryPersistence();
        final SSLSocketFactory socketFactory = createSSLSocket();
        final MqttClient sampleClient = new MqttClient(url, clientId, persistence);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        // MQTT 3.1.1
        connOpts.setMqttVersion(4);
        connOpts.setSocketFactory(socketFactory);
        //设置是否自动重连
        connOpts.setAutomaticReconnect(true);
        //如果是true，那么清理所有离线消息，即QoS1或者2的所有未接收内容
        connOpts.setCleanSession(false);
        connOpts.setUserName(mqttUsername);
        connOpts.setPassword(mqttPassword.toCharArray());
        connOpts.setKeepAliveInterval(65);

        LogUtil.print(clientId + "进行连接, 目的地: " + url);
        sampleClient.connect(connOpts);

        sampleClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                LogUtil.print("连接失败,原因:" + cause);
                cause.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LogUtil.print("--------------接收到消息,来自Topic [" + topic + "] , 内容是:["
                        + new String(message.getPayload(), "UTF-8") + "],  ");
                String messageId = topic.substring(topic.indexOf("request/") + 8);
                LogUtil.print("messageId: " + messageId);

                final String respTopic = "/sys/a1ePQ9JWJxV/kb-1/rrpc/response/" + messageId;
                final MqttMessage response = new MqttMessage("123test".getBytes("utf-8"));

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            System.out.println("返回响应：" + response);
                            sampleClient.publish(respTopic, response);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                //如果是QoS0的消息，token.resp是没有回复的
                LogUtil.print("消息发送成功! " + ((token == null || token.getResponse() == null) ? "null"
                        : token.getResponse().getKey()));
            }
        });
        LogUtil.print("连接成功:-------------------------");

        //这里测试发送一条消息
        BusinessMsg resp = new BusinessMsg();
        resp.setCorpId("corpId-test");
        resp.setMsgType(MsgType.REGISTE_EVENT);
        RegisteEvent registeEvent = new RegisteEvent();
        registeEvent.setNetRouterMac("routerMac");
        registeEvent.setModel(2);
        resp.setMsgBody(JsonUtil.toJson(registeEvent));

        String eventParams = getEventParams(resp);

        MqttMessage message = new MqttMessage(eventParams.getBytes("utf-8"));
        message.setQos(0);
        System.out.println("消息发布: " + eventParams);
        // /sys/{productKey}/{deviceName}/thing/event/{tsl.event.identifier}/post
        sampleClient.publish("/sys/" + productKey + "/" + deviceName + "/thing/event/NOTICEANDEVENT/post", message);


        //一次订阅永久生效 
        //这个是第一种订阅topic方式，回调到统一的callback
        //sampleClient.subscribe(subTopic);

        //这个是第二种订阅方式, 订阅某个topic，有独立的callback
        String asyncSubTopic = String.format(IotTopicFormatEnum.OBJECT_MODEL_ASYNC_TOPIC_FORMAT.client(), productKey, deviceName);
        System.out.println("async topic:" + asyncSubTopic);
        final String asyncResponseTopic = String.format(IotTopicFormatEnum.OBJECT_MODEL_ASYNC_TOPIC_FORMAT.client(), productKey, deviceName) + "_reply";
        System.out.println("async response topic:" + asyncResponseTopic);
        sampleClient.subscribe(asyncSubTopic, new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LogUtil.print("异步topic收到消息：" + message + ",topic=" + topic);
                byte[] payload = message.getPayload();
                String s = new String(payload);
                JSONObject jsonObject = JSON.parseObject(s);
                String id = jsonObject.getString("id");
                JSONObject params = jsonObject.getJSONObject("params");
                String msgType = params.getString("msgType");
                String msgBody = params.getString("msgBody");
                BusinessMsg req = JsonUtil.toObject(msgBody, BusinessMsg.class);
                BusinessMsg resp = new BusinessMsg();
                resp.setCorpId(req.getCorpId());
                resp.setRequestId(req.getMsgId());
                if (StringUtils.equals(msgType, MsgType.CIPHER_SETUP_REQ)) {
                    resp.setMsgType(MsgType.CIPHER_SETUP_RESP);
                    CipherSetupResp cipherSetupResp = new CipherSetupResp();
                    cipherSetupResp.setRegisterInfo("registerInfo");
                    cipherSetupResp.setUKeyId("testUKeyId");
                    resp.setMsgBody(JsonUtil.toJson(cipherSetupResp));
                }
                if (StringUtils.equals(msgType, MsgType.KEY_UPDATE_REQ)) {
                    resp.setMsgType(MsgType.KEY_UPDATE_RESP);
                    KeyUpdateResp keyUpdateResp = new KeyUpdateResp();
                    keyUpdateResp.setKeyversion(9);
                    keyUpdateResp.setCreateDate(System.currentTimeMillis());
                    keyUpdateResp.setMethonversion(1);
                    keyUpdateResp.setKeyValue("lallalalalalallalllalal");
                    resp.setMsgBody(JsonUtil.toJson(keyUpdateResp));
                }
                Map map = new HashMap();
                map.put("id", id);
                map.put("code", 200);
                Map result = new HashMap();
                result.put("result", "1233123123213123213213213123");
                map.put("data", result);
                String str = JsonUtil.toJson(map);
                System.out.println("返回str: " + str);
                final MqttMessage response = new MqttMessage(str.getBytes("utf-8"));
                message.setQos(0);
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sampleClient.publish(asyncResponseTopic, response);
                            System.out.println("返回响应：" + response);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        //回复RRPC响应
        String rrpcSubTopic = String.format(IotTopicFormatEnum.OBJECT_MODEL_SYNC_TOPIC_FORMAT.client(), productKey, deviceName);
        System.out.println("sync topic:" + rrpcSubTopic);
        sampleClient.subscribe(rrpcSubTopic, new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LogUtil.print("收到请求：" + message + ", topic=" + topic);
                String messageId = topic.substring(topic.indexOf("rrpc/") + 5);
                messageId = messageId.substring(0, messageId.indexOf("/"));
                LogUtil.print("messageId: " + messageId);
                byte[] payload = message.getPayload();
                String s = new String(payload);

                System.out.println("接收到消息：" + s);
                BusinessMsg req = JsonUtil.toObject(s, BusinessMsg.class);
                String msgType = req.getMsgType();
                if (StringUtils.equals(msgType, MsgType.CIPHER_SETUP_REQ)) {
                    final String respTopic = topic;

                    BusinessMsg resp = new BusinessMsg();
                    resp.setRequestId(req.getMsgId());
                    resp.setMsgType(MsgType.CIPHER_SETUP_RESP);
                    CipherSetupResp cipherSetupResp = new CipherSetupResp();

                    cipherSetupResp.setRegisterInfo("registerInfo");
                    cipherSetupResp.setUKeyId("testUKeyId");
                    resp.setMsgBody(JsonUtil.toJson(cipherSetupResp));

                    //RRPC只支持QoS0
                    final MqttMessage response = new MqttMessage(JsonUtil.toJson(resp).getBytes());
                    response.setQos(0);
                    //不能在回调线程中调用publish，会阻塞线程，所以使用线程池
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                sampleClient.publish(respTopic, response);
                                LogUtil.print("回复响应成功，topic=" + respTopic);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });

                }
            }
        });
    }

    private static SSLSocketFactory createSSLSocket() throws Exception {
        SSLContext context = SSLContext.getInstance("TLSV1.2");
        context.init(null, new TrustManager[]{new ALiyunIotX509TrustManager()}, null);
        SSLSocketFactory socketFactory = context.getSocketFactory();
        return socketFactory;
    }

    private static String getPropertyParams(BusinessMsg reqMsg) {
        String identifer = "property";
        Map map = new HashMap();
        map.put("noticeAndEvent", JsonUtil.toJson(reqMsg));


        StringBuilder param = new StringBuilder("{\"method\":\"thing.event.");
        param.append(identifer).append(".post\",\"id\":\"").append(uuid()).append("\",\"params\":")
                .append(JsonUtil.toJson(map)).append(",\"version\":\"1.0\"}");
        return param.toString();
    }

    private static String getEventParams(BusinessMsg reqMsg) {
        String identifer = "NOTICEANDEVENT";
        Map map = new HashMap();
        map.put("noticeAndEvent", JsonUtil.toJson(reqMsg));


        StringBuilder param = new StringBuilder("{\"method\":\"thing.event.");
        param.append(identifer).append(".post\",\"id\":\"").append(uuid()).append("\",\"params\":")
                .append(JsonUtil.toJson(map)).append(",\"version\":\"1.0\"}");
        return param.toString();
    }

    private static String uuid() {
        String s = UUID.randomUUID().toString();
        return s.replace("-", "");
    }
}
