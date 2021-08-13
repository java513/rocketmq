package com.lh.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @program: rocketmq
 * @description: 发送单向消息
 * @author: lh
 * @date: 2021-08-13 00:19
 **/
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");

        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "Tag3", ("hello world,单向消息" + i).getBytes());
            producer.sendOneway(message);
            TimeUnit.SECONDS.sleep(1);
        }
        producer.shutdown();
    }
}
