package com.lh.base.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @program: rocketmq
 * @description:
 * @author: lh
 * @date: 2021-08-13 10:04
 **/
public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");

        producer.start();
        ArrayList<Message> messages = new ArrayList<Message>();
        Message msg1 = new Message("BatchTopic", "Tag1", ("hello world" + 1).getBytes());
        Message msg2 = new Message("BatchTopic", "Tag1", ("hello world" + 2).getBytes());
        Message msg3 = new Message("BatchTopic", "Tag1", ("hello world" + 3).getBytes());
        messages.add(msg1);
        messages.add(msg2);
        messages.add(msg3);
        SendResult result = producer.send(messages);
        SendStatus sendStatus = result.getSendStatus();
        String msgId = result.getMsgId();
        int queueId = result.getMessageQueue().getQueueId();
        System.out.println("发送状态：" + sendStatus + " 消息ID=" + msgId + "队列ID=" + queueId);
        producer.shutdown();
    }
}
