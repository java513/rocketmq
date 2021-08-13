package com.lh.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @program: rocketmq
 * @description: 生产者
 * @author: lh
 * @date: 2021-08-13 00:04
 **/
public class SyncProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");

        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "Tag1", ("hello world" + i).getBytes());
            SendResult result = producer.send(message);
            SendStatus sendStatus = result.getSendStatus();
            String msgId = result.getMsgId();
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送状态："+sendStatus+" 消息ID="+msgId+"队列ID="+queueId);
            TimeUnit.SECONDS.sleep(1);
        }
        producer.shutdown();
    }
}
