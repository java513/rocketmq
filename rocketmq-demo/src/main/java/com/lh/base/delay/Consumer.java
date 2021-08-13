package com.lh.base.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @program: rocketmq
 * @description:
 * @author: lh
 * @date: 2021-08-13 01:18
 **/
public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");
        consumer.subscribe("DelayTopic", "*");
        //consumer.setMessageModel(MessageModel.BROADCASTING); //默认负载均衡
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println("消息ID="+msg.getMsgId()+",延迟时间="+(System.currentTimeMillis()-msg.getStoreTimestamp()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
