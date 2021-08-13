package com.lh.base.transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @program: rocketmq
 * @description:
 * @author: lh
 * @date: 2021-08-13 10:16
 **/
public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");
        //consumer.subscribe("FilterTagTopic", "Tag1 || Tag2");
        //consumer.subscribe("TransactionTopic", MessageSelector.byTag("*"));
        consumer.subscribe("TransactionTopic", "*");
        //consumer.setMessageModel(MessageModel.BROADCASTING); //默认负载均衡
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : list) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者启动～～");
    }
}
