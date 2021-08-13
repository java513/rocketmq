package com.lh.base.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @program: rocketmq
 * @description: 消费者
 * @author: lh
 * @date: 2021-08-13 01:07
 **/
public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");
        consumer.subscribe("OrderTopic", "*");
        //consumer.setMessageModel(MessageModel.BROADCASTING); //默认负载均衡
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                for (MessageExt msg : list) {
                    System.out.println(Thread.currentThread().getName()+"消费消息："+new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }
}
