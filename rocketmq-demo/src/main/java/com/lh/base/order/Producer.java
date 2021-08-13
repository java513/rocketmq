package com.lh.base.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @program: rocketmq
 * @description: 发送者
 * @author: lh
 * @date: 2021-08-13 00:54
 **/
public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");
        producer.start();
        List<OrderStep> orderSteps = OrderStep.buildOrders();
        for (int i = 0; i < orderSteps.size(); i++) {
            String body = orderSteps.get(i)+"";
            final Message msg = new Message("OrderTopic", "Order", "i" + i, body.getBytes());
            SendResult result = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Long orderId = (Long) o;
                    long index = orderId % list.size();
                    return list.get((int) index);
                }
            }, orderSteps.get(i).getOrderId());

            System.out.println("发送结果="+result);
        }
    }
}
