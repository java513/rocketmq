package com.lh.base.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * @program: rocketmq
 * @description:
 * @author: lh
 * @date: 2021-08-13 10:19
 **/
public class Producer {
    public static void main(String[] args) throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("group1");

        producer.setNamesrvAddr("10.211.55.8:9876;10.211.55.9:9876");
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 执行本地事务
             * @param message
             * @param o
             * @return
             */
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if(StringUtils.equals("Taga",message.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if(StringUtils.equals("Tagb",message.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else if (StringUtils.equals("Tagc",message.getTags())){
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            /**
             * mq进行消息事务状态回查
             * @param messageExt
             * @return
             */
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("消息的Tag="+messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        String[] tags ={"Taga","Tagb","Tagc"};

        producer.start();
        for (int i = 0; i < 3; i++) {
            Message message = new Message("TransactionTopic", tags[i], ("hello world" + i).getBytes());
            SendResult result = producer.sendMessageInTransaction(message,null);
            SendStatus sendStatus = result.getSendStatus();
            String msgId = result.getMsgId();
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送状态："+sendStatus+" 消息ID="+msgId+"队列ID="+queueId);
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
