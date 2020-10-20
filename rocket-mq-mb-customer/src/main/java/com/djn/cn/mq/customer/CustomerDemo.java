package com.djn.cn.mq.customer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
/**
 * @author op.nie-dongjia  
 */
public class CustomerDemo {
	 public static void main(String[] args) throws InterruptedException, MQClientException {
	        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rmq-group");
	        consumer.setNamesrvAddr("127.0.0.1:9876");
	        System.out.println("开始接受数据");
	        try {
	            // 设置topic和标签
	            consumer.subscribe("test1", "TagA");
	            consumer.setVipChannelEnabled(false);
	            // 程序第一次启动从消息队列头取数据
	            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
	            consumer.registerMessageListener(new MessageListenerConcurrently() {
	                @Override
					public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
																	ConsumeConcurrentlyContext Context) {
	                    Message msg = list.get(0);
	                    System.out.println("收到数据：" + new String(msg.getBody()));
	                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	                }
	            });
	            consumer.start();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }

}
