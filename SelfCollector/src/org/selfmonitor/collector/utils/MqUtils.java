/*
 *  This file is part of SelfMonitor
 *
 *  Copyright (c) 2018 Andrew Di <anonymous-oss@outlook.com>
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.selfmonitor.collector.utils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class MqUtils {
    private final static Logger log = LoggerFactory.getLogger(MqUtils.class.getName());
    static DefaultMQProducer defaultMQProducer;
    static DefaultMQPushConsumer defaultMQPushConsumer;

    static {
        defaultMQProducer=new DefaultMQProducer("CollectorGroup");
        defaultMQProducer.setNamesrvAddr("localhost:9876");

        defaultMQPushConsumer=new DefaultMQPushConsumer("CollectorGroup");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        try {
            defaultMQProducer.start();
            //defaultMQPushConsumer.subscribe("collector","command");
        }catch (MQClientException e) {
            log.error(e.getErrorMessage());
        }

    }
    public MqUtils() throws MQClientException {

    }

    public boolean mqPut(String msg, String topic, String tag) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message=new Message(topic,tag,msg.getBytes(Charset.forName("UTF-8")));
        SendResult sendResult=defaultMQProducer.send(message);
        if(sendResult.getSendStatus()==SendStatus.SEND_OK) {
            return true;
        }
        return false;

        //todo modify to use rocketMQ Batch Put
    }

    public void shutdown(){
        defaultMQProducer.shutdown();
    }
}
