package com.zhku.mh.activemq.producer;

import com.zhku.mh.activemq.consumer.TestMQConsumerQueue;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @ClassName:
 * @description
 * @author: mh
 * @create: 2019-09-20 15:18
 */
public class TestMQProducerQueue {

    public void MQProducerQueue() throws Exception{
        //1、创建工厂连接对象，需要制定ip和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://172.21.22.38:61617");

        //2、使用连接工厂创建一个连接对象
        Connection connection = connectionFactory.createConnection();

        //3、开启连接
        connection.start();

        //4、创建会话对象
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);

        //5、使用会话对象创建目标对象，包含queue和topic（一对一和一对多）
        Queue queue = session.createQueue("test-queue");

        //6、使用会话对象创建生产者对象
        MessageProducer messageProducer = session.createProducer(queue);

        //7、使用会话对象创建一个消息对象
        TextMessage textMessage = session.createTextMessage("hello!ActiveMQ");

        //8、发送消息
        messageProducer.send(textMessage);

        //9、关闭资源
        messageProducer.close();
        session.close();
        connection.close();
    }

    public void TopicProducer() throws Exception{
        //1、创建工厂连接对象，需要制定ip和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://172.21.22.38:61617");

        //2、使用连接工厂创建一个连接对象
        Connection connection = connectionFactory.createConnection();

        //3、开启连接
        connection.start();

        //4、创建会话对象
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);

        //5、使用会话对象创建目标对象，包含queue和topic（一对一和一对多）
        Topic topic = session.createTopic("test-topic");

        //6、使用会话对象创建生产者对象
        MessageProducer producer = session.createProducer(topic);

        //7、使用会话对象创建一个消息对象
        TextMessage textMessage = session.createTextMessage("hello!ActiveMQ-topic");

        //8、发送消息
        producer.send(textMessage);

        //9、关闭资源
        producer.close();
        session.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        TestMQProducerQueue producerQueue = new TestMQProducerQueue();
//        producerQueue.MQProducerQueue();
            producerQueue.TopicProducer();
    }
}
