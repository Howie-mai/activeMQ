package com.zhku.mh.activemq.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @ClassName:
 * @description
 * @author: mh
 * @create: 2019-09-20 15:17
 */
public class TestMQConsumerQueue {

    public void MQConsumerQueue() throws Exception{
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

        //6、创建一个接受者对象
        MessageConsumer consumer = session.createConsumer(queue);

        //7、向consumer对象中设置一个messageListener对象，用来接收消息
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                if(message instanceof TextMessage){
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        System.out.println(textMessage.getText());
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        //8、等待接收用户信息
        System.in.read();

        //9、关闭资源
        consumer.close();
        session.close();
        connection.close();
    }

    public void TopicConsumer() throws Exception{
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

        //6、创建一个接受者对象
        MessageConsumer consumer = session.createConsumer(topic);

        //7、向consumer对象中设置一个messageListener对象，用来接收消息
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                if(message instanceof TextMessage){
                    TextMessage textMessage = (TextMessage) message;
                    try {
                        System.out.println(textMessage.getText());
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        //8、等待接收用户信息
        System.in.read();

        //9、关闭资源
        consumer.close();
        session.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        TestMQConsumerQueue consumerQueue = new TestMQConsumerQueue();
//        consumerQueue.MQConsumerQueue();
        consumerQueue.TopicConsumer();
    }
}
