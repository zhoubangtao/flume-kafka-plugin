package com.cyou.marketing.flume;


import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import scala.collection.Seq;

import java.util.Properties;

/**
 * Unit test for simple App.
 */
public class AppTest {

    /**
     * Rigourous Test :-)
     */
    @Test
    public void testKafkaTopics() {
      // make config object
      Properties properties = new Properties();
      properties.put("zookeeper.connect","localhost:2181");
      properties.put("zookeeper.connection.timeout.ms","1000000");
      properties.put("group.id","test-consume-group");

      ZkClient zkClient = new ZkClient("localhost:2181",1000000);

      ConsumerConfig consumerConfig = new ConsumerConfig(properties);
      ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

      Seq<String> topicSeq = ZkUtils.getAllTopics(zkClient);
      int topicSize = topicSeq.size();
      String [] topics = new String[topicSize];
      topicSeq.copyToArray(topics);
      System.out.println(topics[1]);
    }
}
