package com.cyou.marketing.flume.kafka;

import com.google.common.collect.ImmutableMap;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;

/**
* Created by zhoubangtao on 6/20/14.
*/
public class KafkaSource extends AbstractSource implements EventDrivenSource, Configurable {

  /**
   * The constant logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

  /**
   * The Parameters.
   */
  private Properties parameters;
  /**
   * The Context.
   */
  private Context context;
  /**
   * The Consumer connector.
   */
  private ConsumerConnector consumerConnector;

  /**
   * Topic check Executor
   */
  private ScheduledExecutorService topicExecutorService;

  /**
   * The Executor service.
   */
  private ExecutorService executorService;

  /**
   * The Source counter.
   */
  private SourceCounter sourceCounter;

  /**
   * Zookeeper Client
   */
  private ZkClient zkClient;

  /**
   * topic list
   */
  private List<String> topics = new ArrayList<String>();

  /**
   * Thread count
   */
  private int tNumber = 0;


  /**
   * Configure void.
   *
   * @param context the context
   */
  @Override
  public void configure(Context context) {
    this.context = context;
    ImmutableMap<String, String> props = context.getParameters();

    this.parameters = new Properties();
    for (String key : props.keySet()) {
      String value = props.get(key);
      this.parameters.put(key, value);
    }

    //source monitoring count
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  /**
   * Start void.
   */
  @Override
  public synchronized void start() {

    super.start();
    sourceCounter.start();
    LOGGER.info("Kafka Source started...");

    String threadCountPerTopic = this.parameters.getProperty("threadCountPerTopic", "1");
    String coreThreadCount = this.parameters.getProperty("minThreadCount", "10");
    String maxThreadCount = this.parameters.getProperty("maxThreadCount", "1000");
    String topicCheckPeriod = this.parameters.getProperty("topicCheckPeriod", "50000");

    zkClient = new ZkClient(parameters.getProperty("zookeeper.connect","localhost:2181"),
        Integer.parseInt(parameters.getProperty("zookeeper.connection.timeout.ms","1000000")));

    topics = getAllTopics(zkClient);

    // make config object
    ConsumerConfig consumerConfig = new ConsumerConfig(this.parameters);
    consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

    for(String topic : topics) {
      topicCountMap.put(topic, new Integer(threadCountPerTopic));
    }


    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
        .createMessageStreams(topicCountMap);

    topicExecutorService = Executors.newSingleThreadScheduledExecutor();

    topicExecutorService.schedule(new TopicCheckWorker(Integer.parseInt(threadCountPerTopic), zkClient), Long.parseLong(topicCheckPeriod),TimeUnit.SECONDS);

    // now launch all the threads
    this.executorService = new ThreadPoolExecutor(Integer.parseInt(coreThreadCount),Integer.parseInt(maxThreadCount),
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());

    // now create an object to consume the messages
    for (final String topic : consumerMap.keySet()) {
      for(final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
        this.executorService.submit(new ConsumerWorker(topic, stream, tNumber, sourceCounter));
      }
      tNumber++;
    }
  }

  /**
   * Stop void.
   */
  @Override
  public synchronized void stop() {
    try {
      shutdown();
    } catch (Exception e) {
      LOGGER.error("", e);
    }
    super.stop();
    sourceCounter.stop();

    // Remove the MBean registered for Monitoring
    ObjectName objName = null;
    try {
      objName = new ObjectName("org.apache.flume.source"
          + ":type=" + getName());

      ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
    } catch (Exception ex) {
      LOGGER.error("Failed to unregister the monitored counter: " + objName, ex);
    }
  }

  /**
   * shutdown consumer threads.
   * @throws Exception the exception
   */
  private void shutdown() throws Exception {
    if (consumerConnector != null) {
      consumerConnector.shutdown();
    }

    if (topicExecutorService != null) {
      topicExecutorService.shutdown();
    }

    if (executorService != null) {
      executorService.shutdown();
    }
    topicExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  /**
   * Topic checker thread
   */
  private class TopicCheckWorker implements Runnable {

    private int threadCountPerTopic;

    private ZkClient zkClient;

    public TopicCheckWorker(int threadCountPerTopic, ZkClient zkClient) {
      this.threadCountPerTopic = threadCountPerTopic;
      this.zkClient = zkClient;
    }

    @Override
    public void run() {
      List<String> tempTopics = getAllTopics(zkClient);
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      for(String topic : tempTopics) {
        if (!topics.contains(topic)) {
          topics.add(topic);
          topicCountMap.put(topic, new Integer(threadCountPerTopic));
        }
      }
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
          .createMessageStreams(topicCountMap);

      for (final String topic : consumerMap.keySet()) {
        for(final KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
          executorService.submit(new ConsumerWorker(topic, stream, tNumber, sourceCounter));
        }
        tNumber++;
      }
    }
  }

  /**
   * Real Consumer Thread.
   */
  private class ConsumerWorker implements Runnable {

    private String topic;
    /**
     * The M _ stream.
     */
    private KafkaStream kafkaStream;
    /**
     * The M _ thread number.
     */
    private int threadNumber;

    private SourceCounter srcCount;

    /**
     * Instantiates a new Consumer test.
     *
     * @param kafkaStream the kafka stream
     * @param threadNumber the thread number
     */
    public ConsumerWorker(String topic, KafkaStream kafkaStream, int threadNumber, SourceCounter srcCount) {
      this.topic = topic;
      this.kafkaStream = kafkaStream;
      this.threadNumber = threadNumber;
      this.srcCount = srcCount;
    }

    /**
     * Run void.
     */
    public void run() {
      ConsumerIterator<byte[], byte[]> it = this.kafkaStream.iterator();
      try {
        while (it.hasNext()) {

          //get message from kafka topic
          byte [] message = it.next().message();

          LOGGER.info("Receive Message [Thread " + this.threadNumber + ": " + new String(message,"UTF-8") + "]");

          Map<String, String> headers = new HashMap<String, String>();

          headers.put("topic", topic);
          //create event
          Event event = EventBuilder.withBody(message);
          //send event to channel
          getChannelProcessor().processEvent(event);
          this.srcCount.incrementEventAcceptedCount();
        }
      } catch (Exception ex) {
        LOGGER.error("",ex);
      }
    }
  }

  /**
   * get all topics
   * @return
   */
  private List<String> getAllTopics(ZkClient zkClient) {
    Seq<String> topicSeq = ZkUtils.getAllTopics(zkClient);
    int topicSize = topicSeq.size();
    String [] topics = new String[topicSize];
    LOGGER.info("Topics", topics);
    return Arrays.asList(topics);
  }
}
