package storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.google.common.collect.ImmutableList;
import kafka.admin.CreateTopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.*;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class AtMostOnceTest {

    private class zkServer implements Runnable {
        @Override
        public void run() {
            ServerConfig zkConfig = new ServerConfig();
            zkConfig.parse(new String[]{"2181", "/tmp/zookeeper/"});
            ZooKeeperServerMain zookeeper = new ZooKeeperServerMain();
            try {
                zookeeper.runFromConfig(zkConfig);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Test
    public void kafkaTest() throws InterruptedException, UnsupportedEncodingException {

        final String kafkaTopic = "kafkaTopic" + System.currentTimeMillis();

        Thread zkThread = new Thread(new zkServer());
        zkThread.start();

        Properties p = new Properties();
        p.setProperty("zookeeper.connect", "localhost:2181");
        p.setProperty("broker.id", "0");
        p.setProperty("port", "" + 9092);
        KafkaServerStartable server = new KafkaServerStartable(new kafka.server.KafkaConfig(p));
        server.startup();

        String [] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "localhost:2181";
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "1";
        arguments[6] = "--topic";
        arguments[7] = kafkaTopic;
        CreateTopicCommand.main(arguments);

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts("localhost:2181"), kafkaTopic, "/kafkastorm", "kafka_spout_id");
        spoutConfig.atMostOnce = true;
        spoutConfig.forceFromStart = true;
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        Map conf = new HashMap();
        conf.put("storm.zookeeper.port", 2181);

        conf.put("transactional.zookeeper.port", null);
        conf.put("storm.zookeeper.servers", ImmutableList.of("localhost"));
        conf.put("storm.zookeeper.session.timeout", 20000);
        conf.put("storm.zookeeper.retry.interval", 1000);
        conf.put("storm.zookeeper.retry.times", 5);
        conf.put("topology.name", "testTopology");

        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("kafkaTestSpout");
        when(context.getComponentTasks("kafkaTestSpout")).thenReturn(ImmutableList.of(3));
        when(context.getThisTaskIndex()).thenReturn(0);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        spout.open(conf, context, collector);

        spout.nextTuple();
        verify(collector, never()).emit(anyList(), any());

        KeyedMessage<String, String> msg = new KeyedMessage<String, String>(kafkaTopic, "testMessageOne");
        producer.send(msg);
        spout.nextTuple();

        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        verify(collector, times(1)).emit(argument.capture(), any());
        assertEquals("testMessageOne", new String((byte[]) argument.getValue().get(0), "utf-8"));

        spout.nextTuple();
        verify(collector, times(1)).emit(anyList(), any());

        msg = new KeyedMessage<String, String>(kafkaTopic, "testMessageTwo");
        producer.send(msg);
        spout.nextTuple();
        ArgumentCaptor<Object> msgIdCaptor = ArgumentCaptor.forClass(Object.class);
        verify(collector, times(2)).emit(argument.capture(), msgIdCaptor.capture());
        assertEquals("testMessageTwo", new String((byte[]) argument.getValue().get(0), "utf-8"));
        spout.fail(msgIdCaptor.getValue());
        spout.nextTuple();
        verify(collector, times(2)).emit(anyList(), any());

        System.out.println("close");
        spout.close();
        spoutConfig.forceFromStart = false;
        spout = new KafkaSpout(spoutConfig);
        spout.open(conf, context, collector);
        spout.nextTuple();
        verify(collector, times(2)).emit(anyList(), any());

        server.shutdown();
    }
}
