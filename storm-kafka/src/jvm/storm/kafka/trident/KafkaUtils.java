package storm.kafka.trident;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import backtype.storm.utils.Utils;
import kafka.api.FetchRequest;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.trident.operation.TridentCollector;

public class KafkaUtils {
     public static Map emitPartitionBatchNew(TridentKafkaConfig config, int partition, SimpleConsumer consumer, TransactionAttempt attempt, TridentCollector collector, Map lastMeta, String topologyInstanceId) {
         StaticHosts hosts = (StaticHosts) config.hosts;
         long offset;
         if(lastMeta!=null) {
             String lastInstanceId = null;
             Map lastTopoMeta = (Map) lastMeta.get("topology");
             if(lastTopoMeta!=null) {
                 lastInstanceId = (String) lastTopoMeta.get("id");
             }
             if(config.forceFromStart && !topologyInstanceId.equals(lastInstanceId)) {
                 offset = consumer.getOffsetsBefore(config.topic, partition.partition, config.startOffsetTime, 1)[0];
             } else {
                 offset = (Long) lastMeta.get("nextOffset");
             }
         } else {
             long startTime = -1;
             if(config.forceFromStart) startTime = config.startOffsetTime;
             offset = consumer.getOffsetsBefore(config.topic, partition.partition, startTime, 1)[0];
         }
         ByteBufferMessageSet msgs;
         try {
            msgs = consumer.fetch(new FetchRequest(config.topic, partition % hosts.partitionsPerHost, offset, config.fetchSizeBytes));
            msgs.underlying().size();
         } catch (OffsetOutOfRangeException _) {
             long timeStamp = lastMeta.get("timeStamp")!=null? (Long) lastMeta.get("timeStamp"):0L;
             long[] offsets = consumer.getOffsetsBefore(config.topic, partition % hosts.partitionsPerHost, timeStamp, 1);
             if(offsets!=null && offsets.length > 0)
                 offset = offsets[0];
             else
                 offset = 0L;

             msgs = consumer.fetch(new FetchRequest(config.topic, partition % hosts.partitionsPerHost, offset, config.fetchSizeBytes));
         } catch(Exception e) {
             if(e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
         }
         long endoffset = offset;
         for(MessageAndOffset msg: msgs) {
             emit(config, collector, msg.message());
             endoffset = msg.offset();
         }
         Map newMeta = new HashMap();
         newMeta.put("offset", offset);
         newMeta.put("nextOffset", endoffset);
         newMeta.put("instanceId", topologyInstanceId);
         newMeta.put("partition", partition.partition);
         newMeta.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
         newMeta.put("topic", config.topic);
         newMeta.put("topology", ImmutableMap.of("name", topologyName, "id", topologyInstanceId));
         return newMeta;
     }

     public static void emit(TridentKafkaConfig config, TransactionAttempt attempt, TridentCollector collector, Message msg) {
         List<Object> values = config.scheme.deserialize(Utils.toByteArray(msg.payload()));
         if(values!=null) {
             for(List<Object> value: values)
                 collector.emit(value);
         }
     }
}
