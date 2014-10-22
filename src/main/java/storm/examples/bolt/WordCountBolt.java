/**
 * Project : storm_demo
 * Package : storm.examples.bolt
 * Author  : xzllc2010<xzllc2010@gmail.com>
 * Create  : On 10/22/2014
 */

package storm.examples.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        System.out.println("-- Word Counter ["+ name + "-" + id + "] --");
        for (Map.Entry<String, Integer> entry : counts.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
