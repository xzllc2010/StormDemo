/**
 * Project : storm_demo
 * Package : storm.examples.bolt
 * Author  : xzllc2010<xzllc2010@gmail.com>
 * Create  : On 10/22/2014
 */

package storm.examples.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt implements IRichBolt {

    private OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        System.out.println("Called when a task for this component is initialized within a worker on the cluster.");
        this._collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                // Emit the word
                _collector.emit(new Values(word));
            }
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        System.out.println("IBolt is going to be shutdown!");
    }


}
