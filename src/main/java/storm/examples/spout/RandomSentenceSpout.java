/**
 * Project : storm_demo
 * Package : storm.examples.spout
 * Author  : xzllc2010<xzllc2010@gmail.com>
 * Create  : On 10/22/2014
 */

package storm.examples.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void ack(Object id) {
        System.out.println("OK: " + id);
    }

    @Override
    public void fail(Object id) {
        System.out.println("FAILD: " + id);
    }

    @Override
    public void activate() {
        super.activate();
        System.out.println("Spout has been activated out of a deactivated mode!");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        //String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        //        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String[] sentences = new String[]{ "the cow jumped over"};


        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));
    }

    @Override
    public void deactivate() {
        super.deactivate();
        System.out.println("Spout has been deactivated!");
    }

    @Override
    public void close() {
        super.close();
        System.out.println("ISpout is going to be shutdown!");
    }


}
