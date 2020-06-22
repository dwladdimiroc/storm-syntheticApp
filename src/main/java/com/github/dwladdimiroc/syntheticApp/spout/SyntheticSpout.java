package com.github.dwladdimiroc.syntheticApp.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SyntheticSpout implements IRichSpout {
    private static final long serialVersionUID = -6896495716308281352L;

    private static final Logger logger = LoggerFactory.getLogger(SyntheticSpout.class);
    private Map conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    private LinkedBlockingQueue<Integer> queue;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;

        this.queue = new LinkedBlockingQueue<Integer>(100000);

        Thread thread = new Thread(){
            public void run(){
                while (true) {
                    int rand = (int) Math.floor(Math.random() * 10);
                    for (int i = 0; i < rand; i++) {
                        queue.add(rand);
                    }
                    Utils.sleep(100);
                }
            }
        };
        thread.start();
    }

    @Override
    public void close() {
        logger.info("Close");
    }

    @Override
    public void activate() {
        logger.info("Activate");

    }

    @Override
    public void deactivate() {
        logger.info("Deactivate");
    }

    @Override
    public void nextTuple() {
        Integer nums = queue.poll();
        if (nums == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(nums));
        }
    }

    @Override
    public void ack(Object msgId) {
        logger.info("ACK " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        logger.info("Fail " + msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numbers"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return conf;
    }

}
