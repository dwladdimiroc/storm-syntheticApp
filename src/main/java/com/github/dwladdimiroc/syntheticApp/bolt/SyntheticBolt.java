package com.github.dwladdimiroc.syntheticApp.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class SyntheticBolt implements IRichBolt {

    private static final long serialVersionUID = 6101916216609388178L;

    private OutputCollector outputCollector;
    private Map mapConf;
    private final long timeSleep;
    private final boolean terminal;

    public SyntheticBolt(long timeSleep, boolean terminal) {
        this.timeSleep = timeSleep;
        this.terminal = terminal;
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.printf("TimeSleep: %d\n", this.timeSleep);
        Utils.sleep(this.timeSleep);
        if (!terminal) {
            this.outputCollector.emit(new Values(tuple));
        }
        this.outputCollector.ack(tuple);
    }

    @Override
    public void prepare(Map mapConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.mapConf = mapConf;
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("numbers"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }

}
