package com.github.dwladdimiroc.syntheticApp.topology;

import com.github.dwladdimiroc.syntheticApp.bolt.SyntheticBolt;
import com.github.dwladdimiroc.syntheticApp.spout.SyntheticSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Topology {
	private static final String TOPOLOGY_NAME = "syntheticApp";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		config.setNumWorkers(1);

		TopologyBuilder builder = new TopologyBuilder();

		// Set Spout
		builder.setSpout("Synthetic", new SyntheticSpout(), 1);

		// Set Bolt
		builder.setBolt("Bolt1", new SyntheticBolt(10, false), 1).shuffleGrouping("Synthetic");
		builder.setBolt("Bolt2", new SyntheticBolt(50, false), 1).shuffleGrouping("Bolt1");
		builder.setBolt("Bolt3", new SyntheticBolt(30, false), 1).shuffleGrouping("Bolt1");
		builder.setBolt("Bolt4", new SyntheticBolt(25, true), 1).shuffleGrouping("Bolt1").shuffleGrouping("Bolt2");

		if (args != null && args.length > 0) {
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(200000);
			cluster.shutdown();
		}

	}
}
