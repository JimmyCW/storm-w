package com.wx;

import com.wx.bolt.ExclaimationBolt;
import com.wx.bolt.PrintBolt;
import com.wx.spout.TestWordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new org.apache.storm.testing.TestWordSpout(),1 );
        builder.setBolt("exclaim", new ExclaimationBolt(), 1).shuffleGrouping("word");
        builder.setBolt("print", new PrintBolt(), 1).shuffleGrouping("exclaim");
        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test3", config, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("test3");
            cluster.shutdown();
        }
    }
}
