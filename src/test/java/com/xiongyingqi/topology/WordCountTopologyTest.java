package com.xiongyingqi.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.xiongyingqi.bolt.ReportBolt;
import com.xiongyingqi.bolt.SplitSentenceBolt;
import com.xiongyingqi.bolt.WordCountBolt;
import com.xiongyingqi.spout.SentenceSpout;
import org.junit.Test;

/**
 * Created by qi<a href="http://xiongyingqi.com">xiongyingqi.com</a> on 2015-07-01 20:06.
 */
public class WordCountTopologyTest {
    public static final String SENTENCE_SPOUT_ID = "sentence-spout";
    public static final String SPLIT_BOLT_ID     = "split-bolt";
    public static final String COUNT_BOLT_ID     = "count-bolt";
    public static final String REPORT_BOLT_ID    = "report-bolt";
    public static final String TOPOLOGY_NAME     = "word-count-topology";

    @Test
    public void testStorm() throws Exception {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        builder.setBolt(SPLIT_BOLT_ID, splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Thread.sleep(100);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }


}
