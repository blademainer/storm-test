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
        // 注册一个sentence spout，并且赋予一个唯一值
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        // 然后注册一个SplitSentenceBolt，这个bolt订阅SentenceSpout发射出来的数据流
        // 将SentenceSpout的唯一ID赋值给shuffleGrouping()方法确立了这种订阅关系
        // shuffleGrouping()方法告诉storm，要将SentenceSpout发射出来的tuple随机均匀的分发给SplitSentenceBolt的实例
        builder.setBolt(SPLIT_BOLT_ID, splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        // 将含有特定数据的tuple路由到特殊的bolt实例中
        // 此处使用BoltDeclarer的fieldsGrouping()方法保证所有“word”字段值相同的tuple会被路由到同一个WordCountBolt实例中
        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        // 将WordCountBolt发出的所有tuple流路由到唯一的ReportBolt中，使用globalGrouping
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Thread.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }


}
