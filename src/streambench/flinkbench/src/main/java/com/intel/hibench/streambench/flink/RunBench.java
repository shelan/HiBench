package com.intel.hibench.streambench.flink;

import com.intel.hibench.streambench.flink.micro.WordCount;
import com.intel.hibench.streambench.flink.util.ConfigLoader;
import com.intel.hibench.streambench.flink.util.FlinkBenchConfig;

/**
 * Created by shelan on 12/29/15.
 */
public class RunBench {

    public static void main(String[] args) throws Exception {
        runAll(args);
    }

    private static void runAll(String[] args) throws Exception {
        FlinkBenchConfig flinkConfig = new FlinkBenchConfig();
        ConfigLoader configLoader = new ConfigLoader(args[1]);

        flinkConfig.zookeeperConnect = configLoader.getProperty("hibench.streamingbench.zookeeper.host");
        flinkConfig.kafkaBrokers =configLoader.getProperty("hibench.streamingbench.brokerList");
        flinkConfig.groupId =configLoader.getProperty("hibench.streamingbench.consumer_group");
        flinkConfig.kafkaTopic =configLoader.getProperty("hibench.streamingbench.topic_name");
        flinkConfig.benchName = configLoader.getProperty("hibench.streamingbench.benchname");

        if("wordcount".equals(flinkConfig.benchName)){
            WordCount wordcount = new WordCount();
            wordcount.run(flinkConfig);
        }
    }
}
