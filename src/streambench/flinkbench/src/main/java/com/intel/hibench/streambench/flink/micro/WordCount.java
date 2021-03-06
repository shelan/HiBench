/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.flink.micro;

import com.intel.hibench.streambench.flink.util.FlinkBenchConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import scala.xml.PrettyPrinter;

import java.util.HashMap;
import java.util.Map;

/**
 * This example shows an implementation of WordCount without using the Tuple2
 * type, but a custom class.
 * <p>
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * <p>
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use POJO data types,
 * <li>write a simple Flink program,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // get input data
        DataStream<String> text = getKafkaDataStream(env, params);

        DataStream<Word> counts =
                // split up the lines into Word objects
                text.flatMap(new Tokenizer())
                        // group by the field word and sum up the frequency
                        .keyBy("word").sum("frequency");


        //TODO: commented because other benchmarks do not write the output. Check if there is anyother option.
        //counts.writeAsText("output", FileSystem.WriteMode.OVERWRITE);
        // execute program
        env.execute("WordCount");
    }


    public void run(FlinkBenchConfig config) throws Exception {

        Map configMap = new HashMap();
        configMap.put("zookeeper.connect", config.zookeeperConnect);
        configMap.put("group.id", config.groupId);
        configMap.put("bootstrap.servers", config.kafkaBrokers);
        configMap.put("topic", config.kafkaTopic);


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromMap(configMap);

        // get input data
        DataStream<String> text = getKafkaDataStream(env, params);

        DataStream<Word> counts =
                // split up the lines into Word objects
                text.flatMap(new Tokenizer())
                        // group by the field word and sum up the frequency
                        .keyBy("word").sum("frequency");

        counts.writeAsText("output", FileSystem.WriteMode.OVERWRITE);
        // execute program
        env.execute("WordCount");
    }

    // *************************************************************************
    // DATA TYPES
    // *************************************************************************

    /**
     * This is the POJO (Plain Old Java Object) that is being used for all the
     * operations. As long as all fields are public or have a getter/setter, the
     * system can handle them
     */
    public static class Word {

        private String word;
        private Integer frequency;

        public Word() {
        }

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getFrequency() {
            return frequency;
        }

        public void setFrequency(Integer frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "(" + word + "," + frequency + ")";
        }
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Word> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Word> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String textPath;
    private static String outputPath;


    private static DataStream<String> getKafkaDataStream(StreamExecutionEnvironment env, ParameterTool params) {
        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer082<>(
                        params.get("topic"),
                        new SimpleStringSchema(),
                        params.getProperties()));
        return messageStream;
    }
}