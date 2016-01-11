// Copyright 2015 Sinfonier Project
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sinfonier;

import static com.sinfonier.util.XMLProperties.XMLCheckNotNull;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;

import com.sinfonier.exception.SinfonierException;
import com.sinfonier.exception.XMLConfigException;
import com.sinfonier.util.ComponentType;
import com.sinfonier.util.XMLProperties;

import java.util.Random;

/**
 * DynamicTopology class. Reads XML configuration file in the classpath and set up the topology in a
 * Storm cluster. Also declare multilanguage static clases to use in storm topologies
 */
public class DynamicTopology {

    private static final Logger LOG = Logger.getLogger(DynamicTopology.class);
    private static String xmlPath;
    private static String topologyName = null;

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage:  DynamicTopology configurationfile [name]");
            System.exit(-1);
        } else {
            xmlPath = args[0];
            if (args.length > 1) {
                topologyName = args[1];
            }
        }

        LOG.info("Reading XML file configuration...");
        XMLConfiguration config = new XMLConfiguration(xmlPath);
        TopologyBuilder builder = new TopologyBuilder();

        /* Spout Configuration */
        List<HierarchicalConfiguration> spouts = config.configurationsAt("spouts.spout");
        configureSpouts(builder, spouts);

        /* Bolt Configuration * */
        List<HierarchicalConfiguration> bolts = config.configurationsAt("bolts.bolt");
        configureBolts(builder, bolts, config);

        /* Drain Configuration * */
        List<HierarchicalConfiguration> drains = config.configurationsAt("drains.drain");
        configureDrains(builder, drains, config);

        /* Configure more Storm options */
        Config conf = getTopologyConfigFromXML(xmlPath);

        if (topologyName != null) { // Submit topology to a cluster
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else { // Local mode
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(1000000); // Alive for 100 seconds = 100000 ms
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    /**
     * Get global topology config from XML.
     * 
     * @param xmlPath Path to xml configuration.
     * @return {@link backtype.storm.Config} object containing global configuration.
     * @throws ConfigurationException
     */
    private static Config getTopologyConfigFromXML(String xmlPath) throws ConfigurationException {
        Config conf = new Config();
        XMLProperties xml = new XMLProperties(ComponentType.OPTIONS, xmlPath);

        conf.setDebug(false);
        // TODO Add more topology options
        return conf;
    }

    /**
     * Configure spouts from XML.
     * 
     * @param builder Topology builder.
     * @param spouts List of spouts, <spout></spout> in xml.
     * @throws Exception
     */
    private static void configureSpouts(TopologyBuilder builder,
            List<HierarchicalConfiguration> spouts) throws Exception {

        if (spouts == null || spouts.isEmpty()) {
            throw new SinfonierException(
                    "There is no spouts. Add at least one spout or review your xml configuration");
        }

        for (HierarchicalConfiguration spout : spouts) {
            String spoutClass = XMLCheckNotNull("@class", spout.getString("[@class]"));
            LOG.info("Creating spout with id:"
                    + XMLCheckNotNull("@abstractionId", spout.getString("[@abstractionId]")));
            
            Object spoutDefault;
            try {
                spoutDefault = Class.forName(spoutClass)
                    .getConstructor(String.class, String.class)
                    .newInstance(spout.getString("[@abstractionId]"), xmlPath);
            } catch (Exception e){
                // If we are working with not java spout newInstance constructor include one more parameter
                spoutDefault = Class.forName(spoutClass)
                    .getConstructor(String.class, String.class, String.class)
                    .newInstance(spout.getString("[@abstractionId]"), xmlPath, Integer.toString(-new Random(System.nanoTime()).nextInt(1000000000)));
            }

            

            SpoutDeclarer spoutDeclarer = null;
            if (spoutDefault instanceof BaseRichSpout) {
                // Spouts pure Java
                spoutDeclarer = builder.setSpout(
                        spout.getString("[@abstractionId]"),
                        (BaseRichSpout) spoutDefault,
                        Integer.parseInt(XMLCheckNotNull("parallelism",
                                spout.getString("parallelism"))));
            } else if (spoutDefault instanceof IRichSpout) {
                // Multilanguage (implements IRichSpout)
                spoutDeclarer = builder.setSpout(
                        spout.getString("[@abstractionId]"),
                        (IRichSpout) spoutDefault,
                        Integer.parseInt(XMLCheckNotNull("parallelism",
                                spout.getString("parallelism"))));
            } else {
		switch (XMLCheckNotNull("@class", spout.getString("[@class]"))) {
                case "com.sinfonier.spouts.Kafka":
			Object spoutKafka = Class.forName(spout.getString("[@class]"))
				.getConstructor(String.class, String.class)
				.newInstance(spout.getString("[@abstractionId]"), xmlPath);
			builder.setSpout(spout.getString("[@abstractionId]"), (KafkaSpout) spoutKafka
				.getClass().getMethod("getKafkaSpout").invoke(spoutKafka),
				spout.getInt("parallelism"));
			break;
		default: // Default case
			throw new XMLConfigException("Cannot find " + spoutClass + " class.");
		}
	    }

            // Set num tasks.
            if (spout.getString("numTasks") != null && spoutDeclarer != null) {
                spoutDeclarer.setNumTasks(spout.getInt("numTasks"));
            }

            // Set tick tuple.
            if (spout.getString("tickTuple") != null && spoutDeclarer != null) {
                spoutDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, spout.getInt("tickTuple"));
            }
        }
    }

    /**
     * Configure bolts from XML.
     * 
     * @param builder Topology builder.
     * @param spouts List of spouts, <spout></spout> in xml.
     * @param config XMLConfiguration object.
     * @throws Exception
     */
     private static void configureBolts(TopologyBuilder builder,
            List<HierarchicalConfiguration> bolts, XMLConfiguration config) throws Exception {
        int i = 0;
        for (HierarchicalConfiguration bolt : bolts) {
            String boltClass = XMLCheckNotNull("@class", bolt.getString("[@class]"));
            LOG.info("Creating bolt with id:"
                    + XMLCheckNotNull("@abstractionId", bolt.getString("[@abstractionId]")));
            
            BoltDeclarer boltDeclarer;            

            try {
                
                boltDeclarer = builder
                .setBolt(
                        bolt.getString("[@abstractionId]"),
                        (IRichBolt) Class.forName(boltClass).getConstructor(String.class)
                                .newInstance(xmlPath),
                        Integer.parseInt(XMLCheckNotNull("parallelism",
                                bolt.getString("parallelism"))));
            } catch (Exception e) {

                boltDeclarer = builder
                .setBolt(
                        bolt.getString("[@abstractionId]"),
                        (IRichBolt) Class.forName(boltClass).getConstructor(String.class, String.class)
                                .newInstance(xmlPath, Integer.toString(-new Random(System.nanoTime()).nextInt(1000000000))),
                        Integer.parseInt(XMLCheckNotNull("parallelism",
                                bolt.getString("parallelism"))));
            }
            
            // Set connections between components.
            List<HierarchicalConfiguration> sources = config.configurationsAt("bolts.bolt(" + i
                    + ").sources.source");
            readSourcesFromBoltsAndDrains(boltDeclarer, sources);

            // Set num tasks.
            if (bolt.getString("numTasks") != null) {
                boltDeclarer.setNumTasks(bolt.getInt("numTasks"));
            }

            // Set tick tuple.
            if (bolt.getString("tickTuple") != null) {
                boltDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, bolt.getInt("tickTuple"));
            }

            i++;
        }
    }



    /**
     * Configure drains from XML.
     * 
     * @param builder Topology builder.
     * @param spouts List of spouts, <spout></spout> in xml.
     * @param config XMLConfiguration object.
     * @throws Exception
     */
    private static void configureDrains(TopologyBuilder builder,
            List<HierarchicalConfiguration> drains, XMLConfiguration config) throws Exception {

        if (drains == null || drains.isEmpty()) {
            throw new SinfonierException(
                    "There is no drains. Add at least one spout or review your xml configuration");
        }

        int i = 0;
        for (HierarchicalConfiguration drain : drains) {
            String drainClass = XMLCheckNotNull("@class", drain.getString("[@class]"));
            LOG.info("Creating drain with id:"
                    + XMLCheckNotNull("@abstractionId", drain.getString("[@abstractionId]")));

            BoltDeclarer drainDeclarer;            

            try {
                
                drainDeclarer = builder.setBolt(
                    drain.getString("[@abstractionId]"),
                    (IRichBolt) Class.forName(drainClass).getConstructor(String.class)
                            .newInstance(xmlPath), Integer.parseInt(XMLCheckNotNull("parallelism",
                            drain.getString("parallelism"))));

            } catch (Exception e) {

                drainDeclarer = builder
                .setBolt(
                        drain.getString("[@abstractionId]"),
                        (IRichBolt) Class.forName(drainClass).getConstructor(String.class, String.class)
                                .newInstance(xmlPath, Integer.toString(-new Random(System.nanoTime()).nextInt(1000000000))),
                        Integer.parseInt(XMLCheckNotNull("parallelism",
                                drain.getString("parallelism"))));
            }

            List<HierarchicalConfiguration> sources = config.configurationsAt("drains.drain(" + i
                    + ").sources.source");
            readSourcesFromBoltsAndDrains(drainDeclarer, sources);

            // Set num tasks.
            if (drain.getString("numTasks") != null) {
                drainDeclarer.setNumTasks(drain.getInt("numTasks"));
            }

            // Set tick tuple.
            if (drain.getString("tickTuple") != null) {
                drainDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, drain.getInt("tickTuple"));
            }

            i++;
        }
    }

    /**
     * Set sources of component from xml.
     * 
     * @param boltDeclarer Current Bolt declaration.
     * @param sources List of <source></source> elements from xml.
     */
    private static void readSourcesFromBoltsAndDrains(BoltDeclarer boltDeclarer,
            List<HierarchicalConfiguration> sources) {

        // There must be contains at least one source.
        if (sources.isEmpty()) {
            throw new XMLConfigException("Error in XML Configuration. Not sources found for bolt");
        }

        for (HierarchicalConfiguration source : sources) {
            switch (source.getString("grouping").toLowerCase()) {
                case "shuffle":
                    if (null == source.getString("streamId")) {
                        boltDeclarer = boltDeclarer.shuffleGrouping(source.getString("sourceId"));
                    } else {
                        boltDeclarer = boltDeclarer.shuffleGrouping(source.getString("sourceId"),
                                source.getString("streamId"));
                    }
                    break;
                case "field":
                    String field = source.getString("grouping[@field]");
                    if (null == source.getString("streamId")) {
                        boltDeclarer = boltDeclarer.fieldsGrouping(source.getString("sourceId"),
                                new Fields(field));
                    } else {
                        boltDeclarer.fieldsGrouping(source.getString("sourceId"),
                                source.getString("streamId"), new Fields(field));
                    }
                    break;
                case "global":
                    if (null == source.getString("streamId")) {
                        boltDeclarer = boltDeclarer.globalGrouping(source.getString("sourceId"));
                    } else {
                        boltDeclarer = boltDeclarer.globalGrouping(source.getString("sourceId"),
                                source.getString("streamId"));
                    }
                    break;
            }
        }
    }
}
