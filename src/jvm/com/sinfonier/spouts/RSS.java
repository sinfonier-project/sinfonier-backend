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

package com.sinfonier.spouts;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

//@formatter:off
/**
* Feed RSS Spout. Retrieve data from feed RSS.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <url></url>}</b> - Needed. Url from RSS you want retrieve. </li>
* <li> <b>{@code <frequency></frequency>}</b> - Needed.  </li>
* <li> <b>{@code <paralellism>1</paralellism> }</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class RSS extends BaseRichSpout {

    private String xmlPath;
    private String spoutName;
    private SpoutOutputCollector _collector;

    private URL url;
    private List<String> emitted = new ArrayList<>();
    private LinkedBlockingQueue<String> queue = null;

    public RSS(String spoutName, String xmlPath) {
        this.xmlPath = xmlPath;
        this.spoutName = spoutName;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        XMLProperties xml = new XMLProperties(spoutName, ComponentType.SPOUT, xmlPath);
        SinfonierUtils.broadcastWorker((String) conf.get(Config.TOPOLOGY_NAME), context);
        _collector = collector;
        try {
            url = new URL(xml.get("url", true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        queue = new LinkedBlockingQueue<String>(1000);
        int frequency = xml.getInt("frequency", true);

        JobDetail job = JobBuilder.newJob(RetrieveAndParseFeed.class)
                .withIdentity("dummyJobName", "group1").build();

        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity("Retrieve Items")
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(frequency)
                                .repeatForever()).build();

        String schedulerName = (String) conf.get(Config.TOPOLOGY_NAME) + "-"
                + context.getThisComponentId() + "-" + UUID.randomUUID();
        System.setProperty("org.quartz.scheduler.instanceName", schedulerName);
        Scheduler scheduler;
        try {
            StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory();
            stdSchedulerFactory.initialize();
            scheduler = stdSchedulerFactory.getScheduler();
            scheduler.getContext().put("queue", queue);
            scheduler.getContext().put("emitted", emitted);
            scheduler.getContext().put("url", url);
            scheduler.start();
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (!queue.isEmpty()) {
            String json = queue.poll();
            _collector.emit(new Values("rssItem", json));
        } else {
            Utils.sleep(50);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "map"));
    }

    public static class RetrieveAndParseFeed implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            SchedulerContext schedulerContext;
            URL url;
            try {
                schedulerContext = context.getScheduler().getContext();
                @SuppressWarnings("unchecked")
                LinkedBlockingQueue<String> queue = (LinkedBlockingQueue<String>) schedulerContext
                        .get("queue");
                url = (URL) schedulerContext.get("url");
                List<String> emitted = (List<String>) schedulerContext.get("emitted");
                InputStream is = url.openStream();
                int ptr = 0;
                StringBuilder builder = new StringBuilder();
                while ((ptr = is.read()) != -1) {
                    builder.append((char) ptr);
                }
                String xml = builder.toString();

                JSONObject jsonObject = XML.toJSONObject(xml).getJSONObject("rss").getJSONObject("channel");
                JSONArray jsonArray = jsonObject.getJSONArray("item");
                for (int i = 0; i < jsonArray.length(); i++) {
                    if (emitted.isEmpty()
                            || !emitted.contains(jsonArray.getJSONObject(i).toString())) {
                        queue.put(jsonArray.getJSONObject(i).toString());
                    }
                }

                emitted.clear();
                for (int i = 0; i < jsonArray.length(); i++) {
                    emitted.add(jsonArray.getJSONObject(i).toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
