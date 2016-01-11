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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

//@formatter:off
/**
 * Twitter Spout. Ingest Data from twitter 
 * <p> XML Options:<br/>
 * <ul>
 * <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
 * <li> <b>{@code <consumerKey></consumerKey>}</b> - Needed. Twitter Consumer Key for Streaming API. </li>
 * <li> <b>{@code <consumerSecret></consumerSecret>}</b> - Needed. Twitter Consumer Secret for Streaming API. </li>
 * <li> <b>{@code <accessToken></accessToken>}</b> - Needed. Twitter access token for Streaming API. </li>
 * <li> <b>{@code <accessTokenSecret></accessTokenSecret>}</b> - Needed. Twitter access token Secret for Streaming API. </li>
 * <li> <b>{@code <keyword>key1</keyword>...<keyword></keyword>}</b> - Optional. Twitter keywords to search for. </li>
 * <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt. </li>
 * <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism. </li>
 * </ul>
 */
//@formatter:on
public class Twitter extends BaseRichSpout {

    private static final long serialVersionUID = 1527666672521152910L;
    private static final String CONSUMER_KEY = "consumerKey";
    private static final String CONSUMER_SECRET = "consumerSecret";
    private static final String ACCESS_TOKEN = "accessToken";
    private static final String ACCESS_TOKEN_SECRET = "accessTokenSecret";
    private static final Logger LOG = LoggerFactory.getLogger(Twitter.class);

    private String xmlPath;
    private String spoutName;
    private XMLProperties xml;
    private SpoutOutputCollector _collector;
    private String entity = "tweet";

    private TwitterStream twitterStream;
    private LinkedBlockingQueue<String> queue = null;

    public Twitter(String spoutName, String xmlPath) {
        this.xmlPath = xmlPath;
        this.spoutName = spoutName;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        xml = new XMLProperties(spoutName, ComponentType.SPOUT, xmlPath);
        _collector = collector;
        SinfonierUtils.broadcastWorker((String) conf.get(Config.TOPOLOGY_NAME), context);

        twitterStream = createTwitterStream();
        queue = new LinkedBlockingQueue<String>(1000);

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {

                queue.offer(TwitterObjectFactory.getRawJSON(status));
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

        };

        twitterStream.addListener(listener);

        List<Object> rawKeywords = xml.getList("keyword");
        // String rawKeywords = xml.get("keywords");
        if (rawKeywords.isEmpty()) {
            twitterStream.sample();
        } else {
            List<String> keywords = new ArrayList<String>();
            for (Object field : rawKeywords) {
                keywords.add(((String) field).trim());
            }
            FilterQuery query = new FilterQuery()
                    .track(keywords.toArray(new String[keywords.size()]));
            twitterStream.filter(query);
        }
    }

    @Override
    public void nextTuple() {
        String json = queue.poll();
        if (json == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(entity, json));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "map"));
    }

    /**
     * Create a TwitterStream. Requires Twitter API keys.
     * 
     * @return {@link twitter4j.TwitterStream}
     */
    private TwitterStream createTwitterStream() {

        ConfigurationBuilder config = new ConfigurationBuilder();
        config.setOAuthConsumerKey(xml.get(CONSUMER_KEY, true));
        config.setOAuthConsumerSecret(xml.get(CONSUMER_SECRET, true));
        config.setOAuthAccessToken(xml.get(ACCESS_TOKEN, true));
        config.setOAuthAccessTokenSecret(xml.get(ACCESS_TOKEN_SECRET, true));
        config.setJSONStoreEnabled(true);
        config.setIncludeEntitiesEnabled(true);

        return new TwitterStreamFactory(config.build()).getInstance();
    }

}
