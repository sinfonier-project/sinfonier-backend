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


package com.sinfonier.drains;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;

public class TweetIt extends BaseSinfonierDrain {

    private String CONSUMER_KEY = "consumerKey";
    private String CONSUMER_SECRET = "consumerSecret";
    private String ACCESS_TOKEN = "accessToken";
    private String ACCESS_TOKEN_SECRET = "accessTokenSecret";
    private String fieldToTweet;
    private String customText = "";

    public TweetIt(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        this.CONSUMER_KEY = (String)this.getParam("consumerKey");
        this.CONSUMER_SECRET = (String)this.getParam("consumerSecret");
        this.ACCESS_TOKEN = (String)this.getParam("accessToken");
        this.ACCESS_TOKEN_SECRET = (String)this.getParam("accessTokenSecret");
        this.fieldToTweet = (String)this.getParam("fieldToTweet");
        this.customText = (String)this.getParam("customText");
    }

    @Override
    public void userexecute() {

        ConfigurationBuilder config = new ConfigurationBuilder();
        config.setOAuthConsumerKey(this.CONSUMER_KEY);
        config.setOAuthConsumerSecret(this.CONSUMER_SECRET);
        config.setOAuthAccessToken(this.ACCESS_TOKEN);
        config.setOAuthAccessTokenSecret(this.ACCESS_TOKEN_SECRET);
        config.setJSONStoreEnabled(true);
        config.setIncludeEntitiesEnabled(true);
        try
        {
            TwitterFactory tf = new TwitterFactory(config.build());
            Twitter twitter = tf.getInstance();
            Status status = twitter.updateStatus(this.customText+" "+this.getField(this.fieldToTweet));
            System.out.println("Successfully updated the status to [" + status.getText() + "].");
        }catch (TwitterException te) {
            te.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void usercleanup() {
    }

}