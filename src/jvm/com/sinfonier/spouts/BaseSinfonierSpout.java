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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.sinfonier.bolts.BaseSinfonierBolt;
import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

/**
 * BaseSinfonierSpout. Spout abstraction for Sinfoniers users.
 */
public abstract class BaseSinfonierSpout extends BaseRichSpout {

    private ObjectMapper mapper;
    private String xmlPath;
    private String spoutName;
    private String workerport;
    private String topologyname;
    private SpoutOutputCollector _collector;
    private XMLProperties xml;

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(BaseSinfonierSpout.class);

    private Map<String, Object> json = new HashMap<String, Object>();
    private String entity;

    /**
     * BaseSinfonierSpout constructor.
     * 
     * @param spoutName Name of spout.
     * @param xmlFile Path to xml configuration file.
     */
    public BaseSinfonierSpout(String spoutName, String xmlFile) {
        this.xmlPath = xmlFile;
        this.spoutName = spoutName;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public final void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        SinfonierUtils.broadcastWorker((String) conf.get(Config.TOPOLOGY_NAME), context);
        mapper = new ObjectMapper();
        xml = new XMLProperties(spoutName, ComponentType.SPOUT, xmlPath);
        _collector = collector;
        entity = xml.get("entity", true);
        this.useropen();
    }

    /**
     * In that function user should initialize his objects, connections ... and prepare them.
     */
    public abstract void useropen();

    @Override
    public final void nextTuple() {
        this.usernextTuple();
        this.json = new HashMap<String, Object>();
    }

    /**
     * This function is called by default every 100 ms. In that function user emit new tuples to the
     * topology.
     */
    public abstract void usernextTuple();

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "map"));
    }

    @Override
    public final void close() {
        this.userclose();
    }

    /**
     * In that function user should clean resources, close connections.
     */
    public abstract void userclose();

    /**
     * Emits {@link com.sinfonier.spouts.BaseSinfonierSpout#json} object.
     */
    public final void emit() {
        String jsonstr = "";
        try {
            jsonstr = mapper.writeValueAsString(this.json);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        _collector.emit(new Values(this.entity, jsonstr));
    }

    /**
     * Get param from xml configuration file.
     * 
     * @param param Name of property.
     * @return Param in a String. You should cast to tye what you want.
     */
    public String getParam(String param) {
        return xml.get(param);
    }

    /**
     * Get param from xml, check if is null and if is null, throw an exception.
     * 
     * @param param Name of property.
     * @param checkNotNull boolean that indicates if you want to check null.
     * @return
     */
    public String getParam(String param, boolean checkNotNull) {
        return xml.get(param, checkNotNull);
    }

    /**
     * Get list of property from xml configuration.
     * 
     * @param param Name of property
     * @return Object list.
     */
    public List<Object> getParamList(String param) {
        return xml.getList(param);
    }

    /**
     * Add field to tuple.
     * 
     * @param key Key. Use . to access nested documents. e.g: user.name would add name key to
     *        document user.
     */
    public void addField(String key, Object value) {
        if (key.indexOf(".") >= 0) {
            addNestedField(key, value);
        } else {
            json.put(key, value);
        }
    }

    /**
     * Divide key in parts (split by '.'), access to nested documents or create them if doesn't
     * exists and finally adds the key.
     * 
     * @param key Name of key.
     * @return Value of given key in tuple.
     */
    private void addNestedField(String key, Object value) {
        String[] parts = key.split("\\.");
        Map<String, Object> jsonCopy = json;
        for (int i = 0; i < parts.length - 1; i++) {
            if (jsonCopy.get(parts[i]) == null) {
                jsonCopy.put(parts[i], new HashMap<String, Object>());
            }
            jsonCopy = (Map<String, Object>) jsonCopy.get(parts[i]);
        }
        jsonCopy.put(parts[parts.length - 1], value);
    }

    /**
     * Get field from tuple.
     * 
     * @param key Key. Use . to access nested documents. e.g: user.name would access to document
     *        user and get name key.
     * @return Value of field corresponding to key.
     */
    public Object getField(String key) {
        if (key.indexOf(".") >= 0) {
            return getNestedField(key);
        }
        return json.get(key);
    }

    /**
     * Divide key in parts (split by '.'), access to nested documents and finally retrieves the key.
     * 
     * @param key Name of key.
     * @return Value of given key in tuple.
     */
    @SuppressWarnings("unchecked")
    private Object getNestedField(String key) {
        String[] parts = key.split("\\.");
        Map<String, Object> value = json;
        for (int i = 0; i < parts.length - 1; i++) {
            value = (Map<String, Object>) value.get(parts[i]);
        }
        return value.get(parts[parts.length - 1]);
    }

    /**
     * Remove a field from tuple.
     * 
     * @param key Key. Use . to refer nested documents. e.g: user.name would access to document user
     *        and get name key.
     */
    public void removeField(String key) {
        if (key.indexOf(".") >= 0) {
            removeNestedField(key);
        }
        json.remove(key);
    }

    /**
     * Divide key in parts (split by '.'), access to nested documents and finally removes the key.
     * 
     * @param key Name of key.
     * @return Value of given key in tuple.
     */
    @SuppressWarnings("unchecked")
    private Object removeNestedField(String key) {
        String[] parts = key.split("\\.");
        Map<String, Object> value = json;
        for (int i = 0; i < parts.length - 1; i++) {
            value = (Map<String, Object>) value.get(parts[i]);
        }
        return value.remove(parts[parts.length - 1]);
    }

    /**
     * Check if exists field corresponding to given key.
     * 
     * @param key Key to check.
     * @return {@code true} if tuple contains key and {@code false} in other case.
     */
    public boolean existsField(String key) {
        return json.containsKey(key);
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    /**
     * Get mapper to manage and transform to JSON.
     * 
     * @return a {@link ObjectMapper}
     */
    public ObjectMapper getMapper() {
        return mapper;
    }

    /**
     * Set current tuple in json raw format.
     * 
     * @param json JSON in String.
     */
    public void setJson(String json) {
        try {
            this.json = mapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
