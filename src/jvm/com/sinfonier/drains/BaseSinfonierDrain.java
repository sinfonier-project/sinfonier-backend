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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

/**
 * BaseSinfonierDrain. Drain abstraction for Sinfoniers users.
 */
public abstract class BaseSinfonierDrain extends BaseRichBolt {

    private ObjectMapper mapper;
    private OutputCollector _collector;
    private TopologyContext _context;
    private String xmlPath;
    private XMLProperties xml;
    private String entity;

    private static final long serialVersionUID = 1L;
    protected static Logger LOG = Logger.getLogger(BaseSinfonierDrain.class);

    private Map<String, Object> json = new HashMap<String, Object>();
    private String rawJson;

    /**
     * Constructor.
     * 
     * @param xmlFile Path to xml file.
     */
    public BaseSinfonierDrain(String xmlFile) {
        this.xmlPath = xmlFile;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String topology_name = (String) stormConf.get((Config.TOPOLOGY_NAME));
        SinfonierUtils.broadcastWorker(topology_name, context);
        mapper = new ObjectMapper();
        xml = new XMLProperties(context.getThisComponentId(), ComponentType.DRAIN, xmlPath);
        _collector = collector;
        _context = context;

        this.userprepare();
    }

    /**
     * In that function user should initialize his objects and prepare them to be executed. User can
     * access to params with {@link BaseSinfonierDrain#getParam(String)},
     * {@link BaseSinfonierDrain#getParam(String, boolean)} and
     * {@link BaseSinfonierDrain#getParamList(String)} functions.
     */
    public abstract void userprepare();

    @Override
    public final void execute(Tuple input) {

        if (isTickTuple(input)) {
            tickTupleCase();
        } else {
            setEntity(input);
            try {
                this.rawJson = input.getStringByField("map");
                this.json = mapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {
                });

            } catch (Exception e) {
                e.printStackTrace();
            }

            this.userexecute();
        }

        _collector.ack(input);
    }

    private void setEntity(Tuple input){
        entity = input.getStringByField("entity");
    }

    /**
     * This function is called every time this drain receives a tuple. In that function user should
     * process data, do transformations, call APIs and whatever you want. Some advices:
     * <p>
     * <ul>
     * <li>Use {@link BaseSinfonierDrain#getJson()} to get map tuple.</li>
     * <li>Use {@link BaseSinfonierDrain#getField(String)} to get fields from tuple. See methods for
     * remove and add too.</li>
     * <li>If you need raw json, call {@link BaseSinfonierDrain#getRawJson()}.</li>
     * </ul>
     * </p>
     */
    public abstract void userexecute();

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public final void cleanup() {
        this.usercleanup();
    }

    /**
     * In that function user should clean resources, close connections to databases and save what
     * user want before topology die.
     */
    public abstract void usercleanup();

    /**
     * User must override this method if use tick tuples. This method will be automatically called
     * when bolt receives a tick tuple.
     */
    public void tickTupleCase() {
    }

    /**
     * Check if current tuple is a tick tuple.
     * 
     * @param tuple current tuple.
     * @return {@code true } if is tick tuple.
     */
    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * Get param from xml configuration file.
     * 
     * @param param Name of property.
     * @return Param in a String. You should cast to type what you want.
     */
    public String getParam(String param) {
        return xml.get(param);
    }

    /**
     * Get param from xml, check if is null and if is null, throw an exception.
     * 
     * @param param Name of property.
     * @param checkNotNull boolean that indicates if you want to check null.
     * @return Param in a String. You should cast to type what you want.
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
     * Get complex property. Complex property is a list of properties where each one is a map that
     * can contains multiple key-values.
     * 
     * @param propertyName Name of property. e.g.: "bars" -> expect
     *        <bars><bar>..</bar>...<bar>..</bar></bars>
     * @return Param in a List<Map<String, Object>>. You should cast to type what you want.
     */
    public List<Map<String, Object>> getComplexProperty(String propertyName) {
        return xml.getComplexProperty(ComponentType.DRAIN, _context.getThisComponentId(),
                propertyName);
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
    public void addNestedField(String key, Object value) {
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
        return json.containsKey((String) key);
    }

    /**
     * Get current tuple in a {@link java.util.Map}.
     * 
     * @return map corresponding to current tuple.
     */
    public Map<String, Object> getJson() {
        return json;
    }

    /**
     * Current tuple in raw string json.
     * 
     * @return
     */
    public String getRawJson() {
        return rawJson;
    }

    /**
     * Get the log object.
     * 
     * @return Log.
     */
    public static Logger getLog() {
        return LOG;
    }
    
    /**
     * Retrieve entity name.
     * 
     * @return the entity name.
     */
    public String getEntity(){
        return entity;
    }
}
