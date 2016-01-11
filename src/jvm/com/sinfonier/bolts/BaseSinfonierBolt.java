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

package com.sinfonier.bolts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.sinfonier.util.ComponentType;
import com.sinfonier.util.SinfonierUtils;
import com.sinfonier.util.XMLProperties;

/**
 * BaseSinfonierBolt. Bolt abstraction for Sinfoniers users.
 */
public abstract class BaseSinfonierBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private OutputCollector _collector;
    private TopologyContext _context;
    private String xmlPath;
    private XMLProperties xml;

    private static final long serialVersionUID = 1L;
    protected static Logger LOG = Logger.getLogger(BaseSinfonierBolt.class);

    private Map<String, Object> json = new HashMap<String, Object>();
    private String entity;
    private String rawJson;
    private Tuple currentTuple;

    /**
     * Constructor.
     * 
     * @param xmlFile Path to XML configuration file.
     */
    public BaseSinfonierBolt(String xmlFile) {
        this.xmlPath = xmlFile;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        SinfonierUtils.broadcastWorker((String) stormConf.get(Config.TOPOLOGY_NAME), context);
        mapper = new ObjectMapper();
        xml = new XMLProperties(context.getThisComponentId(), ComponentType.BOLT, xmlPath);
        _collector = collector;
        _context = context;
        entity = xml.get("entity");
        this.userprepare();
        context.getComponentCommon(context.getThisComponentId()).get_json_conf();
    }

    /**
     * In that function user should initialize his objects and prepare them to be executed. User can
     * access to params with {@link BaseSinfonierBolt#getParam(String)},
     * {@link BaseSinfonierBolt#getParam(String, boolean)} and
     * {@link BaseSinfonierBolt#getParamList(String)} functions.
     */
    public abstract void userprepare();

    @Override
    public final void execute(Tuple input) {

        if (isTickTuple(input)) {
            tickTupleCase();
        } else {
            try {
                this.rawJson = input.getStringByField("map");
                this.json = mapper.readValue(input.getStringByField("map"),
                        new TypeReference<Map<String, Object>>() {
                        });
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (entity == null) {
                entity = input.getStringByField("entity");
            }
	    
	    this.setTupleObj(input);

            this.userexecute();
        }

        _collector.ack(input);
    }

    /**
     * This function is called every time this bolt receives a tuple. In that function user should
     * process data, do transformations, call APIs and whatever you want. Some advices:
     * <p>
     * <ul>
     * <li>Use {@link BaseSinfonierBolt#getJson()} to get map tuple.</li>
     * <li>Use {@link BaseSinfonierBolt#getField(String)} to get fields from tuple. See methods for
     * remove and add too.</li>
     * <li>Call {@link BaseSinfonierBolt#emit()} when tuple is ready to be emitted.</li>
     * <li>If you need raw json, call {@link BaseSinfonierBolt#getRawJson()}.</li>
     * </ul>
     * </p>
     */
    public abstract void userexecute();

    /**
     * Call this method when JSON object is ready to be emitted.
     */
    public final void emit() {
        String jsonstr = "";
        try {
            jsonstr = mapper.writeValueAsString(this.json);
        } catch (JsonGenerationException e) {
            LOG.warn("Can't generate JSON. Failed writeValueAsString.");
            e.printStackTrace();
        } catch (JsonMappingException e) {
            LOG.warn("Can't generate JSON. JSON Mapping Exception.");
            e.printStackTrace();
        } catch (IOException e) {
            LOG.warn("Can't generate JSON. IOException.");
            e.printStackTrace();
        }

        _collector.emit(new Values(entity, jsonstr));
    }

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entity", "map"));
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
     * Replace current tuple by given tuple. Useful if you make deep changes in tuple.
     * 
     * @param json Tuple to set up.
     */
    public void setJSon(Map<String, Object> json) {
        this.json = json;
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
     * Get complex property. Complex property is a list of properties where each one is a map that
     * can contains multiple key-values.
     * 
     * @param propertyName Name of property. e.g.: "bars" -> expect
     *        <bars><bar>..</bar>...<bar>..</bar></bars>
     * @return
     */
    public List<Map<String, Object>> getComplexProperty(String propertyName) {
        return xml.getComplexProperty(ComponentType.BOLT, _context.getThisComponentId(),
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
        } else {
            json.remove(key);
        }
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
     * Set current tuple object.
     *
     * @return
     */
    public void setTupleObj(Tuple input) {
        this.currentTuple = input;
    }

    /**
     * Get Current tuple object.
     *
     * @return
     */
    public Tuple getTupleObj() {
        return this.currentTuple;
    }

    /**
     * Set name of entity tuple to emit to the next component.
     * 
     * @param name name of entity.
     */
    public void setEntity(String name) {
        this.entity = name;
    }
    
    /**
     * Retrieve entity name.
     * 
     * @return the entity name.
     */
    public String getEntity() {
        return entity;
    }
}
