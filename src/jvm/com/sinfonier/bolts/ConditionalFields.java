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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import backtype.storm.Config;
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

//@formatter:off
/**
* Conditional Bolt. Divide the default stream depending on condition.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <url></url>}</b> -Needed. Url from RSS you want retrieve.</li>
* <li> <b>{@code <field></field>}</b> - Needed. Field of entity where we can find full URL. </li>
* <li> <b>{@code <operator></operator>}</b> - Needed. Operator to compare field and value. </li>
* <li> <b>{@code <value></value>}</b> - Needed. Value to be compared to. </li>
* <li> <b>{@code <entity></entity>}</b> - Optional. Entity, in case you want to change it. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Needed. Num tasks of this bolt.</li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism.</li>
* </ul>
*/
//@formatter:on
public class ConditionalFields extends BaseRichBolt {

    private static final long serialVersionUID = -2124724236957307080L;
    private ObjectMapper mapper;
    private String xmlPath;
    private XMLProperties xml;
    private OutputCollector _collector;

    private String field;
    private String operator;
    private String field2;

    private String entity;

    private Map<String, Object> json = new HashMap<String, Object>();

    public ConditionalFields(String xmlPath) {
        this.xmlPath = xmlPath;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        SinfonierUtils.broadcastWorker((String) stormConf.get(Config.TOPOLOGY_NAME), context);
        xml = new XMLProperties(context.getThisComponentId(), ComponentType.BOLT, xmlPath);
        _collector = collector;
        mapper = new ObjectMapper();

        field = xml.get("field1", true);
        operator = xml.get("operator", true);
        field2 = xml.get("field2", true);
        entity = xml.get("entity");

    }

    @Override
    public void execute(Tuple input) {
        try {
            this.json = mapper.readValue(input.getStringByField("map"),
                    new TypeReference<Map<String, Object>>() {
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

        boolean comparisonResult = doComparation();
        String jsonstr = "";
        try {
            jsonstr = mapper.writeValueAsString(this.json);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (comparisonResult) {
            if (entity != null) {
                _collector.emit("yes", new Values(entity, jsonstr));
            } else {
                _collector.emit("yes", new Values(input.getStringByField("entity"), jsonstr));
            }
        } else {
            if (entity != null) {
                _collector.emit("no", new Values(entity, jsonstr));
            } else {
                _collector.emit("no", new Values(input.getStringByField("entity"), jsonstr));
            }
        }
        _collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("yes", new Fields("entity", "map"));
        declarer.declareStream("no", new Fields("entity", "map"));
    }

    private boolean doComparation() {
        boolean result = false;
	Object jsonField = null;
	String jsonField2 = "";
        try{
		jsonField = getField(this.field);
		jsonField2 = (String)getField(this.field2);
	}
	catch(Exception e){
		System.out.println("Field not found on json map received. Check bolt params");
	}
        if (jsonField == null) {
            return false;
        }

        switch (operator) {
            case "<":
                result = compare(jsonField, jsonField2) < 0 ? true : false;
                break;
            case "<=":
                result = compare(jsonField, jsonField2) <= 0 ? true : false;
                break;
            case ">":
                result = compare(jsonField, jsonField2) > 0 ? true : false;
                break;
            case ">=":
                result = compare(jsonField, jsonField2) >= 0 ? true : false;
                break;
            case "==":
                result = jsonField.equals(jsonField2) ? true : false;
                break;
            case "!=":
                result = jsonField.equals(jsonField2) ? false : true;
                break;
            case "RegexExpression":
                result = Pattern.matches(String.valueOf(jsonField), (CharSequence) jsonField2);
                break;
        }

        return result;
    }

    public int compare(Object field, String value) {
        return new Double(String.valueOf(field)).compareTo(Double.parseDouble(value));
    }

    public Object getField(String key) {
        if (key.indexOf(".") >= 0) {
            return getNestedField(key);
        }
        return json.get(key);
    }

    @SuppressWarnings("unchecked")
    private Object getNestedField(String key) {
        String[] parts = key.split("\\.");
        Map<String, Object> value = null;
        for (int i = 0; i < parts.length - 1; i++) {
            value = (Map<String, Object>) json.get(parts[i]);
        }
        return value.get(parts[parts.length - 1]);
    }
}