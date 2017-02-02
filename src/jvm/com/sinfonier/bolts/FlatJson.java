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
import java.util.Iterator;
import java.util.Map;

//@formatter:off
/**
* Flat a JSon. Change structure of Json to a flat json. 
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <entity></entity>}</b> - Optional. Entity, in case you want to change it. </li>
* <li> <b>{@code <separator></separator>}</b> - Optional. Default: _  Not possible "." (dot).</li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt.</li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism.</li>
* </ul>
*/
//@formatter:on
public class FlatJson extends BaseSinfonierBolt {

    private char delimiter = '_';
    private String entityName = "";
    private Map<String, Object> flatJson;

    public FlatJson(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        String paramDelimiter = getParam("separator");
        if (null != paramDelimiter && !paramDelimiter.equals(".") && paramDelimiter.length() < 2) {
            delimiter = paramDelimiter.charAt(0);
        }
    }

    @Override
    public void userexecute() {

        flatJson = new HashMap<String, Object>();
        Map<String, Object> json = getJson();
        flatJson(json);
        setJson(flatJson);
        emit();
    }

    @Override
    public void usercleanup() {

    }

    public void flatJson(Map<String, Object> json) {
        for (Iterator<Map.Entry<String, Object>> it = json.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Object> entry = it.next();
            if (entry.getValue() instanceof Map) {
                entityName = addEntity(entry.getKey());
                json = (Map<String, Object>) entry.getValue();
                flatJson(json);
            } else {
                flatJson.put(getFullKey(entry.getKey()), entry.getValue());
            }
        }

        entityName = resetEntityName();
    }

    public String resetEntityName() {
        int n = entityName.lastIndexOf(".");
        return (n == -1) ? "" : entityName.substring(0, n);
    }

    private String addEntity(String newEntity) {
        if (entityName.equals("")) {
            return newEntity;
        } else {
            return entityName + delimiter + newEntity;
        }
    }

    private String getFullKey(String key) {
        if (entityName.equals("")) {
            return key;
        } else {
            return entityName + delimiter + key;
        }
    }
}
