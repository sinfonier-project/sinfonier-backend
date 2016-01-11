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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//@formatter:off
/**
* Trim an entity depending on action parameter.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <action></action>}</b> - Optional. Values: Allows, Delete. Default Allows. </li>
* <li> <b>{@code <field></field><field>...</field>}</b> - Needed. One label for each field.</li>
* <li> <b>{@code <entity></entity>}</b> - Optional. Entity, in case you want to change it. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt.</li>
* <li> <b>{@code <paralellism>1</paralellism> }</b> -Needed. Parallelism.</li>
* </ul>
*/
//@formatter:on
public class Trim extends BaseSinfonierBolt {

    private List<String> list;
    private boolean allows = true;
    private String entityName = "";

    public Trim(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        String action = getParam("action").trim();
        if (action.equalsIgnoreCase("delete")) {
            allows = false;
        }

        List<Object> fields = getParamList("field");
        list = new ArrayList<String>();
        for (Object field : fields) {
            list.add(((String) field).trim());
        }
    }

    @Override
    public void userexecute() {

        trimSelectedFields(getJson());
        emit();
    }

    public void trimSelectedFields(Map<String, Object> json) {

        // Iterate over map (json)
        for (Iterator<Map.Entry<String, Object>> it = json.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Object> entry = it.next();

            if (entry.getValue() instanceof Map) {
                entityName = addEntity(entry.getKey());
                trimSelectedFields((Map<String, Object>) entry.getValue());
                if (((Map<String, Object>) entry.getValue()).size() == 0) {
                    it.remove();
                }
            } else { // Leaf key
                checkOptionsAndRemove(entry.getKey(), it);
            }
        }

        entityName = resetEntityName();
    }

    public String resetEntityName() {
        int n = entityName.lastIndexOf(".");
        return (n == -1) ? "" : entityName.substring(0, n);
    }

    public void checkOptionsAndRemove(String key, Iterator<Map.Entry<String, Object>> it) {
        if (isAllows() && !contains(list, key)) {
            it.remove();
        } else if (isDelete() && contains(list, key)) {
            it.remove();
        }
    }

    public boolean contains(List<String> keyList, String key) {
        String currentKey = key;
        for (String listKey : keyList) {
            currentKey = "".equals(entityName) ? key : entityName + "." + key;
            if (listKey.equals(currentKey)) {
                return true;
            }

        }

        return false;
    }

    private String addEntity(String newEntity) {
        if (entityName.equals("")) {
            return newEntity;
        } else {
            return entityName + "." + newEntity;
        }
    }

    @Override
    public void usercleanup() {
    }

    /**
     * Check if action parameter is Allows.
     * 
     * @return {@code true} if is Allows.
     */
    public boolean isAllows() {
        return allows;
    }

    /**
     * Check if action parameter is Delete.
     * 
     * @return {@code true} if is Delete.
     */
    public boolean isDelete() {
        return !allows;
    }

}
