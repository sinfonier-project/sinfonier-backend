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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.sinfonier.exception.FilterBoltException;

//@formatter:off
/**
* Filter an entity. 
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <action></action>}</b> - Optional. Values: Permit, Block. Default Permit.</li>
* <li> <b>{@code <match></match>}</b> - Needed. Values: Any, All. Other value => Default All. </li>
* <li> <b>{@code <conditions> <condition> <field></field><operator></operator><value></value> </condition> .. </conditions>}</b> - Needed. Filters. </li>
* <li> <b>{@code <entity></entity>}</b> - Optional. Entity, in case you want to change it. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt. </li>
* <li> <b>{@code <paralellism>1</paralellism> }</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class Filter extends BaseSinfonierBolt {

    private static final long serialVersionUID = -1468919746895350525L;
    private boolean permit = true;
    private boolean all = true;
    private List<Map<String, Object>> conditions;

    public Filter(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        String action = getParam("action").trim();
        if (action.equalsIgnoreCase("block")) {
            permit = false;
        }
        String match = getParam("match", true).trim().toLowerCase();
        if (match.equalsIgnoreCase("any")) {
            all = false;
        }

        conditions = getComplexProperty("conditions");
    }

    @Override
    public void userexecute() {
        boolean emit = isPermit() ? processPermit() : processBlock();

        if (emit) {
            emit();
        }
    }

    @Override
    public void usercleanup() {

    }

    // Possible operators, contains, does not contain, matches regex, >, <, ==,
    @SuppressWarnings("rawtypes")
    private boolean doComparation(Map<String, Object> condition) {
        String field = (String) condition.get("field");
        String operator = (String) condition.get("operator");
        String value = String.valueOf(condition.get("value"));

        Object jsonField = getField(field);

        if (jsonField == null) {
            return false;
        }

        boolean result = false;

        switch (operator) {
        case "<":
            result = compare(jsonField, value) < 0;
            break;
        case "<=":
            result = compare(jsonField, value) <= 0;
            break;
        case ">":
            result = compare(jsonField, value) > 0;
            break;
        case ">=":
            result = compare(jsonField, value) >= 0;
            break;
        case "==":
            result = jsonField.equals(value);
            break;
        case "!=":
            result = !jsonField.equals(value);
            break;
        case "match": // Matches regex
            result = Pattern.matches((String) getJson().get(field), value);
            break;
        case "containsText":
            result = ((String) jsonField).contains(value);
            break;
        case "contains":
            if (jsonField instanceof Collection) {
                result = ((Collection) jsonField).contains(value);
            } else if (jsonField instanceof String) {
                result = ((String) jsonField).contains(value);
            } else {
                throw new FilterBoltException("Cannot check contains in field "
                        + getJson().get(field));
            }
            break;
        case "notcontains":
            if (jsonField instanceof Collection) {
                result = !((Collection) jsonField).contains(value);
            } else if (jsonField instanceof String) {
                result = !((String) jsonField).contains(value);
            } else {
                throw new FilterBoltException("Cannot check contains in field " + jsonField);
            }
            break;
        }

        return result;
    }

    public int compare(Object field, String value) {
        return new Double(String.valueOf(field)).compareTo(Double.parseDouble(value));
    }

    /**
     * Process Permit filter.
     *
     * @return {@code true} if current tuple must be emitted.
     */
    public boolean processPermit() {
        boolean emit = isAll();

        for (Map<String, Object> condition : conditions) {
            boolean result = doComparation(condition);
            if (isAll() && !result) {
                emit = false;
                break;
            } else if (isAny() && result) {
                emit = true;
                break;
            }
        }
        return emit;
    }

    /**
     * Process Block filter.
     *
     * @return {@code true} if current tuple must be emitted.
     */
    public boolean processBlock() {
        boolean emit = !isAll();

        for (Map<String, Object> condition : conditions) {
            boolean result = doComparation(condition);
            if (isAll() && !result) {
                emit = true;
                break;
            } else if (isAny() && result) {
                emit = false;
                break;
            }
        }
        return emit;
    }

    public boolean isPermit() {
        return permit;
    }

    public boolean isBlock() {
        return !permit;
    }

    public boolean isAll() {
        return all;
    }

    public boolean isAny() {
        return !all;
    }
}
