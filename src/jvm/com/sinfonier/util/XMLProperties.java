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

package com.sinfonier.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import com.sinfonier.exception.XMLConfigException;

/**
 * Manages XML file to read the configuration
 * 
 */
public class XMLProperties {

    private XMLConfiguration xml;
    private ComponentType componentType;
    private String componentName;

    /**
     * Simple XMLProperties Constructor
     * 
     * @param xmlPath Path to XML file
     * @param type ComponentType
     */
    public XMLProperties(ComponentType type, String xmlPath) {
        this.componentType = type;
        try {
            xml = new XMLConfiguration(xmlPath);
            xml.setDefaultListDelimiter((char) 0);
            xml.setExpressionEngine(new XPathExpressionEngine());
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor
     * 
     * @param componentName Name of the component
     * @param type ComponentType
     * @param xmlPath Path to XML file
     */
    public XMLProperties(String componentName, ComponentType type, String xmlPath) {
        this.componentName = componentName;
        this.componentType = type;
        try {
            xml = new XMLConfiguration(xmlPath);
            xml.setDefaultListDelimiter((char) 0);
            xml.setExpressionEngine(new XPathExpressionEngine());

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get a property in a String
     * 
     * @param property Property to get
     * @return property value or null if property doesn't exists
     */
    public String get(String property) {
        String result = null;
        switch (componentType) {
            case BOLT:
                result = xml.getString("bolts/bolt[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case SPOUT:
                result = xml.getString("spouts/spout[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case DRAIN:
                result = xml.getString("drains/drain[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case OPTIONS:
                result = xml.getString("options/" + property);
                break;
        }
        return result;
    }

    /**
     * Get a property in a String indicating if want to check property not null.
     * 
     * @param property Property to get
     * @param checkNotNull {@code true} if want to check not null.
     * @return value or null if property doesn't exists
     */
    public String get(String property, boolean checkNotNull) {
        return checkNotNull ? checkNotNull(property, get(property)) : get(property);
    }

    /**
     * Get a property in a Integer.
     * 
     * @param property
     * @return property value or null if property doesn't exists
     */
    public Integer getInt(String property) {
        Integer result = null;
        switch (componentType) {
            case BOLT:
                result = xml.getInt("bolts/bolt[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case SPOUT:
                result = xml.getInt("spouts/spout[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case DRAIN:
                result = xml.getInt("drains/drain[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case OPTIONS:
                result = xml.getInt("options/" + property);
                break;
        }
        return result;
    }

    /**
     * Get a property in a Integer indicating if want to check property not null.
     * 
     * @param property
     * @param checkNotNull
     * @return property value or null if property doesn't exists
     */
    public Integer getInt(String property, boolean checkNotNull) {
        return checkNotNull ? checkIntNotNull(property, getInt(property)) : getInt(property);
    }

    public List<Object> getList(String property) {
        List<Object> result = null;

        switch (componentType) {
            case BOLT:
                result = xml.getList("bolts/bolt[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case SPOUT:
                result = xml.getList("spouts/spout[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case DRAIN:
                result = xml.getList("drains/drain[@abstractionId='" + componentName + "']/"
                        + property);
                break;
            case OPTIONS:
                result = xml.getList("options/" + property);
                break;
        }
        return result;
    }

    /**
     * Get a full path from the XML in a XPathExpressionEngine and check if is null
     * 
     * @param path Path to read from XML
     * @param _class Class you expect to obtain
     * @return Object
     */
    public Object getPathNotNull(String path, Class<?> _class) {
        switch (_class.getSimpleName()) {
            case "String":
                return XMLCheckNotNull(path, xml.getString(path));
            case "Integer":
                XMLCheckNotNull(path, xml.getString(path));
                return xml.getInt(path);
            default:
                return null;
        }
    }

    /**
     * Get a full path from the XML in a XPathExpressionEngine.
     * 
     * @param path Path to read from XML
     * @param _class Class you expect to obtain
     * @return Object
     */
    public Object getPath(String path, Class<?> _class) {
        switch (_class.getSimpleName()) {
            case "String":
                return xml.getString(path);
            case "Integer":
                return xml.getInt(path);
            default:
                return null;
        }
    }

    /**
     * Get property from component.
     * 
     * @param property Property to get
     * @param componentName Name of the component
     * @param componentType Type of the component
     * @param xmlPath Path to xml file configuration
     * @return Property value or null if doesn't exists
     */
    public static String getPropertyFromComponent(String property, String componentName,
            ComponentType componentType, String xmlPath) {
        String result = null;
        XMLConfiguration xml;
        try {
            xml = new XMLConfiguration(xmlPath);
            xml.setExpressionEngine(new XPathExpressionEngine());
            switch (componentType) {
                case BOLT:
                    result = xml.getString("bolts/bolt[@abstractionId='" + componentName + "']/"
                            + property);
                    break;
                case SPOUT:
                    result = xml.getString("spouts/spout[@abstractionId='" + componentName + "']/"
                            + property);
                    break;
                case DRAIN:
                    result = xml.getString("drains/drain[@abstractionId='" + componentName + "']/"
                            + property);
                    break;
                case OPTIONS:
                    break;
            }
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get a complex property. Complex property is a label which contains several sublabels.
     * 
     * @param type {@code com.sinfonier.util.ComponentType}.
     * @param componentName Name of component.
     * @param propertyName Property name.
     * @return
     */
    public List<Map<String, Object>> getComplexProperty(ComponentType type, String componentName,
            String propertyName) {
        StringBuilder sb = new StringBuilder();
        String itemName = propertyName.substring(0, propertyName.length() - 1);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        switch (type) {
            case BOLT:
                sb.append("bolts/bolt[@abstractionId='");
                break;
            case SPOUT:
                sb.append("spouts/spout[@abstractionId='");
                break;
            case DRAIN:
                sb.append("drains/drain[@abstractionId='");
                break;
            default:
                break;
        }
        sb.append(componentName).append("']/").append(propertyName).append("/").append(itemName);

        List<HierarchicalConfiguration> items = xml.configurationsAt(sb.toString());
        for (HierarchicalConfiguration item : items) {
            Iterator<String> keys = item.getKeys();
            Map<String, Object> map = new HashMap<String, Object>();
            while (keys.hasNext()) {
                String key = keys.next();
                map.put(key, item.getProperty(key));
            }
            list.add(map);
        }

        return list;
    }

    /**
     * Check if a property from XML is null. If it's null throw an exception, in other case return
     * same value.
     * 
     * @param property Name of property you want to check.
     * @param value Value of property you want to check
     * @return the same value
     */
    public static String checkNotNull(String property, String value) {
        if (value == null || value.equals("")) {
            String message;
            if (property.startsWith("@"))
                message = "XML Error. Label property " + property + " is null or empty";
            else
                message = "XML Error. Label " + property + " is null or empty";
            throw new XMLConfigException(message);
        }
        return value;
    }

    /**
     * Check if a property from XML is null. If it's null throw an exception, in other case return
     * same value.
     * 
     * @param property Name of property you want to check.
     * @param value Value of property you want to check
     * @return the same value
     */
    public static Integer checkIntNotNull(String property, Integer value) {
        if (value == null)
            throw new XMLConfigException("Error in XML topology configuration in property "
                    + property);
        return value;
    }

    public static void checkList(String property, List<HierarchicalConfiguration> list) {
        if (list == null || list.isEmpty())
            throw new XMLConfigException("XML error. Label "
                    + property.substring(0, property.lastIndexOf('.')) + " must contains children");
    }

    /**
     * Check if XML property is null and in this case throw an Exception
     * 
     * @param property Name of the property you want to check
     * @param obj String object you want to check
     */
    public static String XMLCheckNotNull(String property, String obj) {
        if (obj == null || obj.equals(""))
            throw new XMLConfigException("Error in XML Configuration. Property " + property
                    + " null or empty");
        return obj;
    }

    /**
     * Check if XML property is null and in this case throw an Exception.
     * 
     * @param property Name of the property you want to check
     * @param list List you want to check
     */
    private static void XMLCheckListNotNull(String property, List list) {
        if (list.isEmpty())
            throw new XMLConfigException("Error in XML Configuration. Property " + property
                    + " incorrect. Please check it");
    }

    /**
     * Get Integer from xml configuration file.
     * 
     * @param xml
     * @param property
     */
    public static Integer getInt(HierarchicalConfiguration xml, String property) {
        if (xml.getString(property) == null || xml.getString(property).equals(""))
            throw new XMLConfigException("Error in XML Configuration. Property " + property
                    + " null or empty");
        return xml.getInt(property);
    }
}
