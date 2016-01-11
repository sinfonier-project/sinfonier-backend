#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import storm
import json
import sys
import xml.etree.ElementTree as ET

class BaseSinfonierSpout(storm.Spout):

  def __init__(self):

    pass

  def log(self,msg):

    storm.log(msg)

  def getParam(self,param):

    if param in self.config.keys():
        
        return self.config[param]
    
    else:
        
        return ""

  def initialize(self, conf, context):

        self.d = dict()
        self.config = dict()
        self.entity = ""
        self.xmlPath = str(sys.argv[1])
        self.moduleid = str(sys.argv[3])

        excludedparams = ["spout","parallelism","properties","forceFromStart","startOffsetTime"]
        
        try:
            # Parse all XML file to tree variable
            tree = ET.parse(self.xmlPath)
            
            # Get all spout elements from tree
            for elt in tree.getiterator("spout"):
                # context["-1"] is drainId of this drain
                if context["task->component"][self.moduleid] == elt.get("abstractionId"):
                  # Get subelements in spout element
                  for subelt in elt.iter():
                    # If subelement is not in excluded params
                    if subelt.tag not in excludedparams:
                      
                      # Si el subelemento tiene hijos, entrará en el if
                      # Esto se da en el caso de los keyValue
                      if len(subelt):
                        self.config[subelt.tag] = list()
                        for item in subelt:
                          if item.tag == "keyValue":
                            self.config[subelt.tag].append({item.find("key").text : item.find("value").text})
                          elif item.tag == "keyValueDefault":
                            self.config[subelt.tag].append({item.find("key").text : item.find("value").text, item.find("key").text+"default" : item.find("default").text})
                      else:
                        
                        # If subelement is already processed
                        if subelt.tag in self.config.keys():
                        
                          # Si ya existe la clave y no es una lista, 
                          # lo convertimos a lista y añadimos el nuevo
                          if type(self.config[subelt.tag]) is not list:
                              last = self.config[subelt.tag]
                              self.config[subelt.tag] = list()
                              self.config[subelt.tag].append(last)
                          
                          self.config[subelt.tag].append(subelt.text)

                        else:
                          self.config[subelt.tag] = subelt.text

                      # Se hace un clear del subelt para que no procese de nuevo a sus hijos
                      subelt.clear()

        except Exception, e:
            self.config["error"] = str(e)

        self.useropen()
        
  def emit(self):

    # We always emit tuple = (entity, "{json string}")
    storm.emit([self.entity,json.dumps(self.d)])

  def usernextTuple(self):
    
    raise NotImplementedError("update: This method must be implemented in your class")

  def useropen(self):

    raise NotImplementedError("update: This method must be implemented in your class")

  def nextTuple(self):
    
    self.usernextTuple()
    self.d = dict()

  def addField(self,s,o):
    self.d[s] = o

  def getField(self,s):
    return self.getNestedField(s) if "." in s else self.d[s]

  def removeField(self,s):
    del self.d[s]

  def getNestedField(self,s):
    
    value = self.d.copy()
    for part in s.split("."):
      if type(value[part]) == "dict":
        value = json.loads(value[part])
      else:
        value = value[part]
    return value
  
  '''
  def removeNestedField(self,s):
    
    value = self.d.copy()
    aux = value
    parts = s.split(".")
    if len(parts) == 1:
      aux[parts[0]]
    else:
      for i in range(len(parts)-1):
        value = value[parts[i]]
      del value[parts[len(parts)-1]]
    return aux 
  '''

  def existsField(self,s):
    return True if s in self.d.keys() else None
