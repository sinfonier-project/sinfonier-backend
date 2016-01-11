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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import java.io.PrintWriter;

import backtype.storm.task.TopologyContext;

public class SinfonierUtils {

    /**
     * Create a file in /var/storm/topologies/<top_name>/worker which contains in first line
     * hostname where topology is going to execute and in second line worker is goint to execute it.
     * 
     * @param topologyName Name of current topology.
     * @param context Topology Context.
     */
    public synchronized static final void broadcastWorker(String topologyName,
            TopologyContext context) {

        String filename = "/var/storm/topologies/" + topologyName + "/worker";
        try {
            File file = new File(filename);

            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
            bw.write(InetAddress.getLocalHost().getHostName() + "\n");
            bw.write("worker-" + context.getThisWorkerPort().toString() + ".log");
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
