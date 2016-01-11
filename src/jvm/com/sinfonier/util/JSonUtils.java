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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JSonUtils {

    public static void sendPostDucksBoard(Map<String, Object> json, String url, String apiKey) {
        HttpClient httpClient = new DefaultHttpClient();
        Gson gson = new GsonBuilder().create();
        String payload = gson.toJson(json);

        HttpPost post = new HttpPost(url);
        post.setEntity(new ByteArrayEntity(payload.getBytes(Charset.forName("UTF-8"))));
        post.setHeader("Content-type", "application/json");

        try {
            post.setHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials(apiKey,
                    "x"), post));
        } catch (AuthenticationException e) {
            e.printStackTrace();
        }

        try {
            HttpResponse response = httpClient.execute(post);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
