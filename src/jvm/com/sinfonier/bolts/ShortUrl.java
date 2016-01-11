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
import java.net.MalformedURLException;
import java.util.Map;

import com.google.api.client.googleapis.GoogleHeaders;
import com.google.api.client.googleapis.GoogleTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonHttpContent;
import com.google.api.client.json.JsonHttpParser;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.Key;

//@formatter:off
/**
* Short URL with google shortener.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <url></url>}</b> - Needed. Url from RSS you want retrieve.</li>
* <li> <b>{@code <field></field> }</b> - Needed. Field of entity where we can find full URL. </li>
* <li> <b>{@code <saveTo></saveTo>}</b> - Needed. New field name for short url. </li>
* <li> <b>{@code <apiKey></apiKey>}</b> - Optional. If you don't fill this field, probably overtake api daily limits. </li>
* <li> <b>{@code <entity></entity>}</b> - Optional. Entity, in case you want to change it. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt. </li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class ShortUrl extends BaseSinfonierBolt {

    private String GOOGLE_SHORTENER_URL = "https://www.googleapis.com/urlshortener/v1/url";
    private String field;
    private String saveTo;

    public ShortUrl(String xmlFile) {
        super(xmlFile);
    }

    private static final long serialVersionUID = -3511469659840771128L;

    @Override
    public void userprepare() {
        field = getParam("field", true);
        saveTo = getParam("saveTo", true);
        String apiKey = getParam("apiKey");

        if (null != apiKey) {
            GOOGLE_SHORTENER_URL = "https://www.googleapis.com/urlshortener/v1/url?key=" + apiKey;
        }
    }

    @Override
    public void userexecute() {
        try {
            String longUrl = retrieveURLFromField(getJson());
            String shortUrl = shortenUrl(longUrl);
            addField(saveTo, shortUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
        emit();
    }

    private String retrieveURLFromField(Map<String, Object> json) throws MalformedURLException {
        String[] parts = field.split("\\.");
        Map<String, Object> value = json;
        for (int i = 0; i < parts.length - 1; i++) {
            if (value == null) {
                value = (Map<String, Object>) json.get(parts[i]);
            } else {
                value = (Map<String, Object>) value.get(parts[i]);
            }
        }
        return (String) value.get(parts[parts.length - 1]);
    }

    public synchronized String shortenUrl(String longUrl) throws IOException {
        HttpTransport transport = GoogleTransport.create();

        // configure the headers on the transport.
        transport.defaultHeaders = new GoogleHeaders();
        transport.defaultHeaders.put("Content-Type", "application/json");

        // configure the JSON parser on the transport.
        transport.addParser(new JsonHttpParser());

        // build the HTTP GET request and URL
        HttpRequest request = transport.buildPostRequest();
        request.setUrl(GOOGLE_SHORTENER_URL);

        // Prepare the JSON data structure.
        GenericData data = new GenericData();
        data.put("longUrl", longUrl);
        JsonHttpContent content = new JsonHttpContent();
        content.data = data;

        // Set the JSON content on the request.
        request.content = content;

        UrlShortenerResult result = null;
        try {
            // Execute the request, and parse the response using our Result class.
            HttpResponse response = request.execute();
            result = response.parseAs(UrlShortenerResult.class);
            response.parseAsString();
        } catch (HttpResponseException ex) {
            ex.response.parseAsString();
        }

        return result != null ? result.shortUrl : "";
    }

    public static class UrlShortenerResult extends GenericJson {
        @Key("id")
        public String shortUrl;
    }

    @Override
    public void usercleanup() {
    }
}
