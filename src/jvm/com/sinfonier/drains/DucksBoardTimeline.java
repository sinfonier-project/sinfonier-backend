package com.sinfonier.drains;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.sinfonier.util.JSonUtils;

//@formatter:off
/**
* Ducksboard Bar drain.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <apiKey></apiKey>}</b> - Needed. DucksBoardApiKey. </li>
* <li> <b>{@code <contentField></contentField>}</b> - Needed. Main content of timeline. e.g. text.</li>
* <li> <b>{@code <titleField></titleField> }</b> - Optional. </li>
* <li> <b>{@code <imageField></imageField>}</b> - Optional. Must be an URL. </li>
* <li> <b>{@code <timestampField></timestampField>}</b> - Optional. Default now(). </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt.</li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class DucksBoardTimeline extends BaseSinfonierDrain {

    private static final long serialVersionUID = 2654779834709496533L;

    private static final String PUSHHOST = "https://push.ducksboard.com/v/";

    private String apiKey;
    private String id;
    private String titleField;
    private String imageField;
    private String contentField;
    private String timestampField;

    public DucksBoardTimeline(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        apiKey = getParam("apiKey", true);
        id = getParam("id", true);
        titleField = getParam("titleField");
        imageField = getParam("imageField");
        timestampField = getParam("timestampField");
        contentField = getParam("contentField");
    }

    @Override
    public void userexecute() {
        sendPost();
    }

    @Override
    public void usercleanup() {

    }

    /**
     * Build JSON and call DucksBoard API to send information to timeline.
     */
    public void sendPost() {
        Map<String, Object> json = new HashMap<String, Object>();
        if (timestampField != null) {
            json.put("timestamp", getField(timestampField));
        } else {
            long timestamp = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() / 1000L;
            json.put("timestamp", timestamp);
        }

        Map<String, Object> value = new HashMap<String, Object>();
        if (contentField != null) {
		if (existsField(contentField)){
			value.put("content", getField(contentField).toString());
		}
	}

        if (titleField != null) {
	    if (existsField(titleField)){
            	value.put("title", getField(titleField).toString());
        	}
	}

        if (imageField != null) {
            if (existsField(imageField)){
		value.put("image", getField(imageField).toString());
        	}
	}
        json.put("value", value);

        String url = PUSHHOST + id;
        JSonUtils.sendPostDucksBoard(json, url, apiKey);
    }
}
