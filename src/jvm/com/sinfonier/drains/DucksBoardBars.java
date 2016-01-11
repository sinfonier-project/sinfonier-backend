package com.sinfonier.drains;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sinfonier.util.JSonUtils;

//@formatter:off
/**
* Ducksboard Bar drain.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <apiKey></apiKey>}</b> - Needed. DucksBoardApiKey. </li>
* <li> <b>{@code <bars> <bar> <label></label><id></id><field></field> </bar> .. </bars>}</b> - Needed. Bars and its options. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt.</li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class DucksBoardBars extends BaseSinfonierDrain {

    private static final long serialVersionUID = 3922006991100275139L;
    private static final String PUSHHOST = "https://push.ducksboard.com/v/";

    private String apiKey;
    private List<Map<String, Object>> bars;

    public DucksBoardBars(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        apiKey = getParam("apiKey", true);
        bars = getComplexProperty("bars");
        if (bars.size() > 5) {
            bars.subList(0, 5);
        }
    }

    @Override
    public void userexecute() {
        for (Map<String, Object> bar : bars) {
            postBar(bar);
        }
    }

    @Override
    public void usercleanup() {
    }

    public void postBar(Map<String, Object> map) {
        String label = (String) map.get("label");
        String field = (String) map.get("field");

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("value", getJson().get(field));

        String url = PUSHHOST + (String) map.get("id");
        JSonUtils.sendPostDucksBoard(json, url, apiKey);
    }
}