package com.sinfonier.drains;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sinfonier.exception.DucksBoardException;
import com.sinfonier.util.JSonUtils;

//@formatter:off
/**
* Ducksboard Boxes drain.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <apiKey></apiKey>}</b> - Needed. DucksBoardApiKey.  </li>
* <li> <b>{@code <boxes> <box> <label></label><id></id><field></field> </box> .. </boxes>}</b> - Needed. Boxes and its options. Three or four. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt. </li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism.  </li>
* </ul>
*/
//@formatter:on
public class DucksBoardBoxes extends BaseSinfonierDrain {

    private static final long serialVersionUID = -5255175808958749406L;
    private static final String PUSHHOST = "https://push.ducksboard.com/v/";

    private String apiKey;
    private List<Map<String, Object>> boxes;

    public DucksBoardBoxes(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        apiKey = getParam("apiKey", true);
        boxes = getComplexProperty("bars");
        if (boxes.size() < 3 || boxes.size() > 4) {
            throw new DucksBoardException(
                    "There must be THREE or FOUR boxes. DucksBoard limitation.");
        }
    }

    @Override
    public void userexecute() {
        for (Map<String, Object> box : boxes) {
            postBox(box);
        }
    }

    @Override
    public void usercleanup() {

    }

    public void postBox(Map<String, Object> map) {
        String label = (String) map.get("label");
        String field = (String) map.get("field");

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("value", getJson().get(field));

        String url = PUSHHOST + (String) map.get("id");
        JSonUtils.sendPostDucksBoard(json, url, apiKey);
    }
}
