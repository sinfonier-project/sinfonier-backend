package com.sinfonier.drains;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sinfonier.util.JSonUtils;

//@formatter:off
/**
* Ducksboard Map drain.
* <p> XML Options:<br/>
* <ul>
* <li> <b>{@code <sources> <source> <sourceId></sourceId> <grouping field="field"></grouping> </source> ... </sources>}</b> - Needed. Sources where this bolt must receive tuples. </li>
* <li> <b>{@code <apiKey></apiKey>}</b> - Needed. DucksBoardApiKey. </li>
* <li> <b>{@code <latField></latField> }</b> - Needed. Name of field which contains latitude. </li>
* <li> <b>{@code <lonField></lonField>}</b> - Needed. Name of field which contains longitude. </li>
* <li> <b>{@code <id></id>}</b> - Needed. Name of field which contains longitude. </li>
* <li> <b>{@code <numTasks></numTasks>}</b> - Optional. Num tasks of this bolt. </li>
* <li> <b>{@code <paralellism>1</paralellism>}</b> - Needed. Parallelism. </li>
* </ul>
*/
//@formatter:on
public class DucksBoardMap extends BaseSinfonierDrain {

    private static final long serialVersionUID = 3832712827919157844L;

    private static final String PUSHHOST = "https://push.ducksboard.com/v/";

    private String latField;
    private String lonField;
    private String apiKey;
    private String widgetId;
    private List<String> jsonFields;

    public DucksBoardMap(String xmlFile) {
        super(xmlFile);
    }

    @Override
    public void userprepare() {
        latField = getParam("latField", true);
        lonField = getParam("lonField", true);
        apiKey = getParam("apiKey", true);

        List<Object> fields = getParamList("field");
        jsonFields = new ArrayList<String>();

        for (Object field : fields) {
            jsonFields.add(((String) field).trim());
        }

        widgetId = getParam("id", true);
    }

    @Override
    public void userexecute() {

        Map<String, Object> json = new LinkedHashMap<String, Object>();
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("latitude", getField(latField));
        value.put("longitude", getField(lonField));

        for (String field : jsonFields) {
            value.put(field, getField(field));
        }

        json.put("value", value);
        JSonUtils.sendPostDucksBoard(json, getPushUrl(widgetId), apiKey);
    }

    @Override
    public void usercleanup() {

    }

    protected String getPushUrl(String label) {
        return PUSHHOST + label;
    }
}
