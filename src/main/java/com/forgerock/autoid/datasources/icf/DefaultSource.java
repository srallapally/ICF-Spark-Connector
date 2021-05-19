package com.forgerock.autoid.datasources.icf;

import com.forgerock.autoid.datasources.icfutils.ConnectorDriver;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;


import java.util.*;

public class DefaultSource implements DataSourceRegister, RelationProvider {
    private ConnectorDriver connectorDriver = null;
    private String bundleDir;
    private String propsAsJson;
    private String attributes;
    private String keyAttribute;
    private String entityName;
    private String bundleName;
    private String bundleVersion;
    private HashMap<String,String> properties;
    private Set<String> attributesToGet;
    private static transient Logger log = LoggerFactory.getLogger(DefaultSource.class);
    @Override
    public String shortName() {
        return "autoid";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        java.util.Map<String,String> parametersAsJavaMap = mapAsJavaMapConverter(parameters).asJava();
        propsAsJson = parametersAsJavaMap.get("props");
        JSONParser parser = new JSONParser();
        Object obj = null;
        try {
            obj = parser.parse(propsAsJson);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        properties  = escapeChars(obj);
        attributes = parametersAsJavaMap.get("attributesToGet");
        attributesToGet = new HashSet<String>(Arrays.asList(attributes.split(",")));
        keyAttribute = parametersAsJavaMap.get("keyAttribute");
        entityName = parametersAsJavaMap.get("entityName");
        bundleDir = parametersAsJavaMap.get("bundleDir");
        bundleName = parametersAsJavaMap.get("bundleName");
        bundleVersion = parametersAsJavaMap.get("bundleVersion");
        //connectorDriver = new ConnectorDriver(bundleDir);
        System.out.println(this.getClass().getName()+" Printing supplied input"+
                "key attribute: "+keyAttribute);
        return new AutoIdSourceRelation(sqlContext,
                                        properties,
                                        attributesToGet,
                                        keyAttribute,
                                        entityName,
                                        bundleDir,
                                        bundleName,
                                        bundleVersion);
    }

    private HashMap<String,String> escapeChars(Object o){
        HashMap<String,String> hm = (HashMap<String, String>)o;
        HashMap<String,String> ret = new HashMap();
        for (java.util.Map.Entry<String, String> entry : hm.entrySet()) {
            ret.put(entry.getKey(),StringEscapeUtils.escapeJava(entry.getValue()));
        }
        return ret;
    }
}
