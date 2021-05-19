package com.forgerock.autoid.datasources.icfutils;

import com.forgerock.autoid.datasources.utils.ICFAttribute;
import com.forgerock.autoid.datasources.utils.ObjColumn;
import org.identityconnectors.framework.api.*;
import org.identityconnectors.framework.api.operations.SchemaApiOp;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class ConnectorDriver implements Serializable {
    private static final long serialVersionUID = 1693733605308646415L;
    private static transient Logger log = LoggerFactory.getLogger(ConnectorDriver.class);
   // private String libDir;
    private static ConnectorFrameworkFactory fwkFactory;
    private static ConnectorFacade connectorFacade = null;
    private static ConnectorInfoManager infoManager = null;
    private static ConnectorInfo connectorInfo = null;
    private static APIConfiguration apiConfiguration = null;
    private List<ArrayList> searchResults = new ArrayList<>();
    Integer i = 0;

    public ConnectorDriver(String dir) {
        fwkFactory = new ConnectorFrameworkFactory(dir);
    }

    public void configureConnector(String bundle, String version,HashMap<String,String> properties){
        connectorInfo = fwkFactory.findConnectorInfo(bundle,version);
        setConfigProperties(properties);
        getConnectorFacade();
    }
    private void setConfigProperties(HashMap<String,String> properties){
        apiConfiguration = connectorInfo.createDefaultAPIConfiguration();
        apiConfiguration.getResultsHandlerConfiguration().setEnableAttributesToGetSearchResultsHandler(true);
        List<String> propertyNames = apiConfiguration.getConfigurationProperties().getPropertyNames();
        for (String propName : propertyNames) {
            ConfigurationProperty prop = apiConfiguration.getConfigurationProperties().getProperty(propName);
        }
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (!propertyNames.contains(e.getKey())) {
                continue;
            }
            ConfigurationProperty property = apiConfiguration.getConfigurationProperties().getProperty(e.getKey());
            if(property.getType().equals(java.io.File.class)) {
                File f = new File(e.getValue());
                property.setValue(f);
            } else {
                property.setValue(e.getValue());
            }
        }
    }
    public ArrayList<ICFAttribute> getSchemaForEntity(String entityName,String keyAttribute){
         log.debug("Getting schema for "+entityName);
        System.out.println(this.getClass().getName()+"Getting schema for "+entityName);
         return getSchemaForObjectClass(new ObjectClass(entityName),keyAttribute);
    }
    public void dumpSchema(){}
    private ArrayList<ICFAttribute> getSchemaForObjectClass(ObjectClass objectclass, String keyAttribute){
        if(connectorFacade == null) {
            getConnectorFacade();
        }
        if(connectorFacade.getSupportedOperations().contains(SchemaApiOp.class)) {
            Schema schema = connectorFacade.schema();
            if (!(schema == null)) {
                ObjectClassInfo objClsInfo = schema.findObjectClassInfo(objectclass.getObjectClassValue());
                Set<AttributeInfo> attributeInfos = objClsInfo.getAttributeInfo();
                ArrayList<ICFAttribute> alist = new ArrayList();
                Iterator iter = attributeInfos.iterator();
                while (iter.hasNext()) {
                    AttributeInfo info1 = (AttributeInfo) iter.next();
                    ICFAttribute a = new ICFAttribute();
                    System.out.println(this.getClass().getName()+"::getSchemaForObjectClass:ICF Attribute: "+info1.getName());
                    if (!info1.getName().equalsIgnoreCase("__NAME__")) {
                        a.setAttributeName(info1.getName());
                        a.setAttributeType(info1.getType());
                        a.setFlags(info1.getFlags());
                        Set<AttributeInfo.Flags> flags = info1.getFlags();
                        if(flags.contains(AttributeInfo.Flags.REQUIRED)) {
                          a.setRequired(true);
                        }
                        alist.add(a);
                    } else if(info1.getName().equalsIgnoreCase("__NAME__")){
                        System.out.println(this.getClass().getName()+"::getSchemaForObjectClass: Skipping __NAME__");
                    }
                }
                System.out.println(this.getClass().getName()+"::getSchemaForObjectClass: Added "+keyAttribute);
                ICFAttribute keyAttr = new ICFAttribute();
                keyAttr.setAttributeName(keyAttribute);
                keyAttr.setAttributeType(String.class);
                keyAttr.setRequired(true);
                alist.add(keyAttr);
                return alist;
            } else {
                System.out.println("Getting schema failed");
            }
        } else {
            System.out.println("Connector doesn't support schema op");
        }
        return null;
    }

    private void getConnectorFacade(){
        connectorFacade = fwkFactory.newConnectorFacadeInstance(this.apiConfiguration);
    }
    public List<ArrayList> executeSearch(String filter, String entityName, String keyAttribute, Set<String>attributesToGet){
        if(connectorFacade == null) {
            getConnectorFacade();
        }
        ObjectClass objectClass = new ObjectClass(entityName);
        System.out.println("Size of search results before "+searchResults.size());
        handleQuery(connectorFacade,objectClass,null,keyAttribute,attributesToGet);
        System.out.println("Size of search results after "+searchResults.size());
        return this.searchResults;
    }


    private void handleQuery(ConnectorFacade facade, ObjectClass objectClass,String cookie,String keyAttribute,Set<String> attributesToGet){
        ToListResultsHandler handler = new ToListResultsHandler();
        Filter filter = null;
        OperationOptionsBuilder oob = new OperationOptionsBuilder();
        oob.setPageSize(100);
        if(cookie != null) {
            oob.setPagedResultsCookie(cookie);
        } else {
            System.out.println("Cookie was null");
        }
        SearchResult res = null;
        res = search(facade, objectClass,filter, handler, oob,keyAttribute,attributesToGet);
        do {
            res = search(facade, objectClass,filter, handler, oob,keyAttribute,attributesToGet);
            if (res == null) {
                break;
            }
            if ((cookie = res.getPagedResultsCookie()) != null) {
                oob.setPagedResultsCookie(cookie);
            }
        } while(cookie != null);
        List<ConnectorObject> objs = handler.getObjects();
        Iterator i = objs.iterator();
        while(i.hasNext()){
            ArrayList<ObjColumn> row = build((ConnectorObject) i.next(),keyAttribute,attributesToGet);
            searchResults.add(row);
        }
    }
    private SearchResult search(ConnectorFacade facade,ObjectClass objectclass,
                                 Filter filter,
                                 ToListResultsHandler handler,OperationOptionsBuilder options,
                                  String keyAttribute,
                                  Set<String> attributesToGet){
        SearchResult searchResult = facade.search(objectclass, filter,
                new ResultsHandler() {
                    public boolean handle(ConnectorObject obj) {
                        try {
                            ArrayList<ObjColumn> row = build(obj,keyAttribute,attributesToGet);
                            return handler.handle(obj);
                        } catch (Exception e) {
                            // TODO ICF needs a way to handle exceptions through the facade
                            return false;
                        }
                    }
                }, options.build());
        return searchResult;
    }

    public static ArrayList<ObjColumn> build(ConnectorObject obj, String keyAttribute,Set<String> attributesToGet){
        HashMap<String,String> nv = new HashMap<String,String>();
        ArrayList<ObjColumn> row = new ArrayList<ObjColumn>();
        String uid = obj.getUid().getUidValue();
        //System.out.println("User:"+uid);
        for(String attrName: attributesToGet){
            //System.out.println("Fetching "+attrName);
            Attribute attr = obj.getAttributeByName(attrName);
            String name = null;
            String value = null;
            name = attr.getName();
            if(!name.equalsIgnoreCase(keyAttribute)) {
                value = (String) attr.getValue().get(0);
                row.add(new ObjColumn(name,value));
                //System.out.println("Got "+name+"="+value);
            }
        }
        row.add(new ObjColumn(keyAttribute,uid));
        return row;
    }
}
