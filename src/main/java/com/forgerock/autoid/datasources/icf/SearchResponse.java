package com.forgerock.autoid.datasources.icf;

import org.identityconnectors.framework.common.objects.ConnectorObject;

import java.util.List;

public class SearchResponse {
    private List<ConnectorObject> connectorObjects;
    private String pagedResultsCookie;

    public SearchResponse (String pagedResultsCookie,List<ConnectorObject> connectorObjects){
        this.pagedResultsCookie = pagedResultsCookie;
        this.connectorObjects = connectorObjects;
    }
    public void setPagedResultsCookie(String pagedResultsCookie) {
        this.pagedResultsCookie = pagedResultsCookie;
    }

    public void setConnectorObjects(List<ConnectorObject> connectorObjects) {
        this.connectorObjects = connectorObjects;
    }

    public List<ConnectorObject> getConnectorObjects() {
        return connectorObjects;
    }

    public String getPagedResultsCookie() {
        return pagedResultsCookie;
    }
}