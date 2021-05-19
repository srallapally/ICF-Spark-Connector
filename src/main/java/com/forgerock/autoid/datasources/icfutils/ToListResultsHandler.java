package com.forgerock.autoid.datasources.icfutils;
import java.util.ArrayList;
import java.util.List;

import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ResultsHandler;

public class ToListResultsHandler implements ResultsHandler {

    private final List<ConnectorObject> connectorObjects = new ArrayList<ConnectorObject>();

    public boolean handle(ConnectorObject object) {
        connectorObjects.add(object);
        return true;
    }

    public List<ConnectorObject> getObjects() {
        return connectorObjects;
    }
}

