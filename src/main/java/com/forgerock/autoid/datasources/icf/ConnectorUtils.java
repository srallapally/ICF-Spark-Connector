package com.forgerock.autoid.datasources.icf;

import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConfigurationProperties;
import org.identityconnectors.framework.api.ConfigurationProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ConnectorUtils {

    private ConnectorUtils(){}
    public static void configureDefaultAPIConfiguration (HashMap<String,String> props, APIConfiguration apiConfig){
        ConfigurationProperties configProps = apiConfig.getConfigurationProperties();
        configureProperties(props,configProps);
    }
    private static void configureProperties(HashMap<String,String> props, ConfigurationProperties configProps ) {
        List<String> propertyNames = configProps.getPropertyNames();
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (!propertyNames.contains(e.getKey())) {
                /*
                 * The connector's Configuration does not define this property.
                 */
                continue;
            }
            ConfigurationProperty property = configProps.getProperty(e.getKey());
            property.setValue(e.getValue());

        }
    }
}
