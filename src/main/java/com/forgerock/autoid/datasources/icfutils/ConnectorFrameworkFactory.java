package com.forgerock.autoid.datasources.icfutils;
import org.identityconnectors.framework.api.*;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.impl.api.ConnectorInfoManagerFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class ConnectorFrameworkFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConnectorFrameworkFactory.class);
    protected static final String ERROR_MESSAGE = "ConnectorFramework has been acquired";
    private static ClassLoader defaultConnectorBundleParentClassLoader = null;
    private String libDir;
    private static ConnectorInfoManagerFactoryImpl factory = null;
    private static ConnectorInfoManager connectorInfoManager = null;
    private final List<ConnectorFacade> facadeList = new ArrayList();
    private final List<ConnectorInfo> connectorInfoList = new ArrayList();
    private final List<ConnectorInfoManager> connectorInfoManagerList = new ArrayList();
    public ConnectorFrameworkFactory(String dir){
        this.libDir = dir;
        try {
            this.defaultConnectorBundleParentClassLoader = buildLibClassLoader();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    protected ClassLoader getDefaultConnectorBundleParentClassLoader() {
        return defaultConnectorBundleParentClassLoader != null ? defaultConnectorBundleParentClassLoader
                : ConnectorInfoManagerFactory.class.getClassLoader();
    }

    public ConnectorFrameworkFactory setDefaultConnectorBundleParentClassLoader(
            final ClassLoader defaultConnectorBundleParentClassLoader) {
        ClassLoader key = defaultConnectorBundleParentClassLoader;
        if (this.defaultConnectorBundleParentClassLoader == null) {
            this.defaultConnectorBundleParentClassLoader = key;
        } else {
            throw new IllegalStateException(ERROR_MESSAGE);
        }
        return this;
    }

    public ConnectorFacade newConnectorFacadeInstance(final APIConfiguration config) {
        ConnectorFacade facade = ConnectorFacadeFactory.getInstance().newInstance(config);
        facadeList.add(facade);
        //System.out.println("I have "+facadeList.size()+" objects");
        return facade;
    }

    public ConnectorInfo findConnectorInfo(String  connectorName,
                                    String version) {
        //System.out.println("Looking for: "+connectorName+" with version: "+version);
        ConnectorInfoManager manager = getConnectorInfoManager();
        for (ConnectorInfo info : manager.getConnectorInfos()) {
            ConnectorKey key = info.getConnectorKey();
            //System.out.println("Key:"+key.getConnectorName()+" Version: "+key.getBundleVersion());
            if (version.equals(key.getBundleVersion())
                    && connectorName.equals(key.getConnectorName())) {
                // intentionally inefficient to test more code
                connectorInfoList.add(info);
                return manager.findConnectorInfo(key);

            }
        }
        return null;
    }

    public ConnectorInfoManager getConnectorInfoManager() {
        if(this.connectorInfoManagerList.size() == 0) {
            try {
                factory = (ConnectorInfoManagerFactoryImpl) ConnectorInfoManagerFactory.getInstance();
                connectorInfoManager = factory.getLocalManager(getBundleURLs(), defaultConnectorBundleParentClassLoader);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            connectorInfoManagerList.add(connectorInfoManager);
            return connectorInfoManager;
        }
        return this.connectorInfoManagerList.get(0);
    }

    List<URL> getBundleURLs() throws MalformedURLException {
        List<URL> rv = getJarFiles();
        if (rv.isEmpty()) {
            System.out.println("No bundles found in the bundles directory");
        }
        return rv;
    }

    ClassLoader buildLibClassLoader() throws MalformedURLException {
        List<URL> jars = getJarFiles();
        if (!jars.isEmpty()) {
            return new URLClassLoader(jars.toArray(new URL[jars.size()]),
                    ConnectorInfoManagerFactory.class.getClassLoader());
        }
        return null;

    }

    List<URL> getJarFiles() throws MalformedURLException {
        File jarDir = new File(this.libDir);
        if (!jarDir.isDirectory()) {
            throw new ConnectorException(jarDir.getPath() + " does not exist");
        }
        List<URL> rv = new ArrayList<URL>();
        for (File bundle : jarDir.listFiles()) {
            //System.out.println(bundle.getName());
            if (bundle.getName().endsWith(".jar")) {
                rv.add(bundle.toURI().toURL());
            }
        }
        return rv;
    }


}
