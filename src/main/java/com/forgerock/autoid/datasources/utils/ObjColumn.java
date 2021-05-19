package com.forgerock.autoid.datasources.utils;

import java.io.Serializable;

public class ObjColumn implements Serializable {

    private static final long serialVersionUID = -5551665968305328923L;
    private String columnName;
    private String columnValue;

    public ObjColumn(String name,String value){
        this.columnName = name;
        this.columnValue = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }
}
