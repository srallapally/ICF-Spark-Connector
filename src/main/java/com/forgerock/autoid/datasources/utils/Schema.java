package com.forgerock.autoid.datasources.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema implements Serializable {
    private static final long serialVersionUID = -3222038436204304539L;
    private List<StructField> structSchema = new ArrayList<>();
    private Map<String,String> attributes;

    public Schema(){
        attributes = new HashMap();
    }

    public StructType getSparkSchema(){
        StructType structType = DataTypes.createStructType(structSchema);
        //StructType structType = getDummySchema();
        return structType;
    }

    public void setSparkSchema(ArrayList<StructField> structSchema){
        this.structSchema = structSchema;
    }
    public void add(String sourceAttribute, String col){
        attributes.put(sourceAttribute,col);
    }

    private static StructType getDummySchema(){
        StructType structType = new StructType(new StructField[]{
                new StructField("USR_DISPLAY_NAME",DataTypes.StringType,true, Metadata.empty()),
                new StructField("MANAGER_NAME",DataTypes.StringType,true, Metadata.empty()),
                new StructField("USR_DEPARTMENT",DataTypes.StringType,true, Metadata.empty()),
                new StructField("USR_KEY",DataTypes.StringType,false, Metadata.empty())
        });
        return structType;
    }
}
