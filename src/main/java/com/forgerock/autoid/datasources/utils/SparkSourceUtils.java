package com.forgerock.autoid.datasources.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkSourceUtils implements Serializable {
    private static final long serialVersionUID = -1327439123052380149L;
    private static transient Logger log = LoggerFactory.getLogger(SparkSourceUtils.class);
    public static Schema getSparkSchema(ArrayList<ICFAttribute> attributes){
       Schema schema = new Schema();
       List<StructField> sfl = new ArrayList<>();
       Iterator i = attributes.iterator();
       while(i.hasNext()){
          ICFAttribute icfAttribute = (ICFAttribute)i.next();
          System.out.println("Attribute name: "+icfAttribute.getAttributeName());
          if(icfAttribute.isRequired()) {
              sfl.add(DataTypes.createStructField(icfAttribute.getAttributeName(), getDataType(icfAttribute.getAttributeType()), false));
          } else {
              sfl.add(DataTypes.createStructField(icfAttribute.getAttributeName(), getDataType(icfAttribute.getAttributeType()), true));
          }
       }
       //StructType structType = new StructType((StructField[]) sfl.toArray());
       schema.setSparkSchema((ArrayList<StructField>) sfl);
       return schema;
    }

    //public static Row getRow(Schema schema,)
    private static DataType getDataType(Class<?> type) {
        String typeName = type.getSimpleName();
        System.out.println("SparkSourceUtils:"+typeName);
        switch(typeName){
            case "String":
                return DataTypes.StringType;
            case "int":
            case "Integer":
                return DataTypes.IntegerType;
            case "Date":
            case "date":
                return DataTypes.DateType;
            case "Timestamp":
                return DataTypes.TimestampType;
            default:
                log.debug("Using default for type [{}]", typeName);
                return DataTypes.StringType;
        }
    }
}
