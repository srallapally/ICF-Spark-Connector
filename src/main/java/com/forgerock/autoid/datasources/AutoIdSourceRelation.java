package com.forgerock.autoid.datasources;

import com.forgerock.autoid.datasources.icf.ConnectorDriver;
import com.forgerock.autoid.datasources.utils.ObjColumn;
import com.forgerock.autoid.datasources.utils.SparkSourceUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import com.forgerock.autoid.datasources.utils.Schema;
public class AutoIdSourceRelation extends BaseRelation
        implements Serializable, TableScan {
    private static final long serialVersionUID = 7442415233171119527L;
    private static transient Logger log = LoggerFactory.getLogger(AutoIdSourceRelation.class);
    private ConnectorDriver connectorDriver;
    private SQLContext sqlContext;
    private Schema schema = null;
    private String entityName = null;
    private String keyAttribute = null;
    private Set<String> attributesToGet = null;
    private List<ArrayList> results;
    private List<ArrayList> res;
    public AutoIdSourceRelation(SQLContext sqlContext,
                                HashMap<String,String> properties,
                                Set<String> attributesToGet,
                                String keyAttribute,
                                String entityName,
                                String bundleDir,
                                String bundleName,
                                String bundleVersion){
        this.connectorDriver = new ConnectorDriver(bundleDir);
        this.sqlContext = sqlContext;
        this.entityName = entityName;
        this.keyAttribute = keyAttribute;
        this.attributesToGet = attributesToGet;
        connectorDriver.configureConnector(bundleName, bundleVersion,properties);
    }
    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    public void setSqlContext(SQLContext sqlContext){
        this.sqlContext = sqlContext;
    }
    @Override
    public StructType schema() {
        //log.debug("In schema Op");
        System.out.println(this.getClass().getName()+":In schema Op");
        if(schema == null) {
            //log.debug("schema is null so call schemaOp in ICF");
            //System.out.println(this.getClass().getName()+"schema is null so call schemaOp in ICF");
            schema = SparkSourceUtils.getSparkSchema(connectorDriver.getSchemaForEntity(entityName, keyAttribute));
        }
         return schema.getSparkSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        int rowCtr = 0;
        String[] fieldName = schema.getSparkSchema().fieldNames();

        results = connectorDriver.executeSearch(null,entityName,keyAttribute,attributesToGet);
        //results = getDummy();
        //System.out.println(this.getClass().getName()+": Resultset size is "+results.size());
        Row[] rowArray = new Row[results.size()];
        @SuppressWarnings("resource")
        JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());

        Iterator<ArrayList> it = results.iterator();
        while (it.hasNext()){
            ArrayList objrow = it.next();
            List<Object> cells = new ArrayList<>();
            for(int i = 0; i< objrow.size(); i++){
                ObjColumn o = (ObjColumn)objrow.get(i);
                //System.out.println((String)o.getColumnValue());
                cells.add((String)o.getColumnValue());
            }
            Row row = RowFactory.create(cells.toArray());
            rowArray[rowCtr] = row;
            rowCtr++;
        }

        JavaRDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList(rowArray),4);

        //JavaRDD<Row> rowRDD = sparkContext.parallelize(results).map(crow->SparkSourceUtils.getRow(schema,crow));
        return rowRDD.rdd();
    }

    private ArrayList<ArrayList> getDummy(){
        ArrayList<String> al1 = new ArrayList<>();
        ArrayList<ArrayList> al2 = new ArrayList<>();
        al1.add("Sanjay Rallapally");
        al1.add("Mary Writz");
        al1.add("Product");
        al1.add("SRALLAPA");
        al2.add(al1);
        al1 = new ArrayList<>();
        al1.add("Bob Rodgers");
        al1.add("Jeff Klein");
        al1.add("Support");
        al1.add("BRODGERS");
        al2.add(al1);
        return al2;
    }
}
