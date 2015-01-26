package sn.analytics.query;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Created by Sumanth on 26/01/15.
 */
public class QueryEngine {
    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    private final String hadoopConf;
    private final String filePath;
    private final String outDir;
    JavaSparkContext sc;
    JavaSQLContext sqlContext;
    final String tableName = "AccessLogData";
    public QueryEngine(String hadoopConf, String filePath, String outDir) {
        this.hadoopConf = hadoopConf;
        this.filePath = filePath;
        this.outDir = outDir;
    }

    Configuration conf = new Configuration();
    public void init(){

        conf.addResource(new Path(hadoopConf+"/core-site.xml"));
        conf.addResource(new Path(hadoopConf+"/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConf+"/mapred-site.xml"));
        //initSchema();
        initSpark();
    }


    public synchronized void initSpark() {
        SparkConf conf = new SparkConf();
        //large block sizes
        //int MB_128 = 128*1024*1024; 
        //sc.hadoopConfiguration().setInt("dfs.blocksize", MB_128); 
        //sc.hadoopConfiguration().setInt("parquet.block.size", MB_128); 

        final String masterLocal="local";
        conf.setAppName("QueryEngine");
        conf.setMaster(masterLocal);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sc = new JavaSparkContext(conf);

    }

    public void loadRDD() {

        sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);
        sqlContext.parquetFile(filePath).registerAsTable(tableName);
        logger.info("Loaded RDD as table " + tableName);

    }
    
    public void launchQuery(final String queryStr){ 
        logger.debug("Launch query {}",queryStr );

        JavaSchemaRDD queryRDD = sqlContext.sql(queryStr);
        queryRDD.map(new RowToCSV()).saveAsTextFile(outDir);
        queryRDD.unpersist(false);
        //not stopping spark context
    }

    public static void main(String [] args){
        final String hadoopConf = "/hadoop/conf";
        final String filePath = "hdfs://localhost:9000/logdatastore/";
        final String outDir = "/tmp/querydata-"+ UUID.randomUUID().toString();
        final String sqlStr = "SELECT city,accessTimestamp,responseStatusCode FROM AccessLogData WHERE accessTimestamp >= 1415856960000 AND accessTimestamp <= 1415857080000";


        QueryEngine queryRunner = new QueryEngine(hadoopConf,filePath,outDir);
        queryRunner.init();
        queryRunner.loadRDD();
        queryRunner.launchQuery(sqlStr);
        logger.info("Output in directory " + outDir);

        


    }
}
