package sn.analytics.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.io.Serializable;

/**
 * Created by Sumanth on 26/01/15.
 */
public class RowToCSV implements Function<Row, String>,Serializable {
    @Override
    public String call(Row row) throws Exception {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i <row.length();i++){
            sb.append(row.get(i)).append(",");
        }
        return sb.toString();
    }
}
