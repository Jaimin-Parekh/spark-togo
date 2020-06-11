package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class DatasetUtil {

	public static Dataset<Row> changeColumnCase(Dataset<Row> ds, boolean upperCase){
		for(String colName: ds.columns()) {
			String renamed = upperCase ? colName.toUpperCase(): colName.toLowerCase();
			ds.withColumnRenamed(colName, renamed);
		}
		return ds;
	}
	
	public static boolean allValuesSame(Dataset<Row> ds, String colName, String value) {
		long result = ds.filter(functions.col(colName).notEqual(value)).count();
		return result == 0;
	}
	
	public static boolean areSchemasSame(Dataset<Row> ds1, Dataset<Row> ds2) {
		return ds1.schema().fields().equals(ds2.schema().fields());
	}
	
	/**
	 * The method checks if 2 datasets are equals.
	 * Note: This method does not work as expected if any of the dataset have duplicate rows.
	 * 
	 * @param ds1
	 * @param ds2
	 * @return
	 */
	public static boolean areDatasetsSame(Dataset<Row> ds1, Dataset<Row> ds2) {
		if(ds1.schema().fields().equals(ds2.schema().fields())) {
			return ds1.union(ds2).groupBy(colSeq(createColumns(ds1, ds1.columns()))).count().filter("count=1").count()==0;
		}
		return false;
	}
	
	public static Seq<String> colSeq(String[] columns){
		return JavaConverters.asScalaIteratorConverter(Arrays.asList(columns).iterator()).asScala().toSeq();
	}
	
	public static Seq<Column> colSeq(Column[] columns){
		return JavaConverters.asScalaIteratorConverter(Arrays.asList(columns).iterator()).asScala().toSeq();
	}
	
	public static Column[] createColumns(Dataset<Row> ds, String[] columns) {
		List<Column> cols = new ArrayList();
		for(String colName: columns) {
			cols.add(ds.col(colName));
		}
		return cols.stream().toArray(Column[]::new);
	}
	
	/**
	 * This method returns dataset not having values specified in collection notAllowedValues
	 * 
	 * @param ds
	 * @param colName
	 * @param notAllowedValues
	 * @return
	 */
	public static Dataset<Row> filterNegative(Dataset<Row> ds, String colName, List notAllowedValues) {
		return ds.filter(functions.not(functions.col(colName)).isin(notAllowedValues));
	}
}
