package it.polimi.middleware.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Covid Project
 * Input: csv files having the following
 * schema ("dateRepresented: Date, day: Int, month: Int, year: Int,
 *              cases: Int, deaths: Int, country: String, geoId: String, countryID: String,
 *              population: Int, continent: String")
 * Queries
 1. Seven days moving average of new reported cases, for each county and for each day
 2. Percentage increase (with respect to the day before) of the seven days moving average, for each
 country and for each day
 3. Top 10 countries with the highest percentage increase of the seven days moving average, for each
 day
 */

public class CovidAnalysis {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = "CovidAnalysis";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("Date", DataTypes.DateType, true));
        mySchemaFields.add(DataTypes.createStructField("Day", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("Month", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("Year", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("Cases", DataTypes.IntegerType, false));
        mySchemaFields.add(DataTypes.createStructField("Deaths", DataTypes.IntegerType, false));
        mySchemaFields.add(DataTypes.createStructField("Country", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("GeoID", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("CountryID", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("Population", DataTypes.IntegerType, true));
        mySchemaFields.add(DataTypes.createStructField("Continent", DataTypes.StringType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        final Dataset<Row> rawData = spark
                .read()
                .option("delimiter", ",")
                .option("dateFormat", "dd/MM/yyyy")
                .schema(mySchema)
                //        .csv(filePath + "spark-project.4/data/covid19data.csv"); USE THIS TO RUN IN INTELLIJ
                .csv(filePath); // USE THIS TO COMPILE AND THEN CREATE THE JAR
        //Removing rows where there is a negative or not reported number of cases
        final Dataset<Row> cleaned = rawData.filter(rawData.col("Cases").$greater$eq(0).
                and(rawData.col("Cases").isNotNull()));
        //Sorting by increasing date
        final Dataset<Row> sortedData = cleaned.sort(asc("Date"));
        //Query 1
        System.out.println("First Query");
        //Splitting the rows into groups where there is a single day and the 6 previous days of the same country
        WindowSpec window = Window.partitionBy("Country").rowsBetween(-6,0);
        //The moving average is the average of the current day's cases and the six previous values
        final Dataset<Row> mAvg = sortedData.
                withColumn("movingAverage", avg("Cases").over(window));
        mAvg.show(40);

        //Query 2
        System.out.println("Second Query");
        WindowSpec window2 = Window.partitionBy("Country").rowsBetween(-1,0);
        //Adding a new column with the value of the movingAverage of the previous day
        final Dataset<Row> previousAverage = mAvg.withColumn("previousAvg",
                first("movingAverage").over(window2));
        //Adding a new column with the difference of the two values
        final Dataset<Row> difference = previousAverage.withColumn("difference" , previousAverage.col("movingAverage")
                .$minus(previousAverage.col("previousAvg")));
        //Calculating the percentage change using the difference and the old value
        final Dataset<Row> percentageChange = difference.withColumn("percentageChange",
                difference.col("difference").$div(difference.col("previousAvg")).multiply(100));
        previousAverage.drop("GeoID").drop("CountryID").drop("Population")
                .drop("Continent").show(40);
        difference.drop("GeoID").drop("CountryID").drop("Population")
                .drop("Continent").show(40);
        percentageChange.drop("GeoID").drop("CountryID").drop("Population")
                .drop("Continent").show(40);
        //Query 3
        System.out.println("Third query");
        //Partitioning by date because we want to rank for each day and ordering by decreasing percentage change
        WindowSpec window3 = Window.partitionBy("Date").orderBy(desc("percentageChange"));
        //Adding a column with the rank in that day of each row
        final Dataset<Row> percentageRanking = percentageChange.withColumn("row", row_number().over(window3))
                .filter(col("row").$less$eq(10));
        percentageRanking.drop("GeoID").drop("CountryID").drop("Population")
                .drop("Continent").drop("Deaths").show(4000);
    }

}
