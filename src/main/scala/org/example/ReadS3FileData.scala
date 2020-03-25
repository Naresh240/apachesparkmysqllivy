package org.example
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadS3FileData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Write Mysql Data as Parquet").getOrCreate()

    val employeeData=spark.read
      .option("header",true)
      .csv("s3://mysql-data-jar-bucket/employee.csv")
    employeeData.printSchema()
    employeeData.show()

    employeeData.write.format("jdbc")
      .option("url","jdbc:mysql://3.93.153.3:3306/mysqldb")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("dbtable","spark_employee_tbl")
      .option("user","naresh")
      .option("password","Naresh#240")
      .mode(SaveMode.Overwrite)
      .save()

    val dataframe_mysql=spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://3.93.153.3:3306/mysqldb")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("dbtable","spark_employee_tbl")
      .option("user","naresh")
      .option("password","Naresh#240").load()
    dataframe_mysql.printSchema()
    dataframe_mysql.show()

  }
}
