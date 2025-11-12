import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.paraquet as pq

path = "data/student.csv"
file = "sample.paraquet"

df = pd.read_csv(path)
print(df)

table = pa.Table.from_pandas(df)
pq.write_table(table, file)
print("CSV has been converted into Paraquet")
print("_____________________________________")
client = InsecureClient("http://namenode:9870",user="hdfs")
try:
    client.makedirs("/user/hdfs")
    print("Directory has been Made")
except Exception as e:
    print("Directry is Already in Local Disc")

with open(file, "rb") as f:
    client.write("/user/hdfs/sample.parquet", f, overwrite=True)
print(" 'sample.parquet' uploaded to HDFS successfully!")
def create_spark_session(app_name="SparkHDFSIntegration"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS","hdfs://namenode:9000")
        .getOrCreate()
    )
    print("______________________________________")
    print("Spark has been integrated Successfully")
    print("______________________________________")

    return spark
spark = create_spark_session()

hdfs_path = "hdfs://namenode:9000/user/hdfs/sample.parquet"

def read_student_data(spark, path):
    df = spark.read.parquet(path)
    print("Data read from HDFS:")
    df.show()
    return df
spark_df = read_student_data(spark, hdfs_path)

output_path = "hdfs://namenode:9000/user/hdfs/student_output_parquet"

def write_student_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data written successfully to {output_path}")

write_student_data(spark_df, output_path)

def run_queries(spark, df):
    df.createOrReplaceTempView("students")

    print("All students:")
    spark.sql("SELECT * FROM students").show()

    print("Total number of students:")
    spark.sql("SELECT COUNT(*) AS total_students FROM students").show()

    print("Distinct courses offered:")
    spark.sql("SELECT DISTINCT course FROM students").show()

    print("Average GPA and CGPA by course:")
    spark.sql("""
        SELECT course,
               ROUND(AVG(gpa), 2) AS avg_gpa,
               ROUND(AVG(cgpa), 2) AS avg_cgpa
        FROM students
        GROUP BY course
        ORDER BY avg_gpa DESC
    """).show()

    print("Students with GPA >= 3.5:")
    spark.sql("""
        SELECT name, roll_number, course, gpa, cgpa
        FROM students
        WHERE gpa >= 3.5
        ORDER BY gpa DESC
    """).show()

    print("Top 5 students by GPA:")
    spark.sql("""
        SELECT name, roll_number, course, gpa, cgpa
        FROM students
        ORDER BY gpa DESC
        LIMIT 5
    """).show()

    print("Year-wise student count:")
    spark.sql("""
        SELECT year, COUNT(*) AS total_students
        FROM students
        GROUP BY year
        ORDER BY year DESC
    """).show()

    print("GPA Performance Category:")
    spark.sql("""
        SELECT name,
               course,
               gpa,
               CASE
                   WHEN gpa >= 3.7 THEN 'Excellent'
                   WHEN gpa >= 3.0 THEN 'Good'
                   WHEN gpa >= 2.5 THEN 'Average'
                   ELSE 'Needs Improvement'
               END AS performance
        FROM students
        ORDER BY gpa DESC
    """).show()

    print("Average of numeric columns (auto-detected):")
    numeric_cols = [f.name for f in df.schema.fields 
                    if str(f.dataType) in ('IntegerType', 'DoubleType', 'LongType')]
    if numeric_cols:
        spark.sql(
            f"SELECT {', '.join([f'ROUND(AVG({c}), 2) AS avg_{c}' for c in numeric_cols])} FROM students"
        ).show()
    else:
        print("No numeric columns found for averaging.")

run_queries(spark, spark_df)

spark.stop()
print("Spark session stopped successfully.")