from pyspark.sql import SparkSession
import datetime as dt
import matplotlib.pyplot as plt
import mysql.connector as dbconnect
from rich import print
from pyspark.sql.functions import concat, lit, substring, lpad, col, initcap, lower, trim, concat_ws

def extract():
    spark = SparkSession.builder \
        .appName("CDW Extract") \
        .getOrCreate()

    branch_df = spark.read.option("multiline", "true").json("cdw_sapp_branch.json")
    credit_df = spark.read.option("multiline", "true").json("cdw_sapp_credit.json")
    customer_df = spark.read.option("multiline", "true").json("cdw_sapp_customer.json")

    return branch_df, credit_df, customer_df

branch_df, credit_df, customer_df = extract()
#cdw_df = (branch_df, credit_df, customer_df)
#print(cdw_df)
branch_df.show()
credit_df.show()
customer_df.show()

def transform(customer_df):
    customer_df = customer_df.withColumn("MIDDLE_NAME", lower(trim(col("MIDDLE_NAME"))))
    return customer_df
transform(customer_df)
customer_df.show()