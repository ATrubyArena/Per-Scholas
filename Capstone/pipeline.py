
import json

from pyspark.sql import SparkSession
from rich import print
import requests
from constants import LOAN_URL
from pyspark.sql.functions import concat, lit, substring, lpad, col, initcap, lower, trim, regexp_replace, when, length


def extract(spark):  # Creating an extract function to read the JSON files as dataframes.

    branch_df = spark.read.option("multiline", "true").json(r"C:\Users\alexander.arena\OneDrive - PeopleShores PBC\demo_repo\Capstone\cdw_sapp_branch.json")
    credit_df = spark.read.option("multiline", "true").json(r"C:\Users\alexander.arena\OneDrive - PeopleShores PBC\demo_repo\Capstone\cdw_sapp_credit.json")
    customer_df = spark.read.option("multiline", "true").json(r"C:\Users\alexander.arena\OneDrive - PeopleShores PBC\demo_repo\Capstone\cdw_sapp_customer.json")

    # Fetch loan data from API
    response = requests.get(LOAN_URL)

    # Check status code
    print(f"Loan Data API Status Code: {response.status_code}")
    if response.status_code == 200: #If status code 
        # Save JSON to a file so Spark can read it
        with open("loan_data.json", "w") as f:
            json.dump(response.json(), f)

        # Load loan data into DataFrame
        loan_df = spark.read.option("multiline", "true").json(r"C:\Users\alexander.arena\OneDrive - PeopleShores PBC\demo_repo\Capstone\loan_data.json")
    else:
        print("Failed to fetch loan data.")
        loan_df = spark.createDataFrame([], schema=None)  #Creating an empty list if error occures so other data frames still get created

    return customer_df, branch_df, credit_df, loan_df

#=========================================================================================================================================================================
def transform(customer_df, branch_df, credit_df):
    #Using .withcolumn to format customer names in customer_df to their desired structer using initcap, trim to remove spaces on the end, and col to specifiy the column.
    #Convert the Name to Title Case
    customer_df = customer_df.withColumn("FIRST_NAME", initcap(trim(col("FIRST_NAME")))) #initcap is the pyspark version of .title
    
    #Convert the middle name in lower case
    customer_df = customer_df.withColumn("MIDDLE_NAME", lower(trim(col("MIDDLE_NAME"))))
    
    #Convert the Last Name in Title Case
    customer_df = customer_df.withColumn("LAST_NAME", initcap(trim(col("LAST_NAME"))))
    
    #Concatenate Apartment no and Street name of customer's Residence with comma as a seperator (Street, Apartment) 
    customer_df = customer_df.withColumn("FULL_STREET_ADDRESS",concat(trim(col("STREET_NAME")), 
                            lit(", Apt# "), trim(col("APT_NO")))).drop("APT_NO", "STREET_NAME") #dropping the APT_NO and STREET_NAME
    
    #Change the format of phone number to (XXX)XXX-XXXX
    customer_df = customer_df.withColumn(
            "CUST_PHONE_CLEAN",
            regexp_replace(col("CUST_PHONE"), r"\D", "")# Removing all non-digit characters from the phone number
        ).withColumn(
            "CUST_PHONE_FORMATTED",
            when(
                length(col("CUST_PHONE_CLEAN")) == 10, #If number has 10 digits, format as (XXX)XXX-XXXX
                concat(lit("("), substring(col("CUST_PHONE_CLEAN"), 1, 3), lit(")"),
                substring(col("CUST_PHONE_CLEAN"), 4, 3), lit("-"),
                substring(col("CUST_PHONE_CLEAN"), 7, 4))
            ).when(
                length(col("CUST_PHONE_CLEAN")) == 7, #If number has 7 digits, format as XXX-XXX-XXXX with 'XXX-' placeholder area code
                concat(lit("(XXX)"),
                substring(col("CUST_PHONE_CLEAN"), 1, 3), lit("-"),
                substring(col("CUST_PHONE_CLEAN"), 4, 4))
            ).otherwise(lit("INVALID")) #For anything not 7 or 10 digits, label as INVALID
        )
    
        #Dropping the original "CUST_PHONE" column and the column "CUST_PHONE_CLEAN" that was created for the transform process
    customer_df = customer_df.drop("CUST_PHONE", "CUST_PHONE_CLEAN") \
                         .withColumnRenamed("CUST_PHONE_FORMATTED", "CUST_PHONE") #Renaming final product "CUST_PHONE_FORMATTED" to the original column name "CUST_PHONE"
    
    #If the source value is null load default (99999) value else Direct move
    branch_df = branch_df.fillna({'branch_zip': '99999'})
    
    #Change the format of phone number to (XXX)XXX-XXXX
    branch_df = branch_df \
        .withColumn("BRANCH_PHONE_CLEAN", regexp_replace(col("BRANCH_PHONE"), r"\D", "")) \
        .withColumn("BRANCH_PHONE_FORMATTED",
            when(
                length(col("BRANCH_PHONE_CLEAN")) == 10,
                concat(
                    lit("("), substring(col("BRANCH_PHONE_CLEAN"), 1, 3), lit(")"),
                    substring(col("BRANCH_PHONE_CLEAN"), 4, 3), lit("-"),
                    substring(col("BRANCH_PHONE_CLEAN"), 7, 4)
                )
            )
        )
        # Dropping the original "BRANCH_PHONE" column and the column "BRANCH_PHONE_CLEAN" that was created for the transform process
    branch_df = branch_df.drop("BRANCH_PHONE", "BRANCH_PHONE_CLEAN") \
                .withColumnRenamed("BRANCH_PHONE_FORMATTED", "BRANCH_PHONE") #Renaming final product "BRANCH_PHONE_FORMATTED" to the original column name "BRANCH_PHONE"
    
    # Renaming CREDIT_CARD_NO column to CUST_CC_NO per mapping document
    credit_df = credit_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

    #Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD)
    credit_df = credit_df.withColumn("TIMEID", #Creates Column "DATE"
                concat(col("YEAR").cast("string"), # Concatinating "YEAR" "MONTH" and "DAY"
                lpad(col("MONTH").cast("string"), 2, "0"), #Left padding the "MONTH" column with "0" until it is 2 elements long. 
                lpad(col("DAY").cast("string"), 2, "0") #Same process as month but for the "DAY" column
            )
        ).drop("MONTH", "DAY", "YEAR")
    return customer_df, branch_df, credit_df

#=============================================================================================================================================================================


#creating tables in SQL
def load(df, table_name):
    df.write\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone_db")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", table_name)\
    .option("user", "root")\
    .option("password", "password")\
    .mode("overwrite")\
    .save()

def etl():

    spark = SparkSession.builder\
        .appName("CDW Sapp")\
        .config("spark.jars", r"C:\Spark\jars\mysql-connector-java-8.0.23.jar")\
        .getOrCreate()
    
    customer_df, branch_df, credit_df, loan_df = extract(spark)

    customer_df.show()
    branch_df.show()
    credit_df.show()

    customer_df, branch_df, credit_df = transform(customer_df, branch_df, credit_df)

    customer_df.show()
    branch_df.show()
    credit_df.show()

    load(customer_df, "customer_data")
    load(branch_df, "branch_data")
    load(credit_df, "credit_data")
    load(loan_df, "cdw_sapp_loan_application")