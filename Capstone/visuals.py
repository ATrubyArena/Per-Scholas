from pyspark.sql.functions import col, count, concat_ws, when
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
from pipeline import etl


customer_df, branch_df, credit_df = etl()

# Group by TRANSACTION_TYPE and count occurrences
transaction_type_count_df = credit_df.groupBy("TRANSACTION_TYPE").agg(count("*").alias("TRANSACTION_COUNT"))

# Convert to Pandas for Seaborn visualization
transaction_type_count_pd = transaction_type_count_df.toPandas()

# Sort for cleaner plot
transaction_type_count_pd.sort_values(by="TRANSACTION_COUNT", ascending=False, inplace=True)

transaction_type_count_df.show()

# Plot using Seaborn
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=transaction_type_count_pd, x="TRANSACTION_TYPE", y="TRANSACTION_COUNT", palette="Blues_d")

# Annotate each bar
for p in ax.patches:
    ax.annotate(f'{int(p.get_height())}', 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='bottom', fontsize=10, color='black')

plt.title("Transaction Count by Type")
plt.xlabel("Transaction Type")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



state_count_df = customer_df.groupBy("CUST_STATE").agg(count("*").alias("CUSTOMER_COUNT"))

# Convert to Pandas for plotting
state_count_pd = state_count_df.toPandas()

# Sort and take top 10
top_states = state_count_pd.sort_values(by="CUSTOMER_COUNT", ascending=False).head(10)

# Plot with labels
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=top_states, x="CUST_STATE", y="CUSTOMER_COUNT", palette="dark")

# Annotate each bar with the customer count
for bar in ax.patches:
    height = bar.get_height()
    ax.annotate(f'{int(height)}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),  # vertical offset
                textcoords="offset points",
                ha='center', va='bottom', fontsize=10, color='black')

plt.title("Top 10 States by Customer Count")
plt.xlabel("State")
plt.ylabel("Number of Customers")
plt.tight_layout()
plt.show()


joined_df = credit_df.join(customer_df, credit_df["CUST_SSN"] == customer_df["SSN"])

agg_df = joined_df.withColumn("CUSTOMER_NAME", concat_ws(" ", "FIRST_NAME", "LAST_NAME")) \
    .groupBy("CUSTOMER_NAME") \
    .agg(sum("TRANSACTION_VALUE").alias("TOTAL_SPENT")) \
    .orderBy("TOTAL_SPENT", ascending=False) \
    .limit(10)

# Convert to Pandas for plotting
top_customers_pd = agg_df.toPandas()

# Plotting with Seaborn
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))
sns.barplot(x='TOTAL_SPENT', y='CUSTOMER_NAME', data=top_customers_pd, palette='magma')
plt.title('Top 10 Customers by Total Transaction Amount')
plt.xlabel('Total Transaction Amount ($)')
plt.ylabel('Customer Name')
plt.tight_layout()
plt.show()

spark = SparkSession.builder.appName("Loan Approval Analysis").getOrCreate()

self_employed_df = loan_df.filter(col("Self_Employed") == "Yes")

# Filter for self-employed applicants and calculate counts
approval_stats = self_employed_df.groupBy().agg(
    count(when(col("application_status") == "Y", True)).alias("Approved"),
    count("*").alias("Total")
)

# Calculate percentage
stats_pd = approval_stats.toPandas()
approved = stats_pd["Approved"][0]
total = stats_pd["Total"][0]
percentage = (approved / total) * 100 if total else 0

# Prepare data for plotting
plot_data = {
    "Status": ["Approved", "Not Approved"],
    "Count": [approved, total - approved]
}

# Convert to DataFrame for Seaborn
import pandas as pd
plot_df = pd.DataFrame(plot_data)

# Plot
sns.set(style="whitegrid")
plt.figure(figsize=(8, 6))
sns.barplot(x="Status", y="Count", data=plot_df, palette="Blues_d")
plt.title(f"Loan Approval for Self-Employed Applicants\nApproval Rate: {percentage:.2f}%")
plt.ylabel("Number of Applications")
plt.xlabel("Loan Status")
plt.tight_layout()
plt.show()