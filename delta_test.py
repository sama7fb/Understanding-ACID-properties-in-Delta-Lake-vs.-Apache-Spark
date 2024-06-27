import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *

#  Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("Bank Transactions") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a spark dataframe and write as a delta table
print("Starting Delta table creation")

data = [("acc123", 100), ("acc456", 200)]
schema = StructType([
    StructField("account", StringType(), True),
    StructField("balance", IntegerType(), True)
])

sample_dataframe = spark.createDataFrame(data=data, schema=schema)
sample_dataframe.write.mode("overwrite").format("delta").save("data/delta-table")

# Read Data
print("Reading delta file ...!")

got_df = spark.read.format("delta").load("data/delta-table")
got_df.show()

# Update data in Delta
print("Update data...!")

# delta table path
deltaTable = DeltaTable.forPath(spark, "data/delta-table")
deltaTable.toDF().show()

# First add 50 to the balance of account "acc123"
deltaTable.update(
    condition=expr("account == 'acc123'"),
    set={"balance": expr("balance + 50")}
)

# Show intermediate update result
deltaTable.toDF().show()

# Then deduct 30 from the balance of account "acc123"
deltaTable.update(
    condition=expr("account == 'acc123'"),
    set={"balance": expr("balance - 30")}
)

# Show final update result
deltaTable.toDF().show()

# Transfer funds between accounts
print("Transferring funds between accounts...!")

# delta table path
deltaTable = DeltaTable.forPath(spark, "data/delta-table")
deltaTable.toDF().show()

# Amount to transfer from acc123 to acc456
transfer_amount = 50

# Deduct transfer_amount from acc123
deltaTable.update(
    condition=expr("account == 'acc123'"),
    set={"balance": expr("balance - %d" % transfer_amount)}
)

# Add transfer_amount to acc456
deltaTable.update(
    condition=expr("account == 'acc456'"),
    set={"balance": expr("balance + %d" % transfer_amount)}
)

# Show final update result
deltaTable.toDF().show()

try:
    # Begin transaction - Deduct transfer_amount from acc123
    deltaTable.update(
        condition=expr("account == 'acc123'"),
        set={"balance": expr("balance - %d" % transfer_amount)}
    )

    # Artificial failure introduced here
    raise Exception("Simulated failure after deducting from acc123 but before crediting acc456")

    # Add transfer_amount to acc456 (This part will not be executed due to the exception)
    deltaTable.update(
        condition=expr("account == 'acc456'"),
        set={"balance": expr("balance + %d" % transfer_amount)}
    )

except Exception as e:
    print("Error occurred: ", e)
    # Handle failure - rollback or corrective actions
    # In actual deployment, you might revert changes or log for manual intervention

# Show final state of the Delta table
deltaTable.toDF().show()

print("older version of data")
df_versionzero = spark.read.format("delta").option("versionAsof", 0).load("data/delta-table")
df_versionzero.show()

df_versionzero = spark.read.format("delta").option("versionAsof", 1).load("data/delta-table")
df_versionzero.show()

df_versionzero = spark.read.format("delta").option("versionAsof", 2).load("data/delta-table")
df_versionzero.show()

df_versionzero = spark.read.format("delta").option("versionAsof", 3).load("data/delta-table")
df_versionzero.show()
