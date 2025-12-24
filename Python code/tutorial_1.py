data = spark.table("ecom.default.big_mart_sales")
display(data)

data.printSchema()

my_ddl_schema = '''
                Item_Identifier string
                Item_Weight double
                Item_Fat_Content string
                Item_Visibility double
                Item_Type string
                Item_MRP  double
                Outlet_Identifier string
                Outlet_Establishment_Year long
                Outlet_Size string
                Outlet_Location_Type string
                Outlet_Type string
                Item_Outlet_Sales double
'''

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv') 

data.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content').display()

from pyspark.sql.functions import col
data.filter(col('Item_Fat_Content') == 'Regular').display()

data.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10) ).display()

data.filter(
    (col("Outlet_Size").isNull()) &
    (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
).display()

data.withColumnRenamed('Item_Weight', 'Weight').display()

data = data.withColumn('Total_Sales',col('Item_MRP') * col('Item_Outlet_Sales')).display()

from pyspark.sql.functions import col, regexp_replace
data.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'Reg')).display()


data.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF')).display()


from pyspark.sql.functions import col
from pyspark.sql.types import StringType
data = data.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))
data.printSchema()


from pyspark.sql.types import StringType
data = data.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))
data.printSchema()

data.sort(col('Item_Weight').asc()).display()

data.limit(5).display()

data.drop('Item_Visibility').display()

data.display()

from pyspark.sql.functions import col, initcap
data.select(initcap('Item_Type')).display()

from pyspark.sql.functions import col, initcap, upper, lower
data.select(upper('Item_Type')).display()

from pyspark.sql.functions import col, initcap, upper, lower
data.select(lower('Item_Type')).display()

