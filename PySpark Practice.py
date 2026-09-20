# Databricks notebook source
# PySpark Practice
# Environment: Databricks
# Dataset: BigMart Sales

# **Data Reading**

data_json=spark.read.json("/Volumes/workspace/default/pyspark_data/drivers.json")

data_json.display()

data_json.printSchema()

# **Data Reading**

data=spark.read.csv("/Volumes/workspace/default/pyspark_data/BigMart Sales.csv",header=True,inferSchema=True)

data.display()

type(data)

# **Schema Defination**

data.printSchema()

# **DDL Commands**

ddl_schema="""
            Item_Identifier string,
            Item_Weight string,
            Item_Fat_Content string,
            Item_Visibility double,
            Item_Type string,
            Item_MRP double,
            Outlet_Identifier string,
            Outlet_Establishment_Year integer,
            Outlet_Size string
            """
            
data=spark.read.schema(ddl_schema).csv("/Volumes/workspace/default/pyspark_data/BigMart Sales.csv",header=True,inferSchema=True)

data.display()

data.printSchema()

 # **Structype Schema**

from pyspark.sql.types import *
from pyspark.sql.functions import *

struct_schema = StructType([
                    StructField("Item_Identifier",StringType(), True),
                    StructField("Item_Weight",StringType(), True),
                    StructField("Item_Fat_Content",StringType(), True),
                    StructField("Item_Visibility",StringType(), True),
                    StructField("Item_Type",StringType(), True),
                    StructField("Item_MRP",StringType(), True),
                    StructField("Outlet_Identifier",StringType(), True),
                    StructField("Outlet_Establishment_Year",StringType(), True),
                    StructField("Outlet_Size",StringType(), True)
                    ]) 

data =spark.read.schema(struct_schema).csv("/Volumes/workspace/default/pyspark_data/BigMart Sales.csv",header=True,inferSchema=True)

data.printSchema()

 # **Select**

data.display()

data_sel=data.select("Item_Fat_Content","Item_Weight","Item_Fat_Content")
data_sel.display()

 # **Alias**

data.select(col("Item_Identifier").alias("Item_Id")).display()

data.display()

 # **Filter**

 # **Scenario 1**

data.filter(col("Item_Fat_Content")=="Regular").display()

data=spark.read.csv("/Volumes/workspace/default/pyspark_data/BigMart Sales.csv",header=True,inferSchema=True)

type(data)

 # **Scenario 2**

data.filter((col("Item_Type")=="Soft Drinks") & (col("Item_Weight") <10)).display()

 # **Scenario 3**

data.filter((col("Outlet_Size").isNull()) & (col("Outlet_Location_Type").isin("Tier 1","Tier 2"))).display()

 # **withColumnRenamed**

data.withColumnRenamed("Item_Weight","Item_Wt").display()

 # **WithColumn

data.withColumn("New_col",lit("same")).display()

type(data)

data = data.withColumn("Sales * MRP",col("Item_Outlet_Sales")*col("Item_MRP"))

data.display()

data.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Regular","Reg")).withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Low Fat","Lf")).display()

 # **Type Casting**

data=data.withColumn("Item_Weight",col("Item_Weight").cast(StringType()))

data.printSchema()

 # **Sort**

data.sort(col("Item_Weight").desc()).display()

data.sort(col("Item_Visibility")).display()

data.sort(["Item_Weight", "Item_Visibility"],ascending=[0,0]).display()

data.sort(["Item_Weight", "Item_Visibility"],ascending=[0,1]).display()

 # **Limit**

data.limit(10).display()

 # **Drop**

data.drop("Item_Visibility").display()

 # **drop multiple cols**

data.drop("Item_Visibility","Item_Type").display()

 # **dropDuplicates**

data.dropDuplicates().display()

data.dropDuplicates(subset=['Item_Type']).display()

data.distinct().display()

type(data)

 # **Union**

 # **Preparing Dataframes**

data1 = [('1','kad'),
         ('2', 'sid')]
schema1='id string,name string'
df1=spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
         ('4', 'jas')]
schema2='id string,name string'
df2=spark.createDataFrame(data2,schema2)

df1.display()

df2.display()

df1.union(df2).display()

data1 = [('kad','1'),
         ('sid','2')]
schema1='name string, id string'

df1=spark.createDataFrame(data1,schema1)
df1.display()

df1.union(df2).display()

 # **Union by name**

df1.unionByName(df2).display()

 # **String Functions**

 # **Initcap()**

data.select(initcap(col('Item_Type'))).display()

data.select(upper(col('Item_Type'))).display()

 # **Date Functions()**

data=data.withColumn('curr_date',current_date())

data.display()

data=data.withColumn('week_after',date_add('curr_date',7))

data.display()

data=data.withColumn('date_diff',datediff('week_after','curr_date'))

data.display()

data.withColumn('week_after',date_format('week_after','dd-MM-yyyy'))

data.display()

 # **Handling Nulls**

data.dropna('all').display()

data.fillna('NotAvailable').display()

data.fillna('NotAvailable',subset=["Outlet_Size"]).display()

data.dropna(subset=['Outlet_Size']).display()

data.dropna('any').display()

data.withColumn("Item_Weight",col("Item_Weight").cast(DoubleType()))

data.printSchema()

 # **Split and Indexing**

data.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

data.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

 # **Explode**

data_explode=data.withColumn('Outlet_Type',split('Outlet_Type',' '))

data_explode.display()

data_explode.withColumn('Outlet_Type',explode('Outlet_Type')).display()

 # **Array_contains**

data_explode.display()

data_explode.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

data.display()

 # **Groupby**

data.groupBy('Item_Type').agg(sum('Item_MRP')).display()

data.groupBy("Item_Type").agg(avg('Item_MRP')).display()

data.groupby('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

 # **collect_list()**

da = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(da, schema)
df_book.display()

 # **Pivot**

data.groupBy("Item_Type").pivot("Outlet_Size").agg(avg("Item_MRP")).display()

data.withColumn('food',when(col("Item_Type")=='Meat','Non-Veg').otherwise('Veg-Food')).display()

 # **Joins**

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

df1.display()

df2.display()

df1.join(df2, df1['dept_id']== df2['dept_id'],'inner').display()

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

 # **Anti join**

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

 # **Window Function**

data.display()

from pyspark.sql.window import Window

data = data.withColumn('rowcol',row_number().over(Window.orderBy('Item_identifier'))) 

data.display()

data = data.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))) 

data.display()

data = data.withColumn('dense rank',dense_rank().over(Window.orderBy('Item_Identifier'))) 

data.display()

 # **Cummulative Sum**

data = data.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type')))
data.display()

data = data.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow)))
data.display()

data.display()

 # **User define Function**

def func(x):
    return x*x
    
my_func=udf(func)

data.withColumn('newcol',my_func('Item_MRP')).display()

 # **data writing**

data.write.format('csv').save('/Volumes/workspace/default/pyspark_data/output')

 # **append**

data.write.format('csv').mode('append').save('/Volumes/workspace/default/pyspark_data/output')

 # **Overwrite**

data.write.format('csv').mode('overwrite').save('/Volumes/workspace/default/pyspark_data/output')

 # **error**

data.write.format('csv').mode('error').save('/Volumes/workspace/default/pyspark_data/output') # ignore

data.write.format('csv').mode('ignore').save('/Volumes/workspace/default/pyspark_data/output')

 # **Parquet**

data.write.format('parquet').mode('overwrite').save('/Volumes/workspace/default/pyspark_data/output')

 # Table

data.write.format('parquet').mode('overwrite').saveAsTable('My_Table')

 # Spark SQL

data.display()

 # create temp view

data.createTempView('my_view')

 # select * from my_view

 # select * from my_view where Item_Fat_Content ="Low Fat"

data_new = spark.sql('select * from my_view where Item_Fat_Content ="Low Fat"')

data_new.display()

data.display()