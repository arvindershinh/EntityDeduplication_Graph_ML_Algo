# EntityDeduplication_Graph_ML_Algo

ID Deduplication using Graph ML Algorithm -
Objective - To identify duplicate ID (Same person possessing more than one ID) records in master data.
Activities -
Developed clustering logic by leveraging Pyspark Dataframe for grouping IDs together based on
same entity attributes.
Developed Graph algorithm using Spark Graphx (Pregel API) for merging grouped IDs based on
common ID link between different groups.
Technology Stack - PySpark, Spark GraphX
