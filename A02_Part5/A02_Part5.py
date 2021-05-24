# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark
import pyspark.sql.functions

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, source_node):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("source", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("target", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("weight", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read' to create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", " ") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ------------------------------------------------
    # START OF YOUR CODE:
    # ------------------------------------------------

    # Remember that the entire work must be done “within Spark”:
    # (1) The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL.
    # (2) The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job.
    # (3) The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function.
    # (4) The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing.

    # Type all your code here. Use as many Spark SQL operations as needed.
    # Creates a new dataframes with two columns, cost and path. 
    # Cost is set to -1 as we have not found the max 
    resultDF = inputDF.withColumn("cost", pyspark.sql.functions.lit(-1)).withColumn("path", pyspark.sql.functions.lit("")).drop("target").drop("weight")
    resultDF = resultDF.dropDuplicates(["source"]).sort(resultDF.source)

     # Set the inital cost of the source node itself to 0
    resultDF = resultDF.withColumn("new_cost", pyspark.sql.functions.when(resultDF["source"] == source_node, 0).otherwise(-1)).drop("cost") 
    # Set the inital path of source node ot itself
    # Otherwise is "" so that the new paths can be entered into it
    resultDF = resultDF.withColumn("new_path", pyspark.sql.functions.when(resultDF["source"] == source_node, str(source_node)).otherwise("")).drop("path") 

    # Create the first set of candidate edges by getting the targets of 1
    edge_candidatesDF = inputDF.filter(inputDF['source'] == source_node).select(inputDF['source'], inputDF['target'], inputDF['weight'])

    while(edge_candidatesDF.count() != 0):
        print("="*50)
        print("Iteration Done")
        print("="*50)
        # Get best edge candidate functionality
        best_candidate = edge_candidatesDF.select(edge_candidatesDF["source"], 
                                                  edge_candidatesDF["target"],
                                                  edge_candidatesDF["weight"]).sort(edge_candidatesDF["weight"].asc()).limit(1)

        # Get the Best Path
        # What this does is that it first gets the source nodes path
        # E.g. It is looking at candidate 4. The path it chooses is 2 to 4. 
        # Node 2 path is 1-2 so it gets this first
        # Then it combines 1-2 with "-" and "4" to make "1-2-4"
        best_path = resultDF.filter(resultDF['source'] == best_candidate.first()['source']).first()['new_path'] + "-" + str(best_candidate.first()['target']) 

        # Get the best cost
        # This gets sources original cost and then combines it with the current path cost e.g.
        # We are trying to make node 4 total cost. 1-2 cost is 3 and 4 is 2 so 3+2 = 5
        best_cost = resultDF.filter(resultDF['source'] == best_candidate.first()['source']).first()['new_cost'] + best_candidate.first()['weight'] 

        # Get the target
        best_target = best_candidate.first()['target']

        # Get rid of the row with the target that is having its path entered
        resultDF = resultDF.where("source!="+str(best_target))

        # the columns of resultDF
        columns = ['source', 'new_cost', 'new_path']

        # Create a new dataframe of just the row that will be added to resultDF
        newRow = spark.createDataFrame([(best_target, best_cost,best_path)], columns)

        # Add the updated row
        resultDF = resultDF.union(newRow)
        
        # Get rid of the old candidate
        edge_candidatesDF = edge_candidatesDF.filter(edge_candidatesDF['target'] != best_target)

        # Create a dataframe of potential new candidates
        new_potential_candidatesDF = inputDF.filter(inputDF['source'] == best_target).select(inputDF['source'], inputDF['target'], inputDF['weight'])
        
        # Find all the targets that current don't have a cost
        found_pathDF = resultDF.filter(resultDF['new_cost'] == -1).select(resultDF['source'])
        found_pathDF = found_pathDF.withColumnRenamed("source", "unfound_targets")

        # This filters new_potential_candidatesDF so that it only contains targets taht don't have a cost yet
        new_potential_candidatesDF = new_potential_candidatesDF.join(found_pathDF, new_potential_candidatesDF['target'] == found_pathDF['unfound_targets'], "inner")

        # Get rid of the uneeded column
        new_candidatesDF = new_potential_candidatesDF.drop("unfound_targets")

        # Merge edge_candidates with the new candidates found
        edge_candidatesDF = edge_candidatesDF.union(new_candidatesDF)

    # Create the solution Dataframe in the correct format
    solutionDF = resultDF.withColumnRenamed("source", "id").withColumnRenamed("new_cost", "cost").withColumnRenamed("new_path", "path").sort(resultDF['source'].asc())






    # ------------------------------------------------
    # END OF YOUR CODE
    # ------------------------------------------------

    # Operation A1: 'collect' to get all results
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    source_node = 1

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L15-25_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_3/"
    # my_dataset_dir = "/content/drive/MyDrive/CIT/Laptop/4th Year/Semester 2/Big_Data_Analytics/BigData/Examples/L15-25_Spark_Environment/FileStore/tables/6_Assignments/my_dataset_3/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, source_node)
