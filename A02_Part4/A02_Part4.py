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

# ------------------------------------------------
# IMPORTS
# ------------------------------------------------
import codecs
import functools
import os

# ------------------------------------------------
# FUNCTION read_graph_from_folder
# ------------------------------------------------
def read_graph_from_folder(my_dataset_dir):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the number of nodes
    num_nodes = 0

    # 1.2. We output the connections per node
    edges_per_node = {}

    # 2. We list the files from the directory my_dataset_dir
    list_of_files = os.listdir(my_dataset_dir)
    if ('.DS_Store' in list_of_files):
        list_of_files.remove('.DS_Store')
    list_of_files.sort()

    # 3. We traverse the files one by one
    for file in list_of_files:
        # 3.1. We open the file for reading
        my_input_stream = codecs.open(my_dataset_dir + file, "r", encoding='utf-8')

        # 3.2. We traverse the file
        for line in my_input_stream:
            # 3.2.1. We parse the line
            (source_node, target_node, _) = tuple(line.strip().split(" "))

            # 3.2.2. We search for new nodes appearing in the line
            for node_name in [source_node, target_node]:
                # 3.2.2.1. If the node has not appeared before
                if node_name not in edges_per_node:
                    # I. We associate the new node with an empty list of edges
                    edges_per_node[node_name] = []

                    # II. We increase the number of different nodes found so far
                    num_nodes += 1

            # 3.2.3. We populate the edges of source_node
            edges_per_node[source_node].append( target_node )

        # 3.4. We close the file
        my_input_stream.close()

    # 4. We make the info to be a tuple (num_neighbours, neighbours_list)
    for node_name in edges_per_node:
        neighbour_list = edges_per_node[node_name]
        edges_per_node[node_name] = (len(neighbour_list), neighbour_list)

    # 5. We assign res
    res = (num_nodes, edges_per_node)

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION compute_page_rank
# ------------------------------------------
def compute_page_rank(edges_per_node, reset_probability, max_iterations):

    # ------------------------------------------------
    # START OF YOUR CODE:
    # ------------------------------------------------

    # Remember that the function must return a dictionary with:
    # Key => The node number
    # Value => The PageRank value computed for this node.
    
    # Type all your code here.

    # Return dictionary with {node_number : value}
    res = {}

    # Initalize the res to have each node initial value
    # Basically every iteration uses the previous page rank and iteration 0 for each should be 1/n, where n is the total number of nodes
    N = len(edges_per_node)
    for node_name in edges_per_node:
        res[node_name] = 1

    # For loop for the iterations
    for i in range(max_iterations):
        # Since python does shallow copies with variables, we will need a DEEP copy
        # for using previous iteration page rank vlaues so use list.copy to get a deep copy
        prev_it_res = res.copy()

        # Go through each node
        for node in edges_per_node:
            # Get all the connected nodes to the node in the current loop
            con_nodes = edges_per_node[node][1]

            # Get the number of connected edges for each connected node
            no_con_nodes = []

            # This list will contain the page_rank/number of edges
            node_calc = 0

            # Using the nodes obtained from above
            for index, con in enumerate(con_nodes):
                # Get the number of connections each node has
                node_cons = edges_per_node[con][0]
                # Get the previous page rank
                previous_page = prev_it_res[con_nodes[index]]
                # Create the new pag rank
                new_page = previous_page / node_cons
                # Sum the new page ranks into one variable
                node_calc += new_page

            # Sum calc values and add to page rank
            page_rank = reset_probability + (1 - reset_probability) * node_calc

            res[node] = page_rank

    return res

    # ------------------------------------------------
    # END OF YOUR CODE
    # ------------------------------------------------

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(my_dataset_dir, reset_probability, max_iterations):
    # 1. We read the graph from the file
    (num_nodes, edges_per_node) = read_graph_from_folder(my_dataset_dir)

    # 2. We compute the shortest paths to each node
    page_rank_per_node = compute_page_rank(edges_per_node, reset_probability, max_iterations)

    # 3. We sort the nodes in decreasing order in their rank
    rank_per_node = [ (round(page_rank_per_node[node], 2), node) for node in page_rank_per_node ]
    rank_per_node.sort(reverse=True)

    # 4. We print them
    for item in rank_per_node:
        print("id=" + str(item[1]) + "; pagerank=" + str(item[0]))

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
    # 1. We get the input values
    reset_probability = 0.15
    max_iterations = 3

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L15-25_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_2/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 3. We call to my_main
    my_main(my_dataset_dir, reset_probability, max_iterations)
