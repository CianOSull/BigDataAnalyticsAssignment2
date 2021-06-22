# BigDataAnalyticsAssignment2
 Second Project for Big Data Analytics module that is about using Spark SQL.<br />
 
## Project specifiction:<br />
**Tasks**:
1. A02_Part1/A02_Part1.py
2. A02_Part2/A02_Part2.py
3. A02_Part3/A02_Part3.py<br />
Complete the function my_main of the Python program. 
Do not modify the name of the function nor the parameters it receives.
The entire work must be done within Spark SQL:
<br /> The function my_main must start with the creation operation 'read' 
above loading the dataset to Spark SQL.
<br /> The function my_main must finish with an action operation 'collect', 
gathering and printing by the screen the result of the Spark SQL job.
<br /> The function my_main must not contain any other action operation 
'collect' other than the one appearing at the very end of the function.
<br /> The resVAL iterator returned by 'collect' must be printed straight away, 
you cannot edit it to alter its format for printing.
4. A02_Part4/A02_Part4.py<br />
Complete the function compute_page_rank of the Python program. 
Do not modify the name of the function nor the parameters it receives.
The function must return a dictionary with (key, value) pairs, where:
<br /> Each key represents a node id. 
<br /> Each value represents the pagerank value computed for this node id. 
5. A02_Part5/A02_Part5.py<br />
Complete the function my_main of the Python program. 
Do not modify the name of the function nor the parameters it receives.
The entire work must be done within Spark SQL:
<br /> The function my_main must start with the creation operation 'read' 
above loading the dataset to Spark SQL.
<br /> The function my_main must finish with an action operation 'collect', 
gathering and printing by the screen the result of the Spark SQL job.
<br /> The function my_main must not contain any other action operation 
'collect' other than the one appearing at the very end of the function.
<br /> The resVAL iterator returned by 'collect' must be printed straight away, 
you cannot edit it to alter its format for printing.<br />

**Task 1**:<br />
Technology:<br />
Spark SQL <br />
<br />
Your task is to: <br />
• Compute the amount of trips starting from and finishing at each station_name. <br /> 
<br />
_Complete the function my_main of the Python program._<br />
o Do not modify the name of the function nor the parameters it receives. <br />
o The entire work must be done within Spark SQL: <br />
1. The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL. <br />
2. The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job. <br />
3. The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function. <br />
4. The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing. <br />
<br />
Results: <br />
Output one Row per station_name. Rows must follow alphabetic order in the name of the station. Each Row must have the following fields: <br />
Row(station, num_departure_trips, num_arrival_trips) <br />
<br />

**Task 2**: <br />
Technology:<br />
Spark SQL <br />
<br />
Your task is to: <br />
Compute the top_n_bikes with highest total duration time for their trips. <br />
<br />
_Complete the function my_main of the Python program._<br />
o Do not modify the name of the function nor the parameters it receives. <br />
o The entire work must be done within Spark SQL: <br />
1. The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL. <br />
2. The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job. <br />
3. The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function. <br />
4. The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing. <br />
<br />
Results: <br />
Output one Row per bike_id. Rows must follow decreasing order in highest total duration time for their trips. Each Row must have the following fields: <br />
Row(bike_id, totalTime, numTrips=total_number_of_trips) <br />
<br />

**Task 3**: <br />
Technology:<br />
Spark SQL <br />
<br />
Your task is to: <br />
• Sometimes bikes are re-organised (moved) from station A to station B to balance the amount of bikes available in both stations. A truck operates this bike re-balancing service, and the trips done by-truck are not logged into the dataset. Compute all the times a given bike_id was moved by the truck re-balancing system. <br /> 
<br />
_Complete the function my_main of the Python program._<br />
o Do not modify the name of the function nor the parameters it receives. <br />
o The entire work must be done within Spark SQL: <br />
1. The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL. <br />
2. The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job. <br />
3. The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function. <br />
4. The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing. <br />
<br />
Results: <br />
Output one Row per moving trip. Rows must follow temporal order. Each Row must have the following fields: <br /> 
Row(start_time, start_station_name, stop_time, stop_station_name) <br />
For example, if the dataset contains the following 2 trips: <br />
o Trip1: A user used bike_id to start a trip from Station1 on 2019/05/10 09:00:00 and finished the trip at Station2 on 2019/05/10 10:00:00 <br />
o Trip2: A user used bike_id to start a trip from Station3 on 2019/05/10 11:00:00 and finished the trip at Station4 on 2019/05/10 12:00:00 <br />
<br />
And the dataset does not contain any extra trip: <br />
o Trip3: A user used bike_id to start a trip from Station2 and finish at Station3 anytime between 2019/05/10 10:00:00 and 2019/05/10 11:00:00 <br />
Then it is clear that the bike was moved from Station2 to Station3 by truck, and we output: <br />
Row(start_time=2019/05/10 10:00:00, start_station_name=Station2, stop_time=2019/05/10 11:00:00, stop_station_name=Station3) <br />
<br />

**Task 4**: <br />
Technology:<br />
Python (without using the Spark library). <br />
<br />
Your task is to:<br />
•  Compute your own sequential implementation of the PageRank algorithm for the nodes of a given graph (e.g., my_dataset_2). <br /> 
<br />
_Complete the function compute_page_rank of the Python program._<br />
o Do not modify the name of the function nor the parameters it receives. <br />
o The function must return a dictionary with (key, value) pairs, where: <br />
1. Each key represents a node id. <br />
2. Each value represents the pagerank value computed for this node id. <br />
<br />
Results:<br />
Given the requested dictionary, the program automatically outputs one (key, value) pair per line. Lines follow a decreasing order in the page rank value of the node. Each line has the following format: <br />
id=key ; pagerank=value \n <br />
<br />

**Task 5**: <br />
Technology:<br />
Spark SQL <br />
<br />
Your task is to:<br />
• Using Spark SQL, compute the shortest path distance from a source node to the remaining nodes of the graph. <br /> 
<br />
_Complete the function my_main of the Python program._<br />
o Do not modify the name of the function nor the parameters it receives. <br />
o The entire work must be done within Spark SQL: <br />
1. The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL. <br />
2. The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job. <br />
3. The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function. <br />
4. The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing. <br />
<br />
o The difficulty of this exercise is on coming up with your own Spark SQL implementation of the Dijkstra shortest path algorithm. That is, you must implement a Spark SQL program following the steps of the Dijkstra algorithm explained in class. <br />
<br />
Results:<br />
Output one Row per bike_id. Rows must follow a decreasing order in the cost of the path. <br />
Each Row must have the following fields: <br />
Row(id, cost, path) <br />
<br />

**Dataset 1 Information:**<br />
This dataset occupies ~80MB and contains 73 files. Each file contains all the trips 
registered the CitiBike system for a concrete day:<br />
• 2019_05_01.csv => All trips registered on the 1st of May of 2019. <br />
• 2019_05_02.csv => All trips registered on the 2nd of May of 2019. <br />
• 2019_07_12.csv => All trips registered on the 12th of July of 2019.<br />
<br />
Altogether, the files contain 444,110 rows. Each row contains the following fields:<br />
start_time , stop_time , trip_duration , start_station_id , start_station_name , 
start_station_latitude , start_station_longitude , stop_station_id , stop_station_name , 
stop_station_latitude , stop_station_longitude , bike_id , user_type , birth_year , gender , 
trip_id<br />
<br />
• (00) start_time<br />
! A String representing the time the trip started at. <br />
<%Y/%m/%d %H:%M:%S> <br />
! Example: “2019/05/02 10:05:00”<br />
• (01) stop_time<br />
! A String representing the time the trip finished at. <br />
<%Y/%m/%d %H:%M:%S> <br />
! Example: “2019/05/02 10:10:00”<br />
• (02) trip_duration<br />
! An Integer representing the duration of the trip.<br />
! Example: 300<br />
• (03) start_station_id<br />
! An Integer representing the ID of the CityBike station the trip started from.<br />
! Example: 150<br />
• (04) start_station_name<br />
! A String representing the name of the CitiBike station the trip started from.<br />
! Example: “E 2 St &; Avenue C”.<br />
• (05) start_station_latitude<br />
! A Float representing the latitude of the CitiBike station the trip started from.<br />
! Example: 40.7208736<br />
• (06) start_station_longitude<br />
! A Float representing the longitude of the CitiBike station the trip started from.<br />
! Example: -73.98085795<br />
• (07) stop_station_id<br />
! An Integer representing the ID of the CityBike station the trip stopped at.<br />
! Example: 150<br />
• (08) stop_station_name! A String representing the name of the CitiBike station the trip stopped at. <br />
! Example: “E 2 St &; Avenue C”.<br />
• (09) stop_station_latitude<br />
! A Float representing the latitude of the CitiBike station the trip stopped at.<br />
! Example: 40.7208736<br />
• (10) stop_station_longitude<br />
! A Float representing the longitude of the CitiBike station the trip stopped at.<br />
! Example: -73.98085795<br />
• (11) bike_id<br />
! An Integer representing the id of the bike used in the trip. <br />
! Example: 33882.<br />
• (12) user_type<br />
! A String representing the type of user using the bike (it can be either “Subscriber” <br />
or “Customer”). <br />
! Example: “Subscriber”.<br />
• (13) birth_year<br />
! An Integer representing the birth year of the user using the bike. <br />
! Example: 1990.<br />
• (14) gender<br />
! An Integer representing the gender of the user using the bike (it can be either 0 => <br />
Unknown; 1 => male; 2 => female).<br />
! Example: 2.<br />
• (15) trip_id<br />
! An Integer representing the id of the trip. <br />
! Example: 190.<br />

**Dataset 2 Information:**<br />
This dataset consists in the file tiny_graph.txt, which contains 26 edges (indeed, 13 
edges, one on each direction) in a graph with 8 nodes.<br />

**Dataset 3 Information:**<br />
This dataset consists in the file tiny_graph.txt, which contains 18 edges (indeed, 9 
edges, one on each direction) in a graph with 6 nodes.
