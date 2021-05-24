# BigDataAnalyticsAssignment2
 Second Project for Big Data Analytics module.<br />
 
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
