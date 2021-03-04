Pig Questions :

The following set of questions need to be executed in PIG :

1. Register the Piggy bank jar file in the Pig Session. 

    REGISTER /usr/hdp/current/pig-client/lib/piggybank.jar;	
	

2. Load the data using the piggy jar file by skipping the header. 
	Hint : use org.apache.pig.piggybank.storage.CSVExcelStorage 
   
raw_data = LOAD '/home/jayantm/Batches/Batch43/CUTE/FlightsData.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
AS (year:int, month: int, unique_carrier:chararray,origin_airport_id:chararray, dest_airport_id:chararray,
dep_delay:int, arr_delay:int, cancelled:int);


3. Print the records to console of the first 10 records. 

tenRecord = LIMIT raw_data 10;
DUMP tenRecord;

4. Find the average departure delay for each airport.
	Hint : Use the AVG function in the relation.
group_by_departure_airport = GROUP raw_data BY (origin_airport_id);
avg_delay_departure_airport = FOREACH group_by_departure_airport GENERATE group, AVG(raw_data.dep_delay) AS avg_departure_delay;

avg_dep_records = LIMIT avg_delay_departure_airport 10;

DUMP avg_dep_records;


5. Find the average arrival delay for each airport. 

group_by_arrival_airport =  GROUP raw_data BY (dest_airport_id);
avg_delay_arrival_airport = FOREACH group_by_arrival_airport GENERATE group, AVG(raw_data.arr_delay) AS avg_arrival_delay;

avg_arr_records = LIMIT avg_delay_arrival_airport 10;
DUMP avg_arr_records;


6. Find the worst aiports (airport IDs) based on average arrival delay and average departure delay. 

avg_delay_departure_airport_ordered =  ORDER avg_delay_departure_airport BY avg_departure_delay DESC;
avg_delay_arrival_airport_ordered =  ORDER avg_delay_arrival_airport BY avg_arrival_delay DESC;
worst_departures = LIMIT avg_delay_departure_airport_ordered 20;
DUMP worst_departures;
worst_arrivals = LIMIT avg_delay_arrival_airport_ordered 20;
DUMP worst_arrivals;

7. Perform Joins on the flight Data (Raw Data) to the average arrival delay for the relevant arrival and departure airports. 

   join_arrival_delay = JOIN raw_data BY origin_airport_id, avg_delay_departure_airport BY group;
   join_both_delay = JOIN join_arrival_delay BY dest_airport_id, avg_delay_arrival_airport BY group;
	





