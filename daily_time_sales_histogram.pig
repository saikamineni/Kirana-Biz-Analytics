data = LOAD '/Project-2/KBA_Dummy_Data.csv' USING PigStorage(',') AS (
Date:chararray,
Time:chararray,
Mobile_Number:long,
Purchase_item1:chararray,
Quantity:float,
Price_per_unit:float,
Total_Amount:float);

required_data = FOREACH data GENERATE 
Date,
Time,
Total_Amount;

filtered_data = FILTER required_data BY Date MATCHES '$DATE';

cleaned_data = FOREACH filtered_data GENERATE
REPLACE(Time, '^.*$', CONCAT(REGEX_EXTRACT(Time, '(.*):(.*)', 1),REGEX_EXTRACT(Time, '(.*) (.*)', 2))) AS Time:chararray,
Total_Amount AS Total_Amount:float;

corrected_data = FILTER cleaned_data BY Time MATCHES '^.*M$';

grouped_time_data = GROUP corrected_data BY Time;

counted_sales_data = FOREACH grouped_time_data GENERATE group, SUM(corrected_data.Total_Amount) AS total;

STORE counted_sales_data INTO '/Project-2/time_sales_hist/$DATE';

dump counted_sales_data;
