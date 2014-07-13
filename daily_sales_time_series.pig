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
Mobile_Number;

filtered_data = FILTER required_data BY Date MATCHES '$DATE';

cleaned_data = FOREACH filtered_data GENERATE
REPLACE(Time, '^.*$', CONCAT(REGEX_EXTRACT(Time, '(.*):(.*)', 1),REGEX_EXTRACT(Time, '(.*) (.*)', 2))) AS Time:chararray,
Mobile_Number AS Mobile_Number:long;

corrected_data = FILTER cleaned_data BY Time MATCHES '^.*M$';

grouped_time_data = GROUP corrected_data BY Time;

counted_sales_data = FOREACH grouped_time_data {mob = corrected_data.Mobile_Number; uniq_no = distinct mob; GENERATE group, COUNT(uniq_no) AS total;};

STORE counted_sales_data INTO '/Project-2/peak_hours_line/$DATE';

dump counted_sales_data;
