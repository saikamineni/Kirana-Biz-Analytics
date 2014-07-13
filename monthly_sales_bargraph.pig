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
Total_Amount;

year_data = FILTER required_data BY Date MATCHES '^.*/.*/$YEAR$';

only_month = FOREACH year_data GENERATE 
REPLACE(Date, '^.*$', REGEX_EXTRACT(REGEX_EXTRACT(Date, '^(.*)/(.*)$', 1), '^(.*)/(.*)$', 1)) AS Month,
Total_Amount;

grouped = GROUP only_month BY Month;

counted = FOREACH grouped GENERATE group, SUM(only_month.Total_Amount) AS total;

STORE counted INTO '/Project-2/month_sales_bargraph/$YEAR';

DUMP counted;
