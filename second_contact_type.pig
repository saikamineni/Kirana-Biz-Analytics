data = LOAD '/Projectdir/Project_DataSet-1.txt' AS (
Case:int,
Acc_Type:chararray,
Center:chararray,
Escalation_Point_Sec:chararray,
All_Phone_Numbers:chararray,
Sl_Start_Base_Dt:chararray,
Contact_Type:chararray,
Escalation_Dept:chararray,
Customer_Name:chararray,
Primary_Executive:chararray,
Other_Intended_Recipient:chararray,
Additional_Intended_Recipients:chararray,
Contact_Method:chararray,
Case_Status:chararray,
Customer_Area:chararray,
complaint_Note_Txt:chararray,
Case_Due_On:chararray,
Product_Id:chararray,
Complaint_Type_Sec1:chararray,
Complaint_Type_Pri1:chararray,
Complaint_Type_4th1:chararray,
All_Case_Notes:chararray,
City:chararray,
Problem_Type:chararray,
Address_line1:chararray,
Created_On:chararray,
Address_Line2:chararray,
Case_Company_Name:chararray,
Escalation_Point_pri:chararray,
case_closed_on:chararray,
Repeat_customer_flag:chararray,
Duplicate_flag:chararray,
Complaint_type_ter1:chararray,
Escalation_Driver:chararray,
Escalation_point_Ter:chararray,
Keyword:chararray,
SL_Start_Dt:chararray,
Customer_State_Cd:chararray,
Repeat_90_Days_Flag:chararray,
Product_Line:chararray,
Region:chararray,
Department:chararray,
Sl_Stop_Dt:chararray,
Category:chararray,
DaysToClose_Official:int,
Cal_Days_to_Close:int,
Biz_Days_to_Close:int,
Met_Missed_OfficialDays:chararray,
Met_Missed_Resoution_to_SVL:chararray,
Met_Missed_Contact_SlOfficial:chararray,
Contact_SVC_Lvl_MetInd:chararray,
All_Contact_Attempts:chararray,
Complaint_Type_Sec:chararray,
Complaint_Type_Pri:chararray,
Complaint_Type_4th:chararray,
Complaint_Type_Ter:chararray,
Official_Complaint_Indicator:chararray,
LOB:chararray);


new_data = FOREACH data GENERATE 
Contact_Type,
LOB;

SPLIT new_data INTO 
wireless_LOB IF LOB == 'WIRELESS', 
business_LOB IF LOB == 'BUSINESS';

spl_wire = FOREACH wireless_LOB GENERATE
REPLACE(Contact_Type, '^.*Agency.*$', ' Agency') AS Contact_Type:chararray;

spl1_wire = FOREACH spl_wire GENERATE
REPLACE(Contact_Type, '^.*Executive.*$', ' Executive') AS Contact_Type:chararray;

spl2_wire = FOREACH spl1_wire GENERATE
REPLACE(Contact_Type, '^[A-Z].*$', ' Others') AS Contact_Type:chararray;

spl_busi = FOREACH business_LOB GENERATE
REPLACE(Contact_Type, '^.*Agency.*$', ' Agency') AS Contact_Type:chararray;

spl1_busi = FOREACH spl_busi GENERATE
REPLACE(Contact_Type, '^.*Executive.*$', ' Executive') AS Contact_Type:chararray;

spl2_busi = FOREACH spl1_busi GENERATE
REPLACE(Contact_Type, '^[A-Z].*$', ' Others') AS Contact_Type:chararray;

grp_wire = GROUP spl2_wire BY Contact_Type;

grp_busi = GROUP spl2_busi BY Contact_Type;

grp_count_wire = FOREACH grp_wire GENERATE group, COUNT(spl2_wire) AS count;

grp_count_busi = FOREACH grp_busi GENERATE group, COUNT(spl2_busi) AS count;

STORE grp_count_wire INTO '/Projectdir/Contact_wire';
STORE grp_count_busi INTO '/Projectdir/Contact_busi';

grp = UNION grp_count_wire, grp_count_busi;

dump grp;
