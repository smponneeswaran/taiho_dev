/*
PLO Cleanup
Description: Script tp clean up the plo's to remove the deleted records brought in for incremental load 
Notes:  
     
Revision History: 23-Nov-2016 ACK Replace resequencing of fieldseq using dense_rank with an update from fielddata so values match between CDM and PLOs 
                  15-Dec-2016 DPR Adding cleanup script to remove null values from rpt_pivotal_study_analytics_datapoints PLO
*/

delete from rpt_missing_data where isdeleted = true;

delete from rpttab_fielddata where isdeleted = true;

delete from fielddata where isdeleted = true;

delete from rpt_pivotal_study_analytics_datapoints where forecast_type is null or forecast_date is null;

