/*
rpt_resource_analytics PLO
CDM Version: 2.5
Notes: 
    - CRAs identified by siteresource.resourcetype = 'CRA'

Revision History: 07-Jul-2016 Adam Kaus - Initial Version
                  31-Aug-2016 Adam Kaus - Added dsterm to where clause for filtering by enrollment
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
*/

CREATE TABLE rpt_resource_analytics AS
WITH enrolled_subjects AS (SELECT ds.studyid, 
                                    site.sitecro,
                                    count(distinct ds.comprehendid) AS enrollment_count
                            FROM rpt_subject_disposition ds
                            JOIN site ON (ds.siteid = site.siteid)
                            WHERE ds.dsevent = 'ENROLLED'
                            GROUP BY 1, 2),

    total_cras AS (SELECT sr.studyid,
                            site.sitecro,
                            count(distinct upper(sr.resourcename)) as cra_count
                    FROM siteresource sr
                    JOIN site ON (sr.siteid = site.siteid)
                    WHERE upper(sr.resourcetype) = 'CRA'
                    AND sr.resourceenddtc is null
                    GROUP BY 1, 2)

SELECT study.comprehendid::text AS comprehendid,
        site.sitecro::text AS cro,
        study.therapeuticarea::text AS therapeuticarea,
        study.program::text AS program,
        study.studyid::text AS studyid, 
        study.studyname::text AS studyname,
        coalesce(cra.cra_count, 0)::integer AS cra_count,
        coalesce(enr.enrollment_count, 0)::integer AS enrollment_count,
        now()::timestamp as comprehend_update_time
FROM study
JOIN (SELECT DISTINCT studyid, sitecro FROM site) site ON (study.studyid = site.studyid)
LEFT JOIN enrolled_subjects enr ON (site.studyid = enr.studyid AND coalesce(site.sitecro, 'X') = coalesce(enr.sitecro, 'X'))
LEFT JOIN total_cras cra ON (site.studyid = cra.studyid AND coalesce(site.sitecro, 'X') = coalesce(cra.sitecro, 'X'));
