## CQS Presentation Layer Object Guidelines

### Coding Conventions
1. Table and column names must not be double quotes (thus to ensure they are lowercase in the database)
1. Presentation Layer Objects (PLO) supporting analytics must be named with the following convention: rpt_[name using underscores]
1. Presentation Layer Objects (PLO) supporting tabular reports must be named with the following convention: rpttab_[name using underscores]

### Rules
1. Presentation layer objects must depend only on CCDM objects (no inter-dependencies, temp tables, etc.)
1. The definition of a presentation layer object should simply be a SQL select statement -- no CREATE TABLE or DROP TABLE statements are needed.
1. Presentation layer objects must not rely on custom database functions/procedures
1. Every PLO must have a primary key and at least one unique key
1. Every PLO must have 100% coverage of table and column comments written with human readable names (not a repeat of the table/column name)
1. Every PLO must have not null constraints
1. Every PLO should have performance enhancing indexes added
1. If the underlying CCDM object has predicates for incremental refresh, the PLO must also support/include the same predicates in order to support incremental refresh
1. Tabular listing PLOs should not be utilized for KPI analytics - but only for tabular listings

### Recommendations
1. The comprehendId should be pulled into the PLO whenever possible. In the case of PLOs that are aggregating data to a higher level, e.g. subject level data being aggregated to site level data, the comprehendid should contain the lowest level possible for the resultant PLO object e.g. the PLO is containing data at the site level, the comprehend id would contain studyid~siteid.
1. Metadata that is useful for reporting should be pulled into the PLO whenever possible e.g. include siteName, SiteCountry, and SiteRegion, as well as siteId.  This will make filtering less complex in the application
1. Consider variations in the data being mapped.  If hard coding expected values for a given field, ensure that the DIN team knows/confirms the dependency before continuing e.g. AE.AESEV does not always include Mild, Moderate, Severe and if that is hard coded in PL objects, it will not work for all clients.
1. Suggestion to add the following columns: rpt_custom_edge{n} to allow easier edging e.g. rpt_custom_edge1 should be all the pk columns joined together with “~”.
1. Suggestion to maintain a 1-1 relationship between a PLO and KPI rather than have one PLO support many KPIs whenever possible

### Required Artifacts
1. PLO must be provided as SQL statement with Pull Request to the CCDM repository in the cqs/resource/mapping/presentation_layer folder
1. PLO constraints must be provided with Pull Request to the CCDM repository in the cqs/resources/mapping/
1. PLO requirements describing the logic and the dependent reports provided via PLO inventory document
1. PLO integration testing objectives mapped against requirements provided via PLO inventory document
1. PLO added to [master PLO inventory document](https://docs.google.com/a/comprehend.com/spreadsheets/d/12LpaP6CaoW6RogegeYazFObAC_ZPD3e_LFSwpI1R674/edit?usp=sharing) with appropriate structure
1. Performance metrics run against demo and dev data for the object provided to DIN
