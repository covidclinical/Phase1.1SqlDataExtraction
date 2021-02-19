--##############################################################################
--### 4CE Phase 1.1
--### Date: May 6, 2020
--### Database: Oracle
--### Data Model: i2b2
--### Original MSSQL Created By: Griffin Weber (weber@hms.harvard.edu)
--### 	 Original:https://github.com/GriffinWeber/covid19i2b2/blob/master/4CE_Phase1.1_Files_mssql.sql
--###	 commit 64d83bd69c1a4d856c5150c08516d288afce1fb5
--### Adapted to Oracle by: Robert Bradford (UNC-CH) [rbrad@med.unc.edu]
--### Adapted to load BCH data : Jaspreet Khanna
--### This script expects mapping tables to be populated using the mapping data loader sql script.
--### Covid pos patients are populated using the view VW_COVID_POS_PATIENTS in this script.
--### This script has SQL blocks and based on settings in covid_config table selectively they have to be executed.
--### fixed typo in header of file DailyCountsCSV
--##############################################################################

--------------------------------------------------------------------------------
-- General settings
--------------------------------------------------------------------------------
create table covid_config (
	siteid varchar(20), -- Up to 20 letters or numbers, must start with letter, no spaces or special characters.
	include_race number(1), -- 1 if your site collects race/ethnicity data; 0 if your site does not collect this.
	race_in_fact_table number(1), -- 1 if race in observation_fact.concept_cd; 0 if in patient_dimension.race_cd
	hispanic_in_fact_table number(1), -- 1 if Hispanic/Latino in observation_fact.concept_cd; 0 if in patient_dimension.race_cd
	death_data_accurate number(1), -- 1 if the patient_dimension.death_date field is populated and is accurate
	code_prefix_icd9cm varchar(50), -- prefix (scheme) used in front of a ICD9CM diagnosis code [required]
	code_prefix_icd10cm varchar(50), -- prefix (scheme) used in front of a ICD10CM diagnosis code [required]
	code_prefix_icd9proc varchar(50), -- prefix (scheme) used in front of a ICD9 procedure code [required]
	code_prefix_icd10pcs varchar(50), -- prefix (scheme) used in front of a ICD10 procedure code [required]
	obfuscation_blur number(8,0), -- Add random number +/-blur to each count (0 = no blur)
	obfuscation_small_count_mask number(8,0), -- Replace counts less than mask with -99 (0 = no small count masking)
	obfuscation_small_count_delete number(1), -- Delete rows with small counts (0 = no, 1 = yes)
	obfuscation_demographics number(1), -- Replace combination demographics and total counts with -999 (0 = no, 1 = yes)
	output_as_columns number(1), -- Return the data in tables with separate columns per field
	output_as_csv number(1) -- Return the data in tables with a single column containing comma separated values
);


--- Using i2b2 view to load covid_pos_patients specific to BCH data.
CREATE OR REPLACE VIEW VW_COVID_POS_PATIENTS (PATIENT_NUM, COVID_POS_DATE) AS 
  (SELECT PATIENT_NUM , TO_DATE(TO_CHAR( POSITIVE_DATE , 'YYYY-MM-DD'),'YYYY-MM-DD' ) AS positvie_Date FROM (SELECT  patient_num, min(start_date) AS positive_date
FROM OBSERVATION_FACT WHERE CONCEPT_CD IN ('LAB:1044704735','LAB:1044804335','LAB:1043473617') AND lower(TVAL_CHAR) = 'positive'
GROUP BY PATIENT_NUM ));

--Used data provide by bch to load tables COVID_CODE_MAP,COVID_MED_MAP, COVID_LAB_MAP .
create table COVID_CODE_MAP (
	code varchar(50) not null,
	local_code varchar(50) not null,
    constraint COVID_CODE_MAP_PK PRIMARY KEY (code, local_code)
);
create table COVID_MED_MAP (
	med_class varchar(50) not null,
	code_type varchar(10) not null,
	local_med_code varchar(50) not null,
    constraint COVID_MED_MAP_PK primary key (med_class, code_type, local_med_code)
);

create table COVID_LAB_MAP (
	loinc varchar(20) not null, 
	local_lab_code varchar(50) not null, 
	scale_factor numeric(4), 
	lab_units varchar(20), 
	lab_name varchar(100),
    constraint COVID_LAB_MAP_PK1 PRIMARY KEY (loinc, local_lab_code)
);


create table covid_pos_patients (
	patient_num varchar2(100) not null,
	covid_pos_date date not null,
    constraint covid_pospatients_pk_3 primary key (patient_num, covid_pos_date)
);
--*******Pause After this step 													    
--Use dataloader script COVID_Mapping_And_Config_DL.sql to load mapping tables COVID_CODE_MAP,COVID_MED_MAP, COVID_LAB_MAP .
													     
--Populate covid_pos_patients from i2b2 view.
insert into covid_pos_patients
select * from Vw_Covid_Pos_Patients ;
commit;    


--------------------------------------------------------------------------------
-- Create a list of dates when patients were inpatient starting one week  
--   before their COVID pos date.
--------------------------------------------------------------------------------

create table covid_admissions (
	patient_num varchar2(50) not null,
	admission_date date not null,
	discharge_date date not null,
    constraint covid_admissions primary key (patient_num, admission_date, discharge_date)
);

insert into covid_admissions
	select distinct v.patient_num, cast(start_date as date), cast(coalesce(end_date,current_date) as date)
	from visit_dimension v
		inner join covid_pos_patients p
			on v.patient_num=p.patient_num 
				and v.start_date >= (trunc(p.covid_pos_date)-7)
		inner join covid_code_map m
			on v.inout_cd = m.local_code and m.code = 'inpatient';
commit;

--------------------------------------------------------------------------------
-- Get the list of patients who will be the covid cohort.
-- These will be patients who had an admission between 7 days before and
--   14 days after their covid positive test date.
--------------------------------------------------------------------------------


create table covid_cohort (
	patient_num  varchar2(50) not null,
	admission_date date,
	severe number(8,0),
	severe_date date,
	death_date date,
    constraint covid_cohort primary key (patient_num)
);

insert into covid_cohort
	select p.patient_num, min(admission_date) admission_date, 0, null, null
	from covid_pos_patients p
		inner join covid_admissions a
			on p.patient_num = a.patient_num	
				and a.admission_date <= (trunc(covid_pos_date)+14)
	group by p.patient_num;
commit; 

--******************************************************************************
--******************************************************************************
--*** Determine which patients had severe disease or died
--******************************************************************************
--******************************************************************************

--------------------------------------------------------------------------------
-- Flag the patients who had severe disease anytime since admission.
--------------------------------------------------------------------------------

create table covid_severe_patients (
	patient_num varchar2(50) not null,
	severe_date date
);


-- Get a list of patients with severe codes
-- WARNING: This query might take a few minutes to run.
insert into covid_severe_patients
	select f.patient_num, min(start_date) start_date
	from observation_fact f
		inner join covid_cohort c
			on f.patient_num = c.patient_num and f.start_date >= c.admission_date
		cross apply covid_config x
	where 
		-- Any PaCO2 or PaO2 lab test
		f.concept_cd in (select local_lab_code from covid_lab_map where loinc in ('2019-8','2703-7'))
		-- Any severe medication
		or f.concept_cd in (select local_med_code from covid_med_map where med_class in ('SIANES','SICARDIAC'))
		-- Acute respiratory distress syndrome (diagnosis)
		or f.concept_cd in (code_prefix_icd10cm||'J80', code_prefix_icd9cm||'518.82')
		-- Ventilator associated pneumonia (diagnosis)
		or f.concept_cd in (code_prefix_icd10cm||'J95.851', code_prefix_icd9cm||'997.31')
		-- Insertion of endotracheal tube (procedure)
		or f.concept_cd in (code_prefix_icd10pcs||'0BH17EZ', code_prefix_icd9proc||'96.04')
		-- Invasive mechanical ventilation (procedure)
		or regexp_like(f.concept_cd , code_prefix_icd10pcs||'5A09[345]{1}[A-Z0-9]?') --Converted to ORACLE Regex
		or regexp_like(f.concept_cd , code_prefix_icd9proc||'96.7[012]{1}') --Converted to ORACLE Regex
	group by f.patient_num;
commit;    

-- Update the covid_cohort table to flag severe patients 
MERGE INTO COVID_COHORT c
USING (select patient_num, min(severe_date) severe_date
			from covid_severe_patients
			group by patient_num) s
ON (c.patient_num=s.patient_num)
WHEN MATCHED THEN UPDATE SET c.severe = 1, c.severe_date = s.severe_date;
commit;

--------------------------------------------------------------------------------
-- Add death dates to patients who have died.
--Run below SQL block if in covid_config row exists where death_data_accurate = 1												 
--------------------------------------------------------------------------------

Declare
v_death_data_accurate Integer ;

begin
    Select death_data_accurate into v_death_data_accurate from covid_config where death_data_accurate = 1 ;
    If v_death_data_accurate = 1 then
        -- Get the death date from the patient_dimension table.
        update covid_cohort c
            set c.death_date = (
                select 
                    case when p.death_date > coalesce(severe_date,admission_date) 
                    then p.death_date 
                    else coalesce(severe_date,admission_date) end
                from patient_dimension p where p.patient_num = c.patient_num
            )
            where exists (select c.patient_num from patient_dimension p 
            where p.patient_num = c.patient_num and (p.death_date is not null or p.vital_status_cd in ('Y'))); 

        commit;
    end if;            
end;


--******************************************************************************
--******************************************************************************
--*** Precompute some temp tables
--******************************************************************************
--******************************************************************************

--------------------------------------------------------------------------------
-- Create a list of dates since the first case.
--------------------------------------------------------------------------------


create table covid_date_list_temp as
with n as (
    select 0 n from dual
        union all
    select 1 from dual
        union all
    select 2 from dual
        union all
    select 3 from dual
        union all
    select 4 from dual
        union all
    select 5 from dual
        union all
    select 6 from dual
        union all
    select 7 from dual
        union all
    select 8 from dual
        union all
    select 9 from dual
)
select d
from (
    select nvl(cast((p.s + numtodsinterval(((a.n + (10 * b.n)) + (100 * c.n)),'day')) as date), '01-JAN-2020') d
    from (select min(admission_date) s from covid_cohort) p
        cross join n a cross join n b cross join n c
) l
where d<=current_timestamp;

alter table covid_date_list_temp add constraint temp_datelist_pk primary key (d);


--------------------------------------------------------------------------------
-- Create a table with patient demographics.
--------------------------------------------------------------------------------

create table covid_demographics_temp (
	patient_num varchar2(50),
	sex varchar(10),
	age_group varchar(20),
	race varchar(30)
);


-- Get patients' sex
insert into covid_demographics_temp (patient_num, sex)
	select patient_num, m.code
	from patient_dimension p
		inner join covid_code_map m
			on p.sex_cd = m.local_code
				and m.code in ('male','female')
	where patient_num in (select patient_num from covid_cohort);
    
commit;    
-- Get patients' age
insert into covid_demographics_temp (patient_num, age_group)
	select patient_num,
        -- uncomment if you pre-compute age on patient_dimension
		/*(case
			when age_in_years_num between 0 and 2 then '00to02'
			when age_in_years_num between 3 and 5 then '03to05'
			when age_in_years_num between 6 and 11 then '06to11'
			when age_in_years_num between 12 and 17 then '12to17'
			when age_in_years_num between 18 and 25 then '18to25'
			when age_in_years_num between 26 and 49 then '26to49'
			when age_in_years_num between 50 and 69 then '50to69'
			when age_in_years_num between 70 and 79 then '70to79'
			when age_in_years_num >= 80 then '80plus'
			else 'other' end) age*/
        (case
			when floor(months_between(sysdate, birth_date)/12) between 0 and 2 then '00to02'
			when floor(months_between(sysdate, birth_date)/12) between 3 and 5 then '03to05'
			when floor(months_between(sysdate, birth_date)/12) between 6 and 11 then '06to11'
			when floor(months_between(sysdate, birth_date)/12) between 12 and 17 then '12to17'
			when floor(months_between(sysdate, birth_date)/12) between 18 and 25 then '18to25'
			when floor(months_between(sysdate, birth_date)/12) between 26 and 49 then '26to49'
			when floor(months_between(sysdate, birth_date)/12) between 50 and 69 then '50to69'
			when floor(months_between(sysdate, birth_date)/12) between 70 and 79 then '70to79'
			when floor(months_between(sysdate, birth_date)/12) >= 80 then '80plus'
			else 'other' end) age
	from patient_dimension
	where patient_num in (select patient_num from covid_cohort);
commit;    
-- Get patients' race(s)
-- (race from patient_dimension)
insert into covid_demographics_temp (patient_num, race)
	select p.patient_num, m.code
	from covid_config x
		cross join patient_dimension p
		inner join covid_code_map m
			on p.race_cd = m.local_code
	where p.patient_num in (select patient_num from covid_cohort)
		and x.include_race = 1
		and (
			(x.race_in_fact_table = 0 and m.code in ('american_indian','asian','black','hawaiian_pacific_islander','white'))
			or
			(x.hispanic_in_fact_table = 0 and m.code in ('hispanic_latino'))
		)
;commit;

--Commented this out as we per Config table setting will load it from Patient_dimension table.
-- (race from observation_fact)
/*insert into covid_demographics_temp (patient_num, race)
	select f.patient_num, m.code
	from covid_config x
		cross join observation_fact f
		inner join covid_code_map m
			on f.concept_cd = m.local_code
	where f.patient_num in (select patient_num from covid_cohort)
		and x.include_race = 1
		and (
			(x.race_in_fact_table = 1 and m.code in ('american_indian','asian','black','hawaiian_pacific_islander','white'))
			or
			(x.hispanic_in_fact_table = 1 and m.code in ('hispanic_latino'))
		)
;commit;    */    
-- Make sure every patient has a sex, age_group, and race
insert into covid_demographics_temp (patient_num, sex, age_group, race)
	select patient_num, 'other', null, null
		from covid_cohort
		where patient_num not in (select patient_num from covid_demographics_temp where sex is not null)
	union all
	select patient_num, null, 'other', null
		from covid_cohort
		where patient_num not in (select patient_num from covid_demographics_temp where age_group is not null)
	union all
	select patient_num, null, null, 'other'
		from covid_cohort
		where patient_num not in (select patient_num from covid_demographics_temp where race is not null)
;commit;

--******************************************************************************
--******************************************************************************
--*** Create data tables
--******************************************************************************
--******************************************************************************

--------------------------------------------------------------------------------
-- Create DailyCounts table.
--------------------------------------------------------------------------------
create table covid_daily_counts (
	siteid varchar(50) default '@' not null ,
	calendar_date date not null,
	cumulative_patients_all numeric(8,0),
	cumulative_patients_severe numeric(8,0),
	cumulative_patients_dead numeric(8,0),
	num_pat_in_hosp_on_date numeric(8,0), -- num_patients_in_hospital_on_this_date: shortened to under 128 bytes
	num_pat_in_hospsevere_on_date numeric(8,0), --num_patients_in_hospital_and_severe_on_this_date: shortened to under 128 bytes
    constraint covid_dlycounts_pk primary key (calendar_date)
);



insert into covid_daily_counts
	select '@' siteid, d.*,
		(select count(distinct c.patient_num)
			from covid_admissions p
				inner join covid_cohort c
					on p.patient_num=c.patient_num
			where p.admission_date>=c.admission_date
				and p.admission_date<=d.d and p.discharge_date>=d.d
		) num_pat_in_hosp_on_date,
		(select count(distinct c.patient_num)
			from covid_admissions p
				inner join covid_cohort c
					on p.patient_num=c.patient_num
			where p.admission_date>=c.admission_date
				and p.admission_date<=d.d and p.discharge_date>=d.d
				and c.severe_date<=d.d
		) num_pat_in_hospsevere_on_date
	from (
		select d.d,
			sum(case when c.admission_date<=d.d then 1 else 0 end) cumulative_patients_all,
			sum(case when c.severe_date<=d.d then 1 else 0 end) cumulative_patients_severe,
			sum(case when c.death_date<=d.d then 1 else 0 end) cumulative_patients_dead
		from covid_date_list_temp d
			cross join covid_cohort c
		group by d.d
	) d
;commit;    
-- Set cumulative_patients_dead = -999 if you do not have accurate death data. 
update covid_daily_counts
	set cumulative_patients_dead = -999
	where exists (select * from covid_config where death_data_accurate = 0)
;commit;    

--------------------------------------------------------------------------------
-- Create ClinicalCourse table.
--------------------------------------------------------------------------------

create table covid_clinical_course (
	siteid varchar(50)  default '@' not null,
	days_since_admission int not null,
	num_pat_all_cur_in_hosp numeric(8,0),  --num_patients_all_still_in_hospital: shortened to under 128 bytes
	num_pat_ever_severe_cur_hosp numeric(8,0),  --num_patients_ever_severe_still_in_hospital: shortened to under 128 bytes
    constraint covid_clinicalcourse_pk primary key (days_since_admission)
);

insert into covid_clinical_course
	select '@' siteid, days_since_admission, 
		count(*),
		sum(severe)
	from (
		select distinct trunc(d.d)-trunc(c.admission_date) days_since_admission, 
			c.patient_num, severe
		from covid_date_list_temp d
			inner join covid_admissions p
				on trunc(p.admission_date)<=trunc(d.d) and trunc(p.discharge_date)>=trunc(d.d)
			inner join covid_cohort c
				on p.patient_num=c.patient_num and trunc(p.admission_date)>=trunc(c.admission_date)
	) t
	group by days_since_admission
;commit;    

--------------------------------------------------------------------------------
-- Create Demographics table.
--------------------------------------------------------------------------------
create table covid_demographics (
	siteid varchar(50)  default '@' not null,
	sex varchar(10) not null,
	age_group varchar(20) not null,
	race varchar(30) not null,
	num_patients_all numeric(8,0),
	num_patients_ever_severe numeric(8,0),
    constraint covid_demographics_pk primary key (sex, age_group, race)
);



insert into covid_demographics
	select '@' siteid, sex, age_group, race, count(*), sum(severe)
	from covid_cohort c
		inner join (
			select patient_num, sex from covid_demographics_temp where sex is not null
			union all
			select patient_num, 'all' from covid_cohort
		) s on c.patient_num=s.patient_num
		inner join (
			select patient_num, age_group from covid_demographics_temp where age_group is not null
			union all
			select patient_num, 'all' from covid_cohort
		) a on c.patient_num=a.patient_num
		inner join (
			select patient_num, race from covid_demographics_temp where race is not null
			union all
			select patient_num, 'all' from covid_cohort
		) r on c.patient_num=r.patient_num
	group by sex, age_group, race
;commit;    
-- Set counts = -999 if not including race.
/*
update covid_demographics
	set num_patients_all = -999, num_patients_ever_severe = -999
	where exists (select * from covid_config where include_race = 0)
;commit;
*/

--------------------------------------------------------------------------------
-- Create Labs table.
--------------------------------------------------------------------------------
create table covid_labs (
	siteid varchar(50)  default '@' not null,
	loinc varchar(20) not null,
	days_since_admission int not null,
	units varchar(20),
	num_patients_all numeric(8,0),
	mean_value_all float,
	stdev_value_all float,
	mean_log_value_all float,
	stdev_log_value_all float,
	num_patients_ever_severe numeric(8,0),
	mean_value_ever_severe float,
	stdev_value_ever_severe float,
	mean_log_value_ever_severe float,
	stdev_log_value_ever_severe float,
    constraint covid_labs_pk primary key (loinc, days_since_admission)
);


insert into covid_labs
	select '@', loinc, days_since_admission, lab_units,
		count(*), 
		avg(val), 
		coalesce(stddev(val),0),
		avg(logval), 
		coalesce(stddev(logval),0),
		sum(severe), 
		(case when sum(severe)=0 then -999 else avg(case when severe=1 then val else null end) end), 
		(case when sum(severe)=0 then -999 else coalesce(stddev(case when severe=1 then val else null end),0) end),
		(case when sum(severe)=0 then -999 else avg(case when severe=1 then logval else null end) end), 
		(case when sum(severe)=0 then -999 else coalesce(stddev(case when severe=1 then logval else null end),0) end)
	from (
		select loinc, lab_units, patient_num, severe, days_since_admission, 
			avg(val) val, 
			avg(ln(val+0.5)) logval -- natural log (ln), not log base 10
		from (
			select l.loinc, l.lab_units, f.patient_num, p.severe,
				trunc(f.start_date) - trunc(p.admission_date) days_since_admission,
				f.nval_num*l.scale_factor val
			from observation_fact f
				inner join covid_cohort p 
					on f.patient_num=p.patient_num
				inner join covid_lab_map l
					on f.concept_cd=l.local_lab_code
			where l.local_lab_code is not null
				and f.nval_num is not null
				and f.nval_num >= 0
				and f.start_date >= p.admission_date
				and l.loinc not in ('2019-8','2703-7')
		) t
		group by loinc, lab_units, patient_num, severe, days_since_admission
	) t
	group by loinc, days_since_admission, lab_units
;commit;    

--------------------------------------------------------------------------------
-- Create Diagnosis table.
-- * Select all ICD9 and ICD10 codes.
-- * Note that just the left 3 characters of the ICD codes should be used.
-- * Customize this query if your ICD codes do not have a prefix.
--------------------------------------------------------------------------------

create table covid_diagnoses (
	siteid varchar(50)  default '@' not null,
	icd_code_3chars varchar(10) not null,
	icd_version int not null,
	num_pat_all_before_admission numeric(8,0), --NUM_PATIENTS_ALL_BEFORE_ADMISSION: shortened to under 128 bytes
	num_pat_all_since_admission numeric(8,0), --NUM_PATIENTS_all_SINCE_ADMISSION: shortened to under 128 bytes
	num_pat_ever_severe_before_adm numeric(8,0), --num_patients_ever_severe_before_admission: shortened to under 128 bytes
	num_pat_ever_severe_since_adm numeric(8,0), --num_patients_ever_severe_since_admission: shortened to under 128 bytes
    constraint covid_diagnoses_pk primary key (icd_code_3chars, icd_version)
);


insert into covid_diagnoses
	select '@' siteid, icd_code_3chars, icd_version,
		sum(before_admission), 
		sum(since_admission), 
		sum(severe*before_admission), 
		sum(severe*since_admission)
	from (
		-- ICD9
		select distinct p.patient_num, p.severe, 9 icd_version,
			substr(substr(f.concept_cd, length(code_prefix_icd9cm)+1, 999), 1, 3) icd_code_3chars,
			(case when f.start_date <= (trunc(p.admission_date)-15) then 1 else 0 end) before_admission,
			(case when f.start_date >= p.admission_date then 1 else 0 end) since_admission
		from covid_config x
			cross join observation_fact f
			inner join covid_cohort p 
				on f.patient_num=p.patient_num 
					and f.start_date >= (trunc(p.admission_date)-365)
		where concept_cd like x.code_prefix_icd9cm||'%' and length(x.code_prefix_icd9cm)>0
		-- ICD10
		union all
		select distinct p.patient_num, p.severe, 10 icd_version,
			substr(substr(f.concept_cd, length(code_prefix_icd10cm)+1, 999), 1, 3) icd_code_3chars,
			(case when f.start_date <= (trunc(p.admission_date)-15) then 1 else 0 end) before_admission,
			(case when f.start_date >= p.admission_date then 1 else 0 end) since_admission
		from covid_config x
			cross join observation_fact f
			inner join covid_cohort p 
				on f.patient_num=p.patient_num 
					and f.start_date >= (trunc(p.admission_date)-365)
		where concept_cd like x.code_prefix_icd10cm||'%' and length(x.code_prefix_icd10cm)>0
	) t
	group by icd_code_3chars, icd_version;
commit;    

--------------------------------------------------------------------------------
-- Create Medications table.
--------------------------------------------------------------------------------
create table covid_medications (
	siteid varchar(50)  default '@' not null,
	med_class varchar(20) not null,
	num_pat_all_before_admission numeric(8,0),
	num_pat_all_since_admission numeric(8,0),
	num_pat_ever_severe_before_adm numeric(8,0), --num_patients_ever_severe_before_admission: shortened to under 128 bytes
	num_pat_ever_severe_since_adm numeric(8,0), --num_patients_ever_severe_since_admission: shortened to under 128 bytes
    constraint covid_medications primary key (med_class)
);


insert into covid_medications
	select '@' siteid, med_class,
		sum(before_admission), 
		sum(since_admission), 
		sum(severe*before_admission), 
		sum(severe*since_admission)
	from (
		select distinct p.patient_num, p.severe, m.med_class,	
			(case when f.start_date <= (trunc(p.admission_date)-15) then 1 else 0 end) before_admission,
			(case when f.start_date >= p.admission_date then 1 else 0 end) since_admission
		from observation_fact f
			inner join covid_cohort p 
				on f.patient_num=p.patient_num 
					and f.start_date >= (trunc(p.admission_date)-365)
			inner join covid_med_map m
				on f.concept_cd = m.local_med_code
	) t
	group by med_class;
commit;    

--******************************************************************************
--******************************************************************************
--*** Obfuscate as needed (optional)
--******************************************************************************
--******************************************************************************

--------------------------------------------------------------------------------
-- Blur counts by adding a small random number.
--------------------------------------------------------------------------------
--Commented out below code as we don't want to blur counts.
/*
declare 
        v_obfuscation_blur numeric(8,0);
begin
    select obfuscation_blur into v_obfuscation_blur from covid_config;
	if v_obfuscation_blur > 0 THEN
        
        update covid_daily_counts
            set cumulative_patients_all = cumulative_patients_all + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                cumulative_patients_severe = cumulative_patients_severe + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                cumulative_patients_dead = cumulative_patients_dead + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_in_hosp_on_date = num_pat_in_hosp_on_date + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_in_hospsevere_on_date = num_pat_in_hospsevere_on_date + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur         
        ;commit;  
        update covid_clinical_course
            set num_pat_all_cur_in_hosp = num_pat_all_cur_in_hosp + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_ever_severe_cur_hosp = num_pat_ever_severe_cur_hosp + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur
        ;commit;
        update covid_demographics
            set num_patients_all = num_patients_all + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_patients_ever_severe = num_patients_ever_severe + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur
        ;commit;
        update covid_labs
            set num_patients_all = num_patients_all + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_patients_ever_severe = num_patients_ever_severe + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur
        ;commit;
        update covid_diagnoses
            set num_pat_all_before_admission = num_pat_all_before_admission + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_all_since_admission = num_pat_all_since_admission + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_ever_severe_before_adm = num_pat_ever_severe_before_adm + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_ever_severe_since_adm = num_pat_ever_severe_since_adm + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur
        ;commit;        
        update covid_medications
            set num_pat_all_before_admission = num_pat_all_before_admission + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_all_since_admission = num_pat_all_since_admission + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_ever_severe_before_adm = num_pat_ever_severe_before_adm + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur,
                num_pat_ever_severe_since_adm = num_pat_ever_severe_since_adm + FLOOR(ABS(OWA_OPT_LOCK.CHECKSUM(sys_guid())/2147483648.0)*(v_obfuscation_blur*2+1)) - v_obfuscation_blur
        ;commit;        
    end if;        
end;
*/
--------------------------------------------------------------------------------
-- Mask small counts with -99.
--------------------------------------------------------------------------------
--Commented out below code as we don't want to  Mask small counts.			    
/*declare 
    v_obfuscation_sml_count_mask numeric(8,0); --shortened to under 128 bytes

begin
    select obfuscation_small_count_mask into v_obfuscation_sml_count_mask from covid_config;
	if v_obfuscation_sml_count_mask > 0 THEN
        update covid_daily_counts
            set cumulative_patients_all = (case when cumulative_patients_all<v_obfuscation_sml_count_mask then -99 else cumulative_patients_all end),
                cumulative_patients_severe = (case when cumulative_patients_severe<v_obfuscation_sml_count_mask then -99 else cumulative_patients_severe end),
                cumulative_patients_dead = (case when cumulative_patients_dead<v_obfuscation_sml_count_mask then -99 else cumulative_patients_dead end),
                num_pat_in_hosp_on_date = (case when num_pat_in_hosp_on_date<v_obfuscation_sml_count_mask then -99 else num_pat_in_hosp_on_date end),
                num_pat_in_hospsevere_on_date = (case when num_pat_in_hospsevere_on_date<v_obfuscation_sml_count_mask then -99 else num_pat_in_hospsevere_on_date end)
        ;commit;       
        update covid_clinical_course
            set num_pat_all_cur_in_hosp = (case when num_pat_all_cur_in_hosp<v_obfuscation_sml_count_mask then -99 else num_pat_all_cur_in_hosp end),
                num_pat_ever_severe_cur_hosp = (case when num_pat_ever_severe_cur_hosp<v_obfuscation_sml_count_mask then -99 else num_pat_ever_severe_cur_hosp end)
        ;commit;
        update covid_demographics
            set num_patients_all = (case when num_patients_all<v_obfuscation_sml_count_mask then -99 else num_patients_all end),
                num_patients_ever_severe = (case when num_patients_ever_severe<v_obfuscation_sml_count_mask then -99 else num_patients_ever_severe end)
        ;commit;
        update covid_labs
            set num_patients_all=-99, mean_value_all=-99, stdev_value_all=-99, mean_log_value_all=-99, stdev_log_value_all=-99
            where num_patients_all<v_obfuscation_sml_count_mask
        ;commit;
        update covid_labs
            set num_patients_ever_severe=-99, mean_value_ever_severe=-99, STDEV_VALUE_EVER_SEVERE=-99, mean_log_value_ever_severe=-99, stdev_log_value_ever_severe=-99
            where num_patients_ever_severe<v_obfuscation_sml_count_mask
        ;commit;
        update covid_diagnoses
            set num_pat_all_before_admission = (case when num_pat_all_before_admission<v_obfuscation_sml_count_mask then -99 else num_pat_all_before_admission end),
                num_pat_all_since_admission = (case when num_pat_all_since_admission<v_obfuscation_sml_count_mask then -99 else num_pat_all_since_admission end),
                num_pat_ever_severe_before_adm = (case when num_pat_ever_severe_before_adm<v_obfuscation_sml_count_mask then -99 else num_pat_ever_severe_before_adm end),
                num_pat_ever_severe_since_adm = (case when num_pat_ever_severe_since_adm<v_obfuscation_sml_count_mask then -99 else num_pat_ever_severe_since_adm end)
        ;commit;
        update covid_medications
            set num_pat_all_before_admission = (case when num_pat_all_before_admission<v_obfuscation_sml_count_mask then -99 else num_pat_all_before_admission end),
                num_pat_all_since_admission = (case when num_pat_all_since_admission<v_obfuscation_sml_count_mask then -99 else num_pat_all_since_admission end),
                num_pat_ever_severe_before_adm = (case when num_pat_ever_severe_before_adm<v_obfuscation_sml_count_mask then -99 else num_pat_ever_severe_before_adm end),
                num_pat_ever_severe_since_adm = (case when num_pat_ever_severe_since_adm<v_obfuscation_sml_count_mask then -99 else num_pat_ever_severe_since_adm end)
        ;commit;
        END IF;        
end;
*/
--------------------------------------------------------------------------------
-- To protect obfuscated demographics breakdowns, keep individual sex, age,
--   and race breakdowns, set combinations and the total count to -999.
--------------------------------------------------------------------------------
--Commented out below code as we don't want to  Mask demographics counts.
/*declare
    v_obfuscate_dem numeric(8,0);
begin
    select obfuscation_demographics into v_obfuscate_dem from covid_config;
    if v_obfuscate_dem > 0 THEN
        update covid_demographics
            set num_patients_all = -999, num_patients_ever_severe = -999
            where (case sex when 'all' then 1 else 0 end)
                +(case race when 'all' then 1 else 0 end)
                +(case age_group when 'all' then 1 else 0 end)<>2
        ;commit;        
    END IF;            
end;
*/
--------------------------------------------------------------------------------
-- Delete small counts.
--------------------------------------------------------------------------------
--Commented out below code as we don't want to  Delete small counts			    
/*
declare 
    v_obfuscation_sml_cnt_delete numeric(8,0); --v_obfuscation_small_count_delete: shortened to under 128 bytes
begin
    select obfuscation_small_count_delete into v_obfuscation_sml_cnt_delete from covid_config;
    if v_obfuscation_sml_cnt_delete > 0 THEN
        select obfuscation_small_count_mask into v_obfuscation_sml_cnt_delete from covid_config;
        delete from covid_daily_counts where cumulative_patients_all<v_obfuscation_sml_cnt_delete;commit;
        delete from covid_clinical_course where num_pat_all_cur_in_hosp<v_obfuscation_sml_cnt_delete;commit;
        delete from covid_labs where num_patients_all<v_obfuscation_sml_cnt_delete;commit;
        delete from covid_diagnoses where num_pat_all_before_admission<v_obfuscation_sml_cnt_delete and num_pat_all_since_admission<v_obfuscation_sml_cnt_delete;commit;
        delete from covid_medications where num_pat_all_before_admission<v_obfuscation_sml_cnt_delete and num_pat_all_since_admission<v_obfuscation_sml_cnt_delete;commit;
    end if;
end;
*/
--******************************************************************************
--******************************************************************************
--*** Finish up
--******************************************************************************
--******************************************************************************

--------------------------------------------------------------------------------
-- Set the siteid to a unique value for your institution.
-- * Make sure you are not using another institution's siteid.
-- * The siteid must be no more than 20 letters or numbers.
-- * It must start with a letter.
-- * It cannot have any blank spaces or special characters.
--------------------------------------------------------------------------------
update covid_daily_counts set siteid = (select siteid from covid_config);commit;
update covid_clinical_course set siteid = (select siteid from covid_config);commit;
update covid_demographics set siteid = (select siteid from covid_config);commit;
update covid_labs set siteid = (select siteid from covid_config);commit;
update covid_diagnoses set siteid = (select siteid from covid_config);commit;
update covid_medications set siteid = (select siteid from covid_config);commit;

--------------------------------------------------------------------------------
--Generate data extracts using listed SQLs.
/*
begin
    if exists (select * from covid_config where output_as_csv = 1) then*/
        -- DailyCounts

--      File #1: DailyCounts-BCH.csv
--      spool DailyCounts-BCH.csv;
        select s DailyCountsCSV
            from (
                select 0 i, 'siteid,calendar_date,cumulative_patients_all,cumulative_patients_severe,cumulative_patients_dead,'
                    ||'num_patients_in_hospital_on_this_date,num_patients_in_hospital_and_severe_on_this_date' s from dual
                union all 
                select row_number() over (order by calendar_date) i,
                    siteid
                    ||','||cast(to_char(calendar_date,'YYYY-MM-DD') as varchar(50)) --YYYY-MM-DD
                    ||','||cast(cumulative_patients_all as varchar(50))
                    ||','||cast(cumulative_patients_severe as varchar(50))
                    ||','||cast(cumulative_patients_dead as varchar(50))
                    ||','||cast(num_pat_in_hosp_on_date as varchar(50))
                    ||','||cast(num_pat_in_hospsevere_on_date as varchar(50))
                from covid_daily_counts
                union all 
                select 9999999, '' from dual--Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;
--spool off;

--    File #2: ClinicalCourse-BCH.csv
--    spool ClinicalCourse-BCH.csv ;   
        select s ClinicalCourseCSV
            from (
                select 0 i, 'siteid,days_since_admission,num_patients_all_still_in_hospital,num_patients_ever_severe_still_in_hospital' s from dual
                union all 
                select row_number() over (order by days_since_admission) i,
                    siteid
                    ||','||cast(days_since_admission as varchar(50))
                    ||','||cast(num_pat_all_cur_in_hosp as varchar(50))
                    ||','||cast(num_pat_ever_severe_cur_hosp as varchar(50))
                from covid_clinical_course
                union all 
                select 9999999, '' from dual  --Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;
--spool off;

--    File #3: Demographics-BCH.csv
--    spool Demographics-BCH.csv;   

        select s DemographicsCSV
            from (
                select 0 i, 'siteid,sex,age_group,race,num_patients_all,num_patients_ever_severe' s from dual
                union all 
                select row_number() over (order by sex, age_group, race) i,
                    siteid
                    ||','||cast(sex as varchar(50))
                    ||','||cast(age_group as varchar(50))
                    ||','||cast(race as varchar(50))
                    ||','||cast(num_patients_all as varchar(50))
                    ||','||cast(num_patients_ever_severe as varchar(50))
                from covid_demographics
                union all select 9999999, '' from dual--Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;
    
--spool off;

--    File #4: Labs-BCH.csv
--    spool Labs-BCH.csv;  
        select s LabsCSV
            from (
                select 0 i, 'siteid,loinc,days_since_admission,units,'
                    ||'num_patients_all,mean_value_all,stdev_value_all,mean_log_value_all,stdev_log_value_all,'
                    ||'num_patients_ever_severe,mean_value_ever_severe,stdev_value_ever_severe,mean_log_value_ever_severe,stdev_log_value_ever_severe' s
                from dual    

                union all 
                select row_number() over (order by loinc, days_since_admission) i,
                    siteid
                    ||','||cast(loinc as varchar(50))
                    ||','||cast(days_since_admission as varchar(50))
                    ||','||cast(units as varchar(50))
                    ||','||cast(num_patients_all as varchar(50))
                    ||','||cast(mean_value_all as varchar(50))
                    ||','||cast(stdev_value_all as varchar(50))
                    ||','||cast(mean_log_value_all as varchar(50))
                    ||','||cast(stdev_log_value_all as varchar(50))
                    ||','||cast(num_patients_ever_severe as varchar(50))
                    ||','||cast(mean_value_ever_severe as varchar(50))
                    ||','||cast(STDEV_VALUE_EVER_SEVERE as varchar(50))
                    ||','||cast(mean_log_value_ever_severe as varchar(50))
                    ||','||cast(stdev_log_value_ever_severe as varchar(50))
                from covid_labs
                union all select 9999999, '' from dual--Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;
    
--spool off;

--    File #5: Diagnoses-BCH.csv
--    spool Diagnoses-BCH.csv ; 
        select s DiagnosesCSV
            from (
                select 0 i, 'siteid,icd_code_3chars,icd_version,'
                    ||'num_patients_all_before_admission,num_patients_all_since_admission,'
                    ||'num_patients_ever_severe_before_admission,num_patients_ever_severe_since_admission' s
                from dual    
                union all 
                select row_number() over (order by num_pat_all_since_admission desc, num_pat_all_before_admission desc) i,
                    siteid
                    ||','||cast(icd_code_3chars as varchar(50))
                    ||','||cast(icd_version as varchar(50))
                    ||','||cast(num_pat_all_before_admission as varchar(50))
                    ||','||cast(num_pat_all_since_admission as varchar(50))
                    ||','||cast(num_pat_ever_severe_before_adm as varchar(50))
                    ||','||cast(num_pat_ever_severe_since_adm as varchar(50))
                from covid_diagnoses
                union all select 9999999, '' from dual--Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;

--spool off;

--    File #6: Medications-BCH.csv
--    spool Medications-BCH.csv ; 

        select s MedicationsCSV
            from (
                select 0 i, 'siteid,med_class,'
                    ||'num_patients_all_before_admission,num_patients_all_since_admission,'
                    ||'num_patients_ever_severe_before_admission,num_patients_ever_severe_since_admission' s
                from dual    
                union all 
                select row_number() over (order by num_pat_all_since_admission desc, num_pat_all_before_admission desc) i,
                    siteid
                    ||','||cast(med_class as varchar(50))
                    ||','||cast(num_pat_all_before_admission as varchar(50))
                    ||','||cast(num_pat_all_since_admission as varchar(50))
                    ||','||cast(num_pat_ever_severe_before_adm as varchar(50))
                    ||','||cast(num_pat_ever_severe_since_adm as varchar(50))
                from covid_medications
                union all select 9999999, '' from dual --Add a blank row to make sure the last line in the file with data ends with a line feed.
            ) t
            order by i;
--spool off;

/*    end if;
end*/
