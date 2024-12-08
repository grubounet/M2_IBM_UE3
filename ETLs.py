import pandas
from datetime import datetime
import psycopg2
import pandas
from pathlib import Path
import os

path_relationship="usagi_vocabulary_RXNORM_EXTENDED_NDC/CONCEPT_RELATIONSHIP.csv"
path_concept="usagi_vocabulary_RXNORM_EXTENDED_NDC/CONCEPT.csv"

table_map_relationship=pandas.read_csv(path_relationship,sep="\t",dtype="str")
table_map_ndc_omop=pandas.read_csv(path_concept,sep="\t",dtype="str")

table_map_ndc_omop_ndc_codes=table_map_ndc_omop.query('domain_id=="Drug" and vocabulary_id=="NDC"')
table_map_ndc_omop_ndc_codes=table_map_ndc_omop_ndc_codes[["concept_id","concept_name","concept_code"]]
table_map_ndc_omop_ndc_codes=table_map_ndc_omop_ndc_codes.rename(columns={"concept_code":"ndc","concept_id":"concept_id_1"})

table_map_ndc_omop_drug_codes=table_map_ndc_omop.query('domain_id=="Drug" and vocabulary_id=="RxNorm"')
table_map_ndc_omop_drug_codes=table_map_ndc_omop_drug_codes[["concept_id","concept_name","concept_code","concept_class_id"]]
table_map_ndc_omop_drug_codes=table_map_ndc_omop_drug_codes.rename(columns={"concept_id":"concept_id_2","concept_name":"omop_standard_name","concept_class_id":"omop_standard_concept_class"})

table_map_relationship_omop=table_map_relationship.merge(table_map_ndc_omop_ndc_codes,on="concept_id_1").merge(table_map_ndc_omop_drug_codes,on="concept_id_2")
table_map_relationship_omop=table_map_relationship_omop.query(f'relationship_id=="Maps to" or relationship_id=="Mapped from"')
table_map_relationship_omop=table_map_relationship_omop.rename(columns={"concept_id_2":"omop_standard_id"})
table_map_relationship_omop=table_map_relationship_omop[["ndc","omop_standard_id","omop_standard_name","omop_standard_concept_class"]]
table_map_relationship_omop=table_map_relationship_omop.drop_duplicates("ndc")
table_map_relationship_omop.to_csv("ndc_to_omop_standard.csv",sep="\t",index=None)

table_map_relationship_is_a=table_map_relationship.query(f'relationship_id=="RxNorm is a"')
table_map_relationship_is_a=table_map_relationship_is_a.rename(columns={"concept_id_1":"omop_standard_id","concept_id_2":"is_a_id"})
table_map_relationship_is_a=table_map_relationship_is_a.merge(table_map_relationship_omop,on="omop_standard_id")
table_map_relationship_is_a=table_map_relationship_is_a.drop_duplicates("omop_standard_id")
table_map_relationship_is_a=table_map_relationship_is_a[["ndc","omop_standard_id","omop_standard_name","is_a_id","omop_standard_concept_class"]]

table_map_relationship_tradename_of=table_map_relationship.query(f'relationship_id=="Tradename of"')
table_map_relationship_tradename_of=table_map_relationship_tradename_of.rename(columns={"concept_id_1":"is_a_id","concept_id_2":"tradename_id"})
table_map_relationship_tradename_of=table_map_relationship_tradename_of.merge(table_map_relationship_is_a,on="is_a_id")
table_map_relationship_tradename_of=table_map_relationship_tradename_of[["ndc","omop_standard_id","omop_standard_name","is_a_id","tradename_id","omop_standard_concept_class"]]

table_map_relationship_ingredient=table_map_relationship.query(f'relationship_id=="RxNorm has ing"')
table_map_relationship_ingredient=table_map_relationship_ingredient.rename(columns={"concept_id_1":"tradename_id","concept_id_2":"ingredient_id"})
table_map_relationship_ingredient=table_map_relationship_ingredient.merge(table_map_relationship_tradename_of,on="tradename_id")
table_map_relationship_ingredient=table_map_relationship_ingredient[["ndc","omop_standard_id","omop_standard_name","is_a_id","tradename_id","ingredient_id","omop_standard_concept_class"]]

rxnorm_ingredients=table_map_ndc_omop.query('domain_id=="Drug" and vocabulary_id=="RxNorm" and concept_class_id=="Ingredient"')
rxnorm_ingredients=rxnorm_ingredients.rename(columns={"concept_name":"ingredient_name",'concept_id':'ingredient_id'})
rxnorm_ingredients=rxnorm_ingredients[["ingredient_name","ingredient_id"]]

table_map_relationship_ingredient=table_map_relationship_ingredient.merge(rxnorm_ingredients,on="ingredient_id")
table_map_relationship_ingredient.to_csv("ndc_to_drugs_mapping.csv",sep="\t",index=None)

table_map_relationship_omop_standard=pandas.read_csv("ndc_to_omop_standard.csv",sep="\t",dtype="str")
table_map_relationship_omop_standard=table_map_relationship_omop_standard.drop_duplicates(subset=["ndc","omop_standard_id"])
table_map_relationship_ingredient=pandas.read_csv("ndc_to_drugs_mapping.csv",sep="\t",dtype="str")
table_map_relationship_ingredient=table_map_relationship_ingredient.drop_duplicates(subset=["ndc","omop_standard_id","ingredient_id"])

mimic_patient=pandas.read_csv("mimic-iii-clinical-database-demo-1.4/PATIENTS.csv",sep=",",dtype="str")
mimic_prescriptions=pandas.read_csv("mimic-iii-clinical-database-demo-1.4/PRESCRIPTIONS.csv",sep=",",dtype="str")
mimic_admission=pandas.read_csv("mimic-iii-clinical-database-demo-1.4/ADMISSIONS.csv",sep=",",dtype="str")

cdm_person=mimic_patient
cdm_person=cdm_person.rename(columns={"subject_id":"person_id"})
cdm_person["gender_concept_id"]=cdm_person.apply(lambda x:"8532" if x["gender"]=="F" else "8507",axis=1)
cdm_person["datetime_dob"]=cdm_person.apply(lambda x:datetime.strptime(x["dob"],"%Y-%m-%d %H:%M:%S"),axis=1)
cdm_person["year_of_birth"]=cdm_person.apply(lambda x:x["datetime_dob"].year,axis=1)
cdm_person["month_of_birth"]=cdm_person.apply(lambda x:x["datetime_dob"].month,axis=1)
cdm_person["day_of_birth"]=cdm_person.apply(lambda x:x["datetime_dob"].day,axis=1)
cdm_person["birth_datetime"]=cdm_person["dob"]
cdm_person["ethnicity_concept_id"]=0
cdm_person["race_concept_id"]=0
cdm_person=cdm_person[["person_id","gender_concept_id","race_concept_id","ethnicity_concept_id","year_of_birth","month_of_birth","day_of_birth","birth_datetime"]]
cdm_person.to_csv("omop_csv/person.csv",sep="\t",index=None)

cdm_death=mimic_patient
cdm_death["death_date"]=cdm_death.apply(lambda x:datetime.strftime(datetime.strptime(x["dod"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_death=cdm_death.rename(columns={"dod":"death_datetime","subject_id":"person_id"})
cdm_death=cdm_death[["person_id","death_date","death_datetime"]]
cdm_death.to_csv("omop_csv/death.csv",sep="\t",index=None)

mimic_prescriptions_copy=mimic_prescriptions
mimic_prescriptions_copy=mimic_prescriptions_copy.dropna(subset=["ndc"])
mimic_prescriptions_copy=mimic_prescriptions_copy[["ndc","drug","prod_strength"]]
mimic_prescriptions_copy=mimic_prescriptions_copy.query('ndc!="0"')

cdm_drug_exposure=mimic_prescriptions
cdm_drug_exposure=cdm_drug_exposure.merge(table_map_relationship_omop_standard,on="ndc",how="inner")
cdm_drug_exposure["drug_type_concept_id"]=0
cdm_drug_exposure=cdm_drug_exposure.dropna(subset=['startdate', 'enddate'])
cdm_drug_exposure["drug_exposure_start_date"]=cdm_drug_exposure.apply(lambda x:datetime.strftime(datetime.strptime(x["startdate"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_drug_exposure["drug_exposure_end_date"]=cdm_drug_exposure.apply(lambda x:datetime.strftime(datetime.strptime(x["enddate"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_drug_exposure=cdm_drug_exposure.rename(columns={"row_id":"drug_exposure_id","subject_id":"person_id","hadm_id":"visit_occurrence_id","omop_standard_id":"drug_concept_id","startdate":"drug_exposure_start_datetime","enddate":"drug_exposure_end_datetime"})
cdm_drug_exposure=cdm_drug_exposure[["person_id","visit_occurrence_id","drug_exposure_id","drug_concept_id","drug_type_concept_id","drug_exposure_start_date","drug_exposure_start_datetime","drug_exposure_end_date","drug_exposure_end_datetime"]]
cdm_drug_exposure.to_csv("omop_csv/drug_exposure.csv",sep="\t",index=None)

cdm_drug_era=mimic_prescriptions
cdm_drug_era=cdm_drug_era.merge(table_map_relationship_ingredient,on="ndc")    
cdm_drug_era=cdm_drug_era.dropna(subset=['startdate', 'enddate'])
cdm_drug_era["drug_era_start_date"]=cdm_drug_era.apply(lambda x:datetime.strftime(datetime.strptime(x["startdate"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_drug_era["drug_era_end_date"]=cdm_drug_era.apply(lambda x:datetime.strftime(datetime.strptime(x["enddate"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_drug_era=cdm_drug_era.rename(columns={"subject_id":"person_id","ingredient_id":"drug_concept_id"})
cdm_drug_era=cdm_drug_era[["person_id","drug_concept_id","drug_era_start_date","drug_era_end_date"]]
cdm_drug_era=cdm_drug_era.reset_index()
cdm_drug_era=cdm_drug_era.rename(columns={"index":"drug_era_id"})
cdm_drug_era.to_csv("omop_csv/drug_era.csv",sep="\t",index=None)

cdm_visit_occurrence=mimic_admission
cdm_visit_occurrence["visit_start_date"]=cdm_visit_occurrence.apply(lambda x:datetime.strftime(datetime.strptime(x["admittime"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_visit_occurrence["visit_end_date"]=cdm_visit_occurrence.apply(lambda x:datetime.strftime(datetime.strptime(x["dischtime"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_visit_occurrence=cdm_visit_occurrence.rename(columns={"subject_id":"person_id","hadm_id":"visit_occurrence_id","admittime":"visit_start_datetime","dischtime":"visit_end_datetime"})
cdm_visit_occurrence["visit_type_concept_id"]=0
cdm_visit_occurrence["visit_concept_id"]=0
cdm_visit_occurrence=cdm_visit_occurrence[["visit_occurrence_id","person_id","visit_concept_id","visit_start_date","visit_start_datetime","visit_end_date","visit_end_datetime","visit_type_concept_id"]]
cdm_visit_occurrence.to_csv("omop_csv/visit_occurrence.csv",sep="\t",index=None)

cdm_observation_period=mimic_admission
cdm_observation_period=cdm_observation_period.rename(columns={"row_id":"observation_period_id","subject_id":"person_id"})
cdm_observation_period["observation_period_start_date"]=cdm_observation_period.apply(lambda x:datetime.strftime(datetime.strptime(x["admittime"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_observation_period["observation_period_end_date"]=cdm_observation_period.apply(lambda x:datetime.strftime(datetime.strptime(x["dischtime"],"%Y-%m-%d %H:%M:%S"),"%Y-%m-%d"),axis=1)
cdm_observation_period["period_type_concept_id"]=0
cdm_observation_period=cdm_observation_period[["observation_period_id","person_id","observation_period_start_date","observation_period_end_date","period_type_concept_id"]]
cdm_observation_period.to_csv("omop_csv/observation_period.csv",sep="\t",index=None)

conn = psycopg2.connect(database="omop_projet",
                        host="127.0.0.1",
                        user="postgres",
                        password="mypass",
                        port="5432")

cursor = conn.cursor()

for omop_table_name in os.listdir("omop_csv"):
    print(f"\n\nquerying table {omop_table_name}\n\n")
    omop_table_path=Path("omop_csv",omop_table_name)
    
    omop_table_data=pandas.read_csv(omop_table_path,sep="\t",keep_default_na=False,na_values=['NaN'])
    omop_table_dict=omop_table_data.to_dict(orient='records')

    sql_attributes=str(tuple(omop_table_dict[0].keys())).replace("'","")
    for row in omop_table_dict:
        query="INSERT INTO public."+omop_table_path.stem+" "+sql_attributes+ " VALUES " + str(tuple(row.values()))    
        cursor.execute(query)
        continue

conn.commit()

