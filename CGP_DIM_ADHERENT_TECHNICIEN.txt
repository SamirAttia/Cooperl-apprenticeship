CREATE EXTERNAL TABLE IF NOT EXISTS cooperl_global_test_bi_db.CGP_DIM_ADHERENT_TECHNICIEN (
	ADT_ID_Adherent_technicien bigint COMMENT 'clé Technique',
	ADT_CODE_adherent_PK int COMMENT 'code adherent',
	ADT_CODE_technicien_pk int COMMENT 'code technicien ',
    ADT_code_activite_pk int COMMENT 'code activité',
    ADT_dath_maj timestamp COMMENT 'date-heure de l insert de l enregistrement '
) comment ' Adherents et techniciens du groupement d eleveurs porcs '
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format '=' 1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_ADHERENT_TECHNICIEN/'
INSERT INTO cooperl_global_test_bi_db.CGP_DIM_ADHERENT_TECHNICIEN 
WITH adt1 AS (

	select 	
             ROW_NUMBER() OVER (
				ORDER BY numero_adherent,
					type_activite,
					numero_technicien
			) AS ID_adherents,	
			adherents_techniciens.numero_adherent AS numero_adherent,
			type_activite,
			adherents_techniciens.numero_technicien
			
		FROM adherents_techniciens
	)
	select 
	-1 as ID_adherents,
	-1 as numero_adherent ,
	-1 as numero_technicien, 
	 -1 as type_activite,
	 now() as date_maj
	 
	Union all 
SELECT 
ID_adherents,
 numero_adherent,
 numero_technicien,
 type_activite,


NOW() AS date_maj
FROM adt1 
	