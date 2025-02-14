CREATE EXTERNAL TABLE IF NOT EXISTS cooperl_global_test_bi_db.CGP_DIM_VETERINAIRE_Bovin (
	VTB_id_veterinaire_Bovin INT COMMENT 'Clé Technique',
	VTB_code_adherent_PK INT COMMENT 'Code adhérent',
	VTB_id_adherent_technicien INT COMMENT 'ID adhérent technicien',
	VTB_CODE_technicien_PK INT COMMENT 'Code technicien',
	VTB_nom_technicien STRING COMMENT 'Nom technicien',
	VTB_code_site_rattachement INT COMMENT 'Code site rattachement',
	VTB_tel_portable STRING COMMENT 'Téléphone portable',
	VTB_adresse_email STRING COMMENT 'Adresse e-mail',
	VTB_code_activite INT COMMENT 'Code activité',
	VTB_lib_activite_Bovin STRING COMMENT 'Libellé activité',
	VTB_datH_maj_Bovin TIMESTAMP COMMENT 'Date-heure de l insert de l enregistrement'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_VETERINAIRE_Bovin/'
Insert into cooperl_global_test_bi_db.CGP_DIM_VETERINAIRE_Bovin


	with prep_vet as  (SELECT DISTINCT row_number() over() as TCG_id_technicien_groupement,
			numero_adherent as tec_code_adherent_pk ,
		numero_technicien as 	tec_code_technicien,
			CGP_DIM_Technicien.Tec_nom_technicien,
			CGP_DIM_Technicien.tec_code_site_rattachement,
			CGP_DIM_Technicien.TEC_tel_portable,
			CGP_DIM_Technicien.tec_adresse_email,
			type_activite as tec_code_activite,
			cooperl_groupement_porc_silver_db.activites_techniciens.libelle,
			now() as dath
		FROM cooperl_groupement_porc_silver_db.adherents_techniciens
			inner JOIN cooperl_global_test_bi_db.CGP_DIM_Technicien ON cgp_dim_technicien.tec_code_technicien_pk = cooperl_groupement_porc_silver_db.adherents_techniciens.numero_technicien
			inner JOIN cooperl_groupement_porc_silver_db.activites_techniciens ON cooperl_groupement_porc_silver_db.activites_techniciens.code_activite = adherents_techniciens.type_activite
		WHERE cooperl_groupement_porc_silver_db.adherents_techniciens.type_activite = 92 and tec_profil_supprimer=false
		
	)
select 
-1 as TCG_id_technicien_groupement,
-1 as tec_code_adherent_pk,
-1 as tec_code_technicien,
'NRE' as Tec_nom_technicien,
-1 as 	tec_code_site_rattachement,
'NRE' as TEC_tel_portable,
'NRE' as tec_adresse_email,
-1 as tec_code_activite,
'NRE' as libelle,
			now() as dath_maj
Union all 
select 
TCG_id_technicien_groupement,
tec_code_adherent_pk,
tec_code_technicien,
coalesce (Tec_nom_technicien,'NRE'),
coalesce (tec_code_site_rattachement,-1),
coalesce (TEC_tel_portable,'NRE'),
coalesce (tec_adresse_email,'NRE'),
coalesce(tec_code_activite,-1),
coalesce(libelle,'NRE'),
			now() as dath_maj
from prep_vet