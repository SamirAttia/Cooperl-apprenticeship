CREATE EXTERNAL TABLE IF NOT EXISTS cooperl_global_test_bi_db.CGP_DIM_TIERS_FARMAPRO (
	TFA_ID_TIERS_FARMAPRO bigint COMMENT 'Clé technique',
	TFA_ID_SOCIETE int COMMENT 'Clé technique cdp_dim_societe',
	TFA_CODE_SOCIETE int COMMENT 'Code Société',
	TFA_CODE_TIERS_PK int COMMENT 'Code Tiers Farmapro',
	TFA_LIB_TIERS String COMMENT 'Libellé Tiers Farmapro',
	TFA_CODE_LIB_TIERS String COMMENT 'Code-Libellé Tiers Farmapro',
	TFA_txt_ADRESSE string COMMENT 'Adresse Tiers Farmapro',
	TFA_txt_LOCALITE string COMMENT 'Localité Tiers Farmapro',
	TFA_CODE_POSTAL string COMMENT 'Code Postal Tiers Farmapro',
	TFA_txt_VILLE string COMMENT 'Ville Tiers Farmapro',
	TFA_CODE_TIERS_A_FACTURER int COMMENT 'Code Tiers Farmapro à facturer',
	TFA_CODE_CATEGORIE int COMMENT 'Code Catégorie Tiers Farmapro',
	TFA_LIB_CATEGORIE string COMMENT 'Libellé Catégorie Tiers Farmapro',
	TFA_CODE_PAYS string COMMENT 'Code Pays Tiers Farmapro',
	TFA_LIB_PAYS string COMMENT 'Libellé Pays Tiers Farmapro',
	EST_tiers_farmparo_SUPPRIME boolean COMMENT 'Est un tiers Farmapro supprimé?',
	EST_tiers_farmapro_ASSUJETTI_TVA boolean COMMENT 'Est un tiers Farmapro assujetti TVA?',
	TFA_DATH_MAJ timestamp COMMENT 'date-heure de l insert de l enregistrement'
) COMMENT 'Table des tiers Farmapro'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_TIERS_FARMAPRO/'
INSERT INTO cooperl_global_test_bi_db.CGP_DIM_TIERS_FARMAPRO
with prep_tiers_farmapro as  (SELECT row_number() OVER(
		ORDER BY numero_adherent ASC
	) AS TFA_ID_TIERS_FARMAPRO,
	cgp_dim_societe.soc_id_societe AS TFA_ID_SOCIETE,
	cgp_dim_societe.soc_code_societe_PK AS TFA_CODE_SOCIETE,
	numero_adherent AS TFA_CODE_TIERS_PK,
	nom_prenom AS TFA_LIB_TIERS,
	CONCAT(cast(numero_adherent AS varchar), '-', nom_prenom) AS TFA_CODE_LIB_TIERS,
	rue AS TFA_txt_ADRESSE,
	localite AS TFA_txt_LOCATLITE,
	code_postal AS TFA_CODE_POSTAL,
	bureau_postal AS TFA_txt_VILLE,
CASE
		WHEN numero_adherent_a_facturer = 0 THEN numero_adherent ELSE numero_adherent_a_facturer
	END AS TFA_CODE_TIERS_A_FACTURER,
CASE
		WHEN numero_adherent > 30000 THEN 0 ELSE categorie
	END AS TFA_CODE_CATEGORIE,
CASE
		WHEN cast(numero_adherent AS varchar) LIKE '39%' THEN 'Distributeur Export'
		WHEN numero_adherent > 30000 THEN 'Distributeur FR'
		WHEN categorie = 0 THEN 'Distributeur FR'
		WHEN categorie = 1 THEN 'Grossiste'
		WHEN categorie = 2 THEN 'Eleveur'
		WHEN categorie = 3 THEN 'Cooperl'
	END AS TFA_LIB_CATEGORIE,
CASE
		WHEN adherents_clients_fournisseurs_farmapro.code_pays IS NULL
		OR REGEXP_LIKE(
			adherents_clients_fournisseurs_farmapro.code_pays,
			'^[0-9]*$'
		) THEN 'F' ELSE adherents_clients_fournisseurs_farmapro.code_pays
	END AS TFA_CODE_PAYS,
	libelle_pays AS TFA_LIB_PAYS,
	CAST(
		CASE
			WHEN adherent_supprime = 9 THEN TRUE ELSE FALSE
		END AS BOOLEAN
	) AS EST_tiers_farmapro_SUPPRIME,
	CAST(
		CASE
			WHEN assujetti_tva = 0 THEN FALSE ELSE TRUE
		END AS BOOLEAN
	) AS EST_tiers_farmapro_ASSUJETTI_TVA,
	NOW() AS TFA_DATH_MAJ
FROM cooperl_groupement_porc_silver_db.adherents_clients_fournisseurs_farmapro
	LEFT JOIN cooperl_global_test_bi_db.cgp_dim_societe ON cgp_dim_societe.soc_code_societe_pk = 9
	LEFT JOIN cooperl_groupement_porc_silver_db.pays ON CASE
		WHEN adherents_clients_fournisseurs_farmapro.code_pays IS NULL
		OR REGEXP_LIKE(
			adherents_clients_fournisseurs_farmapro.code_pays,
			'^[0-9]*$'
		) THEN 'F' ELSE adherents_clients_fournisseurs_farmapro.code_pays
	END = pays.code_pays
WHERE nom_prenom IS NOT NULL
	AND zone_variables IS NOT NULL
	AND categorie NOT IN (4, 9))
Select 
-1 as  TFA_ID_TIERS_FARMAPRO,
-1 as  TFA_ID_SOCIETE,
-1 as  TFA_CODE_SOCIETE,
-1 as  TFA_CODE_TIERS_PK,
'NRE' as  TFA_LIB_TIERS,
'NRE' as  TFA_CODE_LIB_TIERS,
'NRE' as TFA_txt_ADRESSE,
'NRE' as TFA_txt_LOCATLITE,
'NRE' as TFA_CODE_POSTAL,
'NRE' as TFA_txt_VILLE,
-1 as TFA_CODE_TIERS_A_FACTURER,
-1 as  TFA_CODE_CATEGORIE,
'NRE' as  TFA_LIB_CATEGORIE,
'NRE' as TFA_CODE_PAYS,
'NRE' as TFA_LIB_PAYS,
false as EST_tiers_farmapro_SUPPRIME,
 false as EST_tiers_farmapro_ASSUJETTI_TVA,
now() as  TFA_DATH_MAJ
union all 
select 
TFA_ID_TIERS_FARMAPRO,
TFA_ID_SOCIETE,
 TFA_CODE_SOCIETE,
TFA_CODE_TIERS_PK,
coalesce( TFA_LIB_TIERS,'NRE'),
coalesce ( TFA_CODE_LIB_TIERS,'NRE'),
coalesce (TFA_txt_ADRESSE,'NRE'),
coalesce (TFA_txt_LOCATLITE,'NRE'),
coalesce (TFA_CODE_POSTAL,'NRE'),
coalesce (TFA_txt_VILLE,'NRE'),
coalesce (TFA_CODE_TIERS_A_FACTURER,-1),
coalesce (TFA_CODE_CATEGORIE,-1),
coalesce (TFA_LIB_CATEGORIE,'NRE'),
coalesce (TFA_CODE_PAYS,'NRE'),
coalesce (TFA_LIB_PAYS,'NRE'),
coalesce (EST_tiers_farmapro_SUPPRIME,false),
coalesce (EST_tiers_farmapro_ASSUJETTI_TVA,false),
now() as  TFA_DATH_MAJ
from prep_tiers_farmapro
