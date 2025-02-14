CREATE EXTERNAL TABLE IF NOT EXISTS cooperl_global_test_bi_db.CGP_DIM_TIERS_COOPERL (
	TCO_ID_TIERS_COOPERL bigint COMMENT 'ID Tiers Cooperl autogénéré',
	TCO_ID_SOCIETE int COMMENT 'ID Société autogénéré',
	TCO_CODE_SOCIETE int COMMENT 'Code Société',
	TCO_CODE_TIERS_PK int COMMENT 'Code Tiers',
	TCO_LIB_TIERS string COMMENT 'Libellé Tiers',
	TCO_CODE_LIB_TIERS string COMMENT 'Code-Libellé du Tiers',
	TCO_INDICATIF_MARQUAGE string COMMENT 'Indicatif de marquage',
	TCO_txt_ADRESSE string COMMENT 'Adresse',
	TCO_txt_LOCALITE string COMMENT 'Localité',
	TCO_CODE_POSTAL string COMMENT 'Code Postal',
	TCO_txt_VILLE string COMMENT 'Ville',
	tco_code_tiers_a_facturer int COMMENT 'Code Tiers Cooperl à facturer',
	tco_code_categorie int COMMENT 'Code Catégorie',
	tco_lib_categorie string COMMENT 'Libellé Catégorie',
	tco_code_pays string COMMENT 'Code Pays',
	tco_lib_pays string COMMENT 'Libellé Pays',
	tco_id_zone_geo int COMMENT 'ID autogénéré Zone Géographique',
	tco_code_zone_geo_pk string COMMENT 'Code Zone Géographique',
	est_tiers_cooperl_supprime boolean COMMENT 'Est Supprimé?',
	est_tiers_cooperl_assujetti_tva boolean COMMENT 'Est assujetti à la TVA?',
	tco_est_client_polesante boolean COMMENT 'Est Client Pôle Santé?',
	TCO_DATH_MAJ timestamp COMMENT 'date-heure de l insert de l enregistrement'
) COMMENT 'Tiers Cooperl du Groupement d eleveurs de porcs'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_TIERS_COOPERL/'
INSERT INTO cooperl_global_test_bi_db.CGP_DIM_TIERS_COOPERL
Select 
-1 as 	TCO_ID_TIERS_COOPERL ,
-1 as 	TCO_ID_SOCIETE ,
-1 as 	TCO_CODE_SOCIETE ,
-1 as 	TCO_CODE_TIERS_PK ,
'NRE' as 	TCO_LIB_TIERS,
'NRE'as 	TCO_CODE_LIB_TIERS ,
'NRE' as 	TCO_INDICATIF_MARQUAGE ,
'NRE' as 	TCO_txt_ADRESSe ,
'NRE' as	TCO_txt_LOCALITE,
'NRE' as 	TCO_CODE_POSTAL ,
'NRE' as 	TCO_txt_VILLE ,
-1 as 	TCO_CODE_TIERS_A_FACTURER ,
-1 as 	TCO_CODE_CATEGORIE ,
'NRE' as 	TCO_LIB_CATEGORIE ,
'NRE' as 	TCO_CODE_PAYS ,
'NRE' as 	TCO_LIB_PAYS,
-1  as 	TCO_ZNE_ID_ZONE_GEO ,
'NRE' as 	TCO_ZNE_CODE_ZONE_GEO_PK ,
false as 	EST_tiers_cooperl_SUPPRIME ,
false as 	EST_tiers_cooperl_ASSUJETTI_TVA ,
false as  tco_est_client_polesante,
now() as 	TCO_DATH_MAJ 
Union all
SELECT row_number() OVER(ORDER BY numero_adherent ASC) AS TCO_ID_TIERS_COOPERL,
	cgp_dim_societe.soc_id_societe AS TCO_ID_SOCIETE,
	cgp_dim_societe.soc_code_societe_pk AS TCO_CODE_SOCIETE,
	numero_adherent AS TCO_CODE_TIERS_PK,
	coalesce(nullif(trim(nom_prenom),''), 'NRE') AS TCO_LIB_TIERS,
	CONCAT(cast(numero_adherent AS varchar),'-',coalesce(nullif(trim(nom_prenom),''), 'NRE')) AS TCO_CODE_LIB_TIERS,
	coalesce(nullif(trim(numero_tva),''), 'NRE') AS TCO_INDICATIF_MARQUAGE,
	coalesce(nullif(trim(rue),''), 'NRE') AS TCO_txt_ADRESSE,
	coalesce(nullif(trim(localite),''), 'NRE') AS TCO_txt_LOCALITE,
	coalesce(nullif(trim(code_postal),''), 'NRE') AS TCO_CODE_POSTAL,
	coalesce(nullif(trim(bureau_postal),''), 'NRE') AS TCO_txt_VILLE,
    CASE
    	WHEN numero_adherent_a_facturer = 0 THEN coalesce(numero_adherent,-1)
    	ELSE coalesce(numero_adherent_a_facturer,-1)
    END AS TCO_CODE_TIERS_A_FACTURER,
	coalesce(categorie,-1) AS TCO_CODE_CATEGORIE,
    CASE
	    	WHEN categorie = 0 THEN 'Tiers'
	    	WHEN categorie = 1 THEN 'Naisseur'
	    	WHEN categorie = 2 THEN 'Naisseur/Engraisseur'
    		WHEN categorie = 3 THEN 'Engraisseur'
    		WHEN categorie = 4 THEN 'Naisseur'
    		WHEN categorie = 5 THEN 'Sélectionneur'
    		WHEN categorie = 6 THEN 'Post-Sevreur/Engraisseur'
    		WHEN categorie = 7 THEN 'Naisseur Post/Sevreur'
    		WHEN categorie = 8 THEN 'Post. Sevreur'
    		WHEN categorie = 9 THEN 'Autres' 
    		ELSE 'Non Renseigné'
    END AS TCO_LIB_CATEGORIE,
    CASE
		WHEN adherents_clients_fournisseurs.code_pays IS NULL OR REGEXP_LIKE(adherents_clients_fournisseurs.code_pays,'^[0-9]*$') THEN 'F' 
		ELSE adherents_clients_fournisseurs.code_pays
	END AS TCO_CODE_PAYS,
	libelle_pays AS TCO_LIB_PAYS,
	COALESCE(CGP_DIM_ZONE_GEOGRAPHIQUE.ZNE_ID_ZONE_GEO, -1) as TCO_ZNE_ID_ZONE_GEO,
	COALESCE(CGP_DIM_ZONE_GEOGRAPHIQUE.ZNE_CODE_ZONE_GEO_PK,'-1') as TCO_ZNE_CODE_ZONE_GEO_PK ,
	CAST(CASE
		    	WHEN adherent_supprime = 9 THEN TRUE 
		    	ELSE FALSE
		END AS BOOLEAN) AS EST_tiers_cooperl_SUPPRIME,
	CAST(CASE
			    WHEN assujetti_tva = 9 THEN TRUE 
			    ELSE FALSE
		END AS BOOLEAN) AS EST_tiers_cooperl_ASSUJETTI_TVA,
    case 
        when numero_adherent BETWEEN 10000 AND 69999 OR numero_adherent BETWEEN 80000 AND 89999 then true 
        else false 
    end as tco_est_client_polesante,
	NOW() AS TCO_DATH_MAJ
FROM cooperl_groupement_porc_silver_db.adherents_clients_fournisseurs
Inner JOIN cooperl_global_test_bi_db.cgp_dim_societe ON cgp_dim_societe.soc_code_societe_pk = 3
Inner JOIN cooperl_global_test_bi_db.CGP_DIM_ZONE_GEOGRAPHIQUE ON CGP_DIM_ZONE_GEOGRAPHIQUE.ZNE_CODE_ZONE_GEO_PK = coalesce(nullif(trim(adherents_clients_fournisseurs.zone_geographique),''), '-1')
LEFT JOIN cooperl_groupement_porc_silver_db.pays ON 
	CASE
		WHEN adherents_clients_fournisseurs.code_pays IS NULL
		OR REGEXP_LIKE(adherents_clients_fournisseurs.code_pays,'^[0-9]*$') THEN 'F' 
		ELSE adherents_clients_fournisseurs.code_pays
	END = pays.code_pays
WHERE ((	numero_adherent BETWEEN 10000 AND 69999	OR numero_adherent BETWEEN 80000 AND 89999) --ces codes identifient les clients du pôlesante
AND zone_variables IS NOT NULL)