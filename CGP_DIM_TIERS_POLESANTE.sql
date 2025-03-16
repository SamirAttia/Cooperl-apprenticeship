CREATE EXTERNAL TABLE IF NOT EXISTS cooperl_global_test_bi_db.CGP_DIM_TIERS_POLESANTE (
	tip_id_tiers bigint COMMENT 'Clé technique tiers Pôle Santé',
	tip_id_societe int COMMENT 'Clé technique Société',
	tip_code_societe_pk int COMMENT 'Code société',
	tip_code_tiers_pk int COMMENT 'Code tiers',
	tip_lib_tiers String COMMENT 'Libellé tiers',
	tip_code_lib_tiers String COMMENT 'Code-libellé tiers',
	tip_indicatif_marquage String COMMENT 'Indicatif MARQUAGE/N° tva',
	tip_txt_adresse string COMMENT 'Adresse',
	tip_txt_localite string COMMENT 'Localité',
	tip_code_postal string COMMENT 'Code Postal',
	tip_txt_ville string COMMENT 'Ville',
	tip_code_tiers_a_facturer int COMMENT 'Code tiers à facturer',
	tip_lib_categorie string COMMENT 'Libellé catégorie',
	tip_code_pays string COMMENT 'Code pays',
	tip_lib_pays string COMMENT 'Libellé pays',
	tip_id_zone_geo int COMMENT 'Clé technique code zone géographique',
	tip_code_zone_geo string COMMENT ' Code zone géographique',
	tip_id_technicien_porc int COMMENT 'ID Technicien porc',
	tip_id_technicien_farmapro int COMMENT 'ID Technicien Farmapro',
	tip_id_veterinaire_porc int COMMENT 'ID Veterinaire Porc',
	est_tiers_polesante_supprime boolean COMMENT 'Est un tiers supprimé?',
	est_tiers_polesante_assujetti_tva boolean COMMENT 'Est assujeti TVA?',
	est_client_polesante boolean COMMENT 'Est client poelsante?',
	tip_dath_maj timestamp COMMENT 'Date-heure MAJ de l enregistrement'
) COMMENT 'Table des tiers du Pôle Santé (Farmapro & Cooperl)'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_TIERS_POLESANTE/'
INSERT INTO cooperl_global_test_bi_db.CGP_DIM_TIERS_POLESANTE 
with CGP_DIM_TIERS_POLESANTE_temp as (
		Select row_number() over(
				order by tfa_id_societe,
					tfa_code_tiers_pk asc
			) as tip_id_tiers,
			tfa_id_societe as tip_id_societe,
			tfa_code_societe as tip_code_societe_pk,
			tfa_code_tiers_pk as tip_code_tiers_pk,
			tfa_lib_tiers as tip_lib_tiers,
			tfa_code_lib_tiers as tip_code_lib_tiers,
			indicatif_marquage as tip_lib_indicatif_marquage,
			tfa_txt_adresse as tip_txt_adresse,
			tfa_txt_localite as tip_txt_localite,
			tfa_code_postal as tip_code_postal,
			tfa_txt_ville as tip_txt_ville,
			tfa_code_tiers_a_facturer as tip_code_tiers_a_facturer,
			case
				when client_farmapro_avec_tarif_cooperl.numero_adherent is not null then 'Filiale Cooperl'
				when tfa_code_societe = 3
				and cast(tfa_code_tiers_pk as varchar) like '39%' then 'Distributeur export'
				when tfa_code_societe = 3
				and tfa_zne_code_zone_geo_pk <> '-1' then 'Eleveur'
				when tfa_code_societe = 9 then tfa_lib_categorie else 'NRE'
			end as tip_lib_categorie,
			tfa_code_pays as tip_code_pays,
			tfa_lib_pays as tip_lib_pays,
			tfa_zne_id_zone_geo as tip_id_zone_geo,
			tfa_zne_code_zone_geo_pk as tip_code_zone_geo,
			est_tiers_farmparo_supprime as est_tiers_polesante_supprime,
			est_tiers_farmapro_assujetti_tva as est_tiers_polesante_assujetti_tva,
		    est_client_polesante,
			now() as tip_dath_maj
		FROM (
				SELECT tfa_id_societe,
					tfa_code_societe,
					tfa_code_tiers_pk,
					tfa_lib_tiers,
					tfa_code_lib_tiers,
					'NRE' AS indicatif_marquage,
					tfa_txt_adresse,
					tfa_txt_localite,
					tfa_code_postal,
					tfa_txt_ville,
					tfa_code_tiers_a_facturer,
					tfa_code_categorie,
					tfa_lib_categorie,
					tfa_code_pays,
					tfa_lib_pays,
					-1 as tfa_zne_id_zone_geo,
					'NRE' as tfa_zne_code_zone_geo_pk,
					est_tiers_farmparo_supprime,
					est_tiers_farmapro_assujetti_tva,
					true as est_client_polesante --tous les tiers farmapro sont des clients du pôle santé 
				FROM cooperl_global_test_bi_db.cgp_dim_tiers_farmapro
				where tfa_id_societe <> -1
				UNION ALL
				SELECT tco_id_societe,
					tco_code_societe,
					tco_code_tiers_pk,
					tco_lib_tiers,
					tco_code_lib_tiers,
					tco_indicatif_marquage,
					tco_txt_adresse,
					tco_txt_localite,
					tco_code_postal,
					tco_txt_ville,
					tco_code_tiers_a_facturer,
					tco_code_categorie,
					tco_lib_categorie,
					tco_code_pays,
					tco_lib_pays,
					tco_id_zone_geo,
					tco_code_zone_geo_pk,
					est_tiers_cooperl_supprime,
					est_tiers_cooperl_assujetti_tva,
					true as est_client_polesante  --tous les tiers cooperl sont des clients du pôle santé 
				FROM cooperl_global_test_bi_db.cgp_dim_tiers_cooperl
				where TCO_ID_SOCIETE <> -1
			) prep_CGP_DIM_TIERS_POLESANTE
			LEFT JOIN cooperl_groupement_porc_silver_db.client_farmapro_avec_tarif_cooperl ON client_farmapro_avec_tarif_cooperl.numero_adherent = prep_CGP_DIM_TIERS_POLESANTE.TFA_CODE_TIERS_PK
	)
select -1 as tip_id_tiers,
	-1 as tip_code_societe_pk,
	-1 as tip_code_tiers_pk,
	-1 as tip_id_societe,
	'NRE' as tip_lib_tiers,
	'NRE' as tip_code_lib_tiers,
	'NRE' as tip_lib_indicatif_marquage,
	'NRE' as tip_txt_adresse,
	'NRE' as tip_txt_localite,
	'NRE' as tip_code_postal,
	'NRE' as tip_txt_ville,
	-1 as tip_code_tiers_a_facturer,
	'NRE' as tip_lib_categorie,
	'NRE' as tip_code_pays,
	'NRE' as tip_lib_pays,
	-1 as tip_id_zone_geo,
	'NRE' as tip_code_zone_geo,
	-1 as tip_id_technicien_porc,
	-1 as tip_id_technicien_farmapro,
	-1 as tip_id_veterinaire_porc,
	false as est_tiers_polesante_supprime,
	false est_tiers_polesante_assujetti_tva,
	false as est_client_polesante ,
	now() as tip_dath_maj
Union ALL
Select tip_id_tiers,
	tip_id_societe,
	tip_code_societe_pk,
	tip_code_tiers_pk,
	tip_lib_tiers,
	tip_code_lib_tiers,
	tip_lib_indicatif_marquage,
	tip_txt_adresse,
	tip_txt_localite,
	tip_code_postal,
	tip_txt_ville,
	tip_code_tiers_a_facturer,
	tip_lib_categorie,
	tip_code_pays,
	tip_lib_pays,
	tip_id_zone_geo,
	tip_code_zone_geo,
	CAST(coalesce(nullif(trim(cast(tcp_id_technicien_porc as varchar)),	''),'-1') AS INT) AS tip_id_technicien_porc,
	CAST(coalesce(nullif(trim(cast(tcf_id_technicien_farmapro as varchar)),''),'-1') AS INT) AS tip_id_technicien_farmapro,
	CAST(coalesce(nullif(trim(cast(vep_id_veterinaire_porc as varchar)),''),'-1') AS INT) AS tip_id_veterinaire_porc,
	est_tiers_polesante_supprime,
	est_tiers_polesante_assujetti_tva,
	est_client_polesante,
	tip_dath_maj
from CGP_DIM_TIERS_POLESANTE_temp
	left join cooperl_global_test_bi_db.cgp_dim_technicien_farmapro on CGP_DIM_TIERS_POLESANTE_temp.tip_code_tiers_pk = cgp_dim_technicien_farmapro.tcf_code_adherent_pk
	left join cooperl_global_test_bi_db.cgp_dim_technicien_porc on CGP_DIM_TIERS_POLESANTE_temp.tip_code_tiers_pk = cgp_dim_technicien_porc.tcp_code_adherent_pk
	left join cooperl_global_test_bi_db.cgp_dim_veterinaire_porc on CGP_DIM_TIERS_POLESANTE_temp.tip_code_tiers_pk = cgp_dim_veterinaire_porc.vep_code_adherent_pk