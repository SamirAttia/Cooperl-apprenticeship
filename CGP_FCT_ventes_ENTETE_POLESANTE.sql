create external Table if not exists cooperl_global_test_bi_db.CGP_FCT_ventes_ENTETE_POLESANTE (
    entete_id int comment 'clé technique',
	code_societe_pk int comment 'code société ',
	annee_comptable_pk int comment 'année comptable',
	mois_comptable_pk int comment 'mois comptable ',
	num_facture_pk bigint comment 'numéro facture',
	date_facture date comment 'date facture',
	code_client bigint comment 'code client',
	mtt_hors_taxe float comment 'montant hors taxe',
	mtt_tva float comment 'montant tva',
	mtt_toutes_taxes_comprises float comment 'montant toutes taxe comprises',
	date_echeance date comment "date échéance ",
	code_mode_reglement int comment "code mode réglement" ,
	dath_maj timestamp comment 'date-heure de MAJ de la table'
) comment "tables de entete des ventes "
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_FCT_ventes_ENTETE_POLESANTE/'
Insert into cooperl_global_test_bi_db.CGP_FCT_ventes_ENTETE_POLESANTE 


 with prepare_main_factures_mensuelles_groupement as (
select 3 as code_societe_pk,
            2000 + annee_comptable as annee_comptable_pk,
            mois_comptable as mois_comptable_pk,
            numero_facture as num_facture_pk,
			date_facture,
		    client_id as code_client,
			base_hors_taxe_exonere + base_hors_taxe_tva_5_5 + base_hors_taxe_tva_10 + base_hors_taxe_tva_20 as mtt_hors_taxe,
			montant_tva_5_5 + montant_tva_10 + montant_tva_20 as mtt_tva,
			montant_toutes_taxes_comprises as mtt_toutes_taxes_comprises ,
			case 
			    when lpad(cast(date_echeance as varchar), 6, '0') = '999999' or  nullif(trim(cast(date_echeance as varchar)),'') is null then cast('1900-01-01' as date) 
			    else cast(date_parse(lpad(cast(date_echeance as varchar), 6, '0'),'%d%m%y') as date) 
			    end as date_echeance,
			coalesce(mode_reglement,-1) as code_mode_reglement
From cooperl_groupement_porc_silver_db.factures_mensuelles_groupement
where extract(year From date_facture) > year(current_date) -3 -- on prend les 3 dernières années
),
prepare_main_factures_mensuelles_farmapro as ( 
select 9 as code_societe_pk ,
            2000 + annee_comptable as annee_comptable_pk,
            mois_comptable as mois_comptable_pk,
            numero_facture as num_facture_pk,
			date_facture,
		    client_id as code_client,
			base_hors_taxe_exonere + base_hors_taxe_tva_5_5 + base_hors_taxe_tva_10 + base_hors_taxe_tva_20 as mtt_hors_taxe,
			montant_tva_5_5 + montant_tva_10 + montant_tva_20 as mtt_tva,
			montant_toutes_taxes_comprises as mtt_toutes_taxes_comprises ,
			case 
			    when lpad(cast(date_echeance as varchar), 6, '0') = '999999' or  nullif(trim(cast(date_echeance as varchar)),'') is null then cast('1900-01-01' as date) 
			    else cast(date_parse(lpad(cast(date_echeance as varchar), 6, '0'),'%d%m%y') as date) 
			    end as date_echeance ,
			coalesce(mode_reglement,-1) as code_mode_reglement
From cooperl_groupement_porc_silver_db.factures_mensuelles_farmapro
where extract(year	From date_facture) > year(current_date) -3 -- on prend les 3 dernières années
),
prepare_main_factures_annuelles_groupement as (
Select 3 as code_societe_pk,
            2000 + annee_comptable as annee_comptable_pk,
            mois_comptable as mois_comptable_pk,
            numero_facture as num_facture_pk,
			date_facture,
		    client_id as code_client,
			base_hors_taxe_exonere + base_hors_taxe_tva_5_5 + base_hors_taxe_tva_10 + base_hors_taxe_tva_20 as mtt_hors_taxe,
			montant_tva_5_5 + montant_tva_10 + montant_tva_20 as mtt_tva,
			montant_toutes_taxes_comprises as mtt_toutes_taxes_comprises ,
			case 
			    when lpad(cast(date_echeance as varchar), 6, '0') = '999999' or  nullif(trim(cast(date_echeance as varchar)),'') is null then cast('1900-01-01' as date) 
			    else cast(date_parse(lpad(cast(date_echeance as varchar), 6, '0'),'%d%m%y') as date) 
			    end as date_echeance,
			coalesce(mode_reglement,-1) as code_mode_reglement
From cooperl_groupement_porc_silver_db.factures_annuelles_groupement
where extract(year From date_facture) > year(current_date) -3 -- on prend les 3 dernières années
),
ventes_entete as (
select * from prepare_main_factures_mensuelles_groupement
union all 
select * from prepare_main_factures_mensuelles_farmapro
union all 
select * from prepare_main_factures_annuelles_groupement
 order by date_facture,num_facture_pk
)
 select  row_number() over( order by annee_comptable_pk) as entete_id,*,now() as dath_maj from ventes_entete 
where cast(num_facture_pk as varchar) like '20%'
order by (annee_comptable_pk,mois_comptable_pk,num_facture_pk)
