create external Table if not exists cooperl_global_test_bi_db.CGP_DIM_ARTICLE_POLESANTE (
	atp_id_article bigint comment 'Clé Technique article Pôle Santé',
	atp_code_article_pk int comment 'Code article Pôle Santé',
	atp_lib_article string comment 'Libellé article Pôle Santé',
	atp_code_lib_article string comment 'Code-Libellé article Pôle Santé',
	atp_code_categorie int comment 'Code Catégorie article Pôle Santé',
	atp_lib_categorie string comment 'Libellé catégorie article Pôle Santé',
	est_article_supprime boolean comment 'Est supprimé',
	est_article_remise boolean comment 'Est remise',
	atp_id_hierarchie INT comment 'Id hiérarchie article Pôle Santé',
	atp_dath_maj timestamp comment 'Date-heure MAJ de l enregistrement'
) comment 'Table des articles du groupement d éleveurs de porcs'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_ARTICLE_POLESANTE/'
Insert into cooperl_global_test_bi_db.CGP_DIM_ARTICLE_POLESANTE 
With CGP_DIM_ARTICLE_POLESANTE_temp as  (
Select  articles_groupements.numero_article as atp_code_article_pk,
        coalesce(nullif(trim( articles_groupements.libelle_article),''),'-1') as atp_lib_article,
		concat(cast(articles_groupements.numero_article as varchar),'-',coalesce(nullif(trim( articles_groupements.libelle_article),''),'-1')) as atp_code_lib_article,
        Case 	When articles_programme_sanitaire_elevage.numero_article is not NULL Then 1
				When articles_libres.numero_article is not NULL  Then 2 Else -1
		End as atp_code_categorie,
        Case
				When articles_programme_sanitaire_elevage.numero_article is not NULL Then 'Produit pharmacie PSE'
				When articles_libres.numero_article is not NULL Then 'Produit libre pharmacie hors PSE' 
				Else 'Autres'
		End as atp_lib_categorie,
		cast (  Case
				    When articles_groupements.code_enregistrement = 99 Then True 
					Else False
			    End as boolean) as est_article_polesante_supprime,
		cast (	Case 
		            When substring(cast(articles_groupements.numero_article as varchar),3,	1) = '9' or (cast (articles_groupements.numero_article as varchar)) = '22256' Then TRUE 
		        Else FALSE
				End as boolean) as est_article_polesante_remise,
		cooperl_global_test_bi_db.cgp_dim_article_hierarchie_polesante.AHP_ID_HIERARCHIE as atp_id_hierarchie
From cooperl_groupement_porc_silver_db.articles_groupements
Left join cooperl_groupement_porc_silver_db.articles_libres on articles_libres.numero_article = articles_groupements.numero_article
Left join cooperl_groupement_porc_silver_db.articles_farmapro on articles_farmapro.numero_article = articles_groupements.numero_article
Left join cooperl_groupement_porc_silver_db.articles_programme_sanitaire_elevage on articles_programme_sanitaire_elevage.numero_article = articles_groupements.numero_article
--On garde le left car il peut exister dans articles_groupements des correspondances familles/sous-familles qui n'existent pas dans cgp_dim_article_hierarchie_polesante
Left join cooperl_global_test_bi_db.cgp_dim_article_hierarchie_polesante on cgp_dim_article_hierarchie_polesante.ahp_code_famille_pk = coalesce(articles_groupements.code_famille,'-1') and cgp_dim_article_hierarchie_polesante.ahp_code_sous_famille_pk = coalesce(cast(articles_groupements.numero_sous_famille as varchar),'-1') and cgp_dim_article_hierarchie_polesante.ahp_code_sous_sous_famille_pk = coalesce (cast(articles_groupements.numero_sous_sous_famille as varchar),'-1')
--Gestion du OU sur les articles présents dans articles_groupements qui n'auraient pas de correspondance dans les 3 tables testées
Where (articles_farmapro.numero_article is not NULL OR articles_programme_sanitaire_elevage.numero_article is not NULL OR articles_libres.numero_article is not NULL) 
)
Select 
    -1 atp_id_article, 
    -1 as atp_code_article_pk, 
    'Non renseigné' as atp_lib_article,
    'Non renseigné' as atp_code_lib_article, 
    -1 as atp_code_categorie, 
    'Non renseigné' as atp_lib_categorie,
    false as est_article_polesante_supprime,
    false as est_article_polesante_remise, 
    -1 as atp_id_hierarchie, 
    now() as atp_dath_maj
Union all 
Select  row_number () over (Order by atp_code_article_pk) as atp_id_article,
	    atp_code_article_pk,
	    atp_lib_article,
	    atp_code_lib_article,
	    atp_code_categorie as atp_code_categorie,
	    atp_lib_categorie as atp_lib_categorie,
        est_article_polesante_supprime as est_article_polesante_supprime,
	    est_article_polesante_remise as est_article_polesante_remise,
	    atp_id_hierarchie as atp_id_hierarchie,
        now() atp_dath_maj
From CGP_DIM_ARTICLE_POLESANTE_temp 