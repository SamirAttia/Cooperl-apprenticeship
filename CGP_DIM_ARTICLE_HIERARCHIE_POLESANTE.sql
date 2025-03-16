create external Table if not exists cooperl_global_test_bi_db.CGP_DIM_ARTICLE_HIERARCHIE_POLESANTE (
	ahp_id_hierarchie bigint comment 'clé technique',
	ahp_code_famille_pk String comment 'Code famille article Pôle Santé',
	ahp_lib_famille String comment 'Libellé famille article Pôle Santé',
	ahp_code_sous_famille_pk String comment 'Code sous-famille article Pôle Santé',
	ahp_lib_sous_famille string comment 'Libellé sous-famille article Pôle Santé',
	ahp_code_sous_sous_famille_pk string comment 'Code sous-sous-famille article Pôle Santé',
	ahp_lib_sous_sous_famille string comment 'Libellé sous-sous-famille article Pôle Santé',
	ahp_dath_maj timestamp comment 'Date-heure MAJ de l enregistrement'
) COMMENT 'Table des hiérarchies articles du Pôle Santé'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_ARTICLE_HIERARCHIE_POLESANTE/'
Insert into cooperl_global_test_bi_db.CGP_DIM_ARTICLE_HIERARCHIE_POLESANTE
Select  -1 as ahp_id_hierarchie,
		'-1' as ahp_code_famille_pk,
		'Non renseigné' as ahp_lib_famille,
		'-1' as ahp_code_sous_famille_pk,
		'Non renseigné' as ahp_lib_sous_famille,
		'-1' as ahp_code_sous_sous_famille_pk,
		'Non renseigné' as ahp_lib_sous_sous_famille,
		now () as ahp_dath_maj
Union All
Select ROW_NUMBER() Over (
			    	Order by sous_familles_articles_veterinaires.code_famille,
					sous_familles_articles_veterinaires.numero_sous_famille,
					sous_sous_familles_articles_veterinaires.numero_sous_sous_famille) AS ahp_id_hierarchie,
	    coalesce(nullif(trim(familles_articles_veterinaires.code_famille),''),'-1') as ahp_code_famille_pk,
		coalesce(nullif(trim(familles_articles_veterinaires.libelle_famille),''),'Non renseigné') as ahp_lib_famille,
		coalesce(nullif(trim(cast(sous_familles_articles_veterinaires.numero_sous_famille as varchar)),''),'-1') as ahp_code_sous_famille_pk,
		coalesce(nullif(trim(sous_familles_articles_veterinaires.libelle_sous_famille),''), 'Non renseigné') as ahp_lib_sous_famille,
		coalesce(nullif(trim(cast(sous_sous_familles_articles_veterinaires.numero_sous_sous_famille as varchar)),''),'-1') as ahp_code_sous_sous_famille_pk,
		coalesce(nullif(trim(sous_sous_familles_articles_veterinaires.libelle_sous_sous_famille),''), 'Non renseigné') as ahp_lib_sous_sous_famille,
		now () as ahp_dath_maj
From cooperl_groupement_porc_silver_db.familles_articles_veterinaires
Left join cooperl_groupement_porc_silver_db.sous_familles_articles_veterinaires on familles_articles_veterinaires.code_famille = sous_familles_articles_veterinaires.code_famille
Left join cooperl_groupement_porc_silver_db.sous_sous_familles_articles_veterinaires on sous_familles_articles_veterinaires.code_famille = sous_sous_familles_articles_veterinaires.code_famille
			and sous_familles_articles_veterinaires.numero_sous_famille = sous_sous_familles_articles_veterinaires.numero_sous_famille
-- 			on a recréer la table sous_sous_familles_articles_veterinaires dans la base cooperl_global_test_bi_db puisque dans le silver y'en a des doublons dans la table qui proviennent de l' AS400 
Where familles_articles_veterinaires.code_famille <> '0'