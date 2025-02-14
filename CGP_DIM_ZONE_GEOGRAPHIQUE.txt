create external Table if not exists cooperl_global_test_bi_db.CGP_DIM_ZONE_GEOGRAPHIQUE (
	zne_id_zone_geo int COMMENT 'Clé technique Zone Géographique',
	zne_code_zone_geo_pk String COMMENT 'Code Zone Géographique',
	zne_lib_zone_geo String COMMENT 'Libellé Zone Géographique',
	zne_code_lib_zone_geo string COMMENT 'Code-Libellé Zone Géographique',
	zne_lib_chef_zone_geo String COMMENT 'Nom & Prénom Chef de Zone',
	zne_code_veterinaire int COMMENT 'Code Vétérinaire',
	zne_lib_veterinaire string COMMENT 'Libellé Vétérinaire',
	zne_code_lib_veterinaire string COMMENT 'Code - Libellé Vétérinaire',
	zne_dath_maj timestamp COMMENT 'Date-heure MAJ de l enregistrement'
) COMMENT 'Zones Géographiques du Groupement d éleveurs de porcs'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://cooperl-gold-dev/groupement_porc/pole_sante/test/CGP_DIM_ZONE_GEOGRAPHIQUE/'
insert into cooperl_global_test_bi_db.CGP_DIM_ZONE_GEOGRAPHIQUE
select 
-1 as zne_id_zone_geo,
'-1' as zne_code_zone_geo,
'NRE' as zne_lib_zone_geo,
'NRE' zne_code_lib_zone_geo,
'NRE' as zne_lib_chef_zone_geo,
-1 as zne_code_veterinaire,
'NRE' as zne_lib_veterinaire,
'NRE' as zne_code_lib_veterinaire,
now() as zne_dath_maj
union all 
select 
row_number() over (order by zone_geographique asc) as zne_id_zone_geo,
zone_geographique as zne_code_zone_geo,
libelle as zne_lib_zone_geo,
concat(zone_geographique, '-', libelle) as zne_code_lib_zone_geo,
coalesce(nullif(trim(chef_de_zone),''),'NRE') as zne_lib_chef_zone_geo,
numero_veto_affecte as zne_code_veterinaire,
coalesce(nullif(trim(nom_technicien),''),'NRE') as zne_lib_veterinaire,
case
when numero_veto_affecte = 0 then 'NRE'
else 
concat(cast(numero_veto_affecte as varchar),'-',coalesce(nullif(trim(nom_technicien),''),'NRE')) 
end as zne_code_lib_veterinaire,
now() as zne_dath_maj
from cooperl_groupement_porc_silver_db.zone_geographique_analyse_groupe
left join cooperl_groupement_porc_silver_db.techniciens_cooperl on numero_technicien = zone_geographique_analyse_groupe.numero_veto_affecte

