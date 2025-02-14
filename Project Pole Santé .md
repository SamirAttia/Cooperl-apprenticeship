
The objective of this project is to develop the CGE Pôle Santé application – a monthly commercial activity tracking system within the Cooperl cooperative.

This application will enable managers within the group to monitor the monthly commercial activity of the health division, providing tools for data collection, analysis, and visualization to support informed strategic decision-making.

The project is based on a already established solution of data modeling unsing AWS redshift as a DWH wich is a very expensive solution on AWS , my intervention was to rebuild the database on AWS Athena wich was written in pyspark on AWS redshift and redeploy the dashboard on AWS quicksight , on AWS the datawherehousing is established on AWS S3 on the notion of semantic layers .

DWH-Projet Pole Santé-181224-130540.pdf : is the final report of the whole project 
MCD_GOLD.drawio : is the model of the database 
(Feuille_1_2024-11-05T12_30_53.pdf,Remises_&_Avoirs_2024-06-21T13_10_59.pdf,Synthèse_2024-06-21T13_01_40.pdf,Tarifs_2024-06-21T13_10_49.pdf,FARMAPRO_2024-06-21T13_06_28.pdf,COOPERL_2024-06-21T13_11_33.pdf) are the dasboard sheets 
All the TXT files starting with CGP are the tables of the model that we created using SQL 


