SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER VIEW [prod_ds].[view_Sellout_Penetration] AS
(
/*
SELECT  

 [ID Country]
,[ID Sk Tax]
,[ID Subsegment]
,[ID Local Distribuitor]
,[id_dsr] AS 'ID DSR'
,[ID SDFC Account]
,[Employees]
,[FL Employees]
,[Go To Market]
,[Opp Total]
,[Opp Local]
,[Category]
,[Opp Category]
,[Opp Category Local]
,'Penetration' AS 'Opp Type'
,'MKT SOB Penetration 2024' AS 'Version'
FROM prod_ds.cat_SOB_Penetration_large

UNION ALL
*/

SELECT 
--Identifiers
o.id_country AS 'ID Country'
,o.id_cliente AS 'ID Sk Tax'
,o.id_subsegmento AS 'ID Subsegment'
,dsr.id_localidad_distribuidor AS 'ID Local Distribuitor'
,dsr.id_vendedor AS 'ID DSR'
,o.sfdc_account_id AS 'ID SDFC Account'

--Customer Info
,o.empleados AS Employees
,o.fl_empleados AS 'FL Employees'
,o.number_categories AS 'Number Current Categories'
--,o.categories AS 'Current Categories'
,CASE 
	WHEN o.desc_rfm IN  ('Champion','Promising') AND o.customer_opp_total>300 THEN  'NBD'
    WHEN o.customer_opp_total>=1000 AND o.id_pais IN ('CR','GT','PA','CL','EC','PE','BO') THEN 'NBD'
    WHEN o.desc_rfm NOT  IN  ('Champion','Promising') AND o.customer_opp_total>200 THEN 'EM'
    ELSE 'DISCARD' 
	END AS 'Go To_Market'
,o.customer_opp_total AS 'Opp Total'
,ROUND(CASE WHEN exc.imp_exchange_rate IS NULL THEN o.customer_opp_total ELSE exc.imp_exchange_rate*o.customer_opp_total END,0) AS 'Opp Local'

--Fields with category level
,o.category AS Category
,o.monthly_value_NSV AS 'Current Amount Category'
,ROUND(CASE WHEN exc.imp_exchange_rate IS NULL THEN o.monthly_value_NSV ELSE exc.imp_exchange_rate*o.monthly_value_NSV END,0) AS 'Current Amount Category Local'
,o.opportunity_value_final AS 'Opp Category'
,ROUND(CASE WHEN exc.imp_exchange_rate IS NULL THEN o.opportunity_value_final ELSE exc.imp_exchange_rate*o.opportunity_value_final END,0) AS 'Opp Category Local'
,CASE WHEN o.opp_type='Penetración' THEN 'Penetration'
	WHEN o.opp_type = 'Ampliación' THEN 'Expansion'
	ELSE o.opp_type
	END AS 'Opp Type'

FROM prod_ds.Sellout_Penetration o
LEFT JOIN prod_ds.tmp_fac_exchange_rate exc ON exc.id_pais = o.id_pais  AND try_cast(exc.id_anio as int)*100 +exc.id_mes = o.id_periodo
LEFT JOIN prod_ds.fact_main_dsr_distributor dsr ON o.id_cliente = dsr.id_sk_tax AND o.id_periodo = dsr.periodo_datos
WHERE id_periodo = (SELECT MAX(id_periodo)
FROM prod_ds.Sellout_Penetration)
--AND opp_type<>'Sin Opp'

)


GO