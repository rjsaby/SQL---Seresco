select cast(submissiondate as date) fecha_sincronzacion
	,(case when substring(identificacion_predio_numero_predial,1,5) = '20250' then 'El Paso'
		   when substring(identificacion_predio_numero_predial,1,5) = '70713' then 'San Onofre'
		   when substring(identificacion_predio_numero_predial,1,5) = '70823' then 'Toluviejo'
		   else 'Error'
		   end) municipio		
	--,submittername
from serladmcampo."MAPHURRICANE"
where substring(identificacion_predio_numero_predial, 1, 5) in ('20250','70713','70823');

create temp table temp_tabletas as
(
select cast(submissiondate as date) fecha_sincronzacion
	,(case when substring(identificacion_predio_numero_predial,1,5) = '20250' then 'El Paso'
		   when substring(identificacion_predio_numero_predial,1,5) = '70713' then 'San Onofre'
		   when substring(identificacion_predio_numero_predial,1,5) = '70823' then 'Toluviejo'
		   else 'Error'
		   end) municipio		
	,submittername tableta
from serladmcampo."MAPHURRICANE"
where substring(identificacion_predio_numero_predial, 1, 5) in ('20250','70713','70823')
);



select fecha_sincronzacion
,TAB0032_L6@seresco.co
,TAB0033_L6@seresco.co
,TAB0034_L6@seresco.co
,TAB0035_L6@seresco.co
,TAB0036_L6@seresco.co
,TAB0037_L6@seresco.co
,TAB0039_L6@seresco.co
,TAB0040_L6@seresco.co
,TAB0041_L6@seresco.co
,TAB0042_L6@seresco.co
,TAB0044_L6@seresco.co
,TAB0045_L6@seresco.co
,TAB0046_L6@seresco.co
,TAB0050_L6@seresco.co
,TAB0051_L6@seresco.co
from
(
select t1.fecha_sincronzacion
,t1.municipio
,t1.tableta
,count(*) sincronizacion_tableta
from
(
select cast(submissiondate as date) fecha_sincronzacion
	,(case when substring(identificacion_predio_numero_predial,1,5) = '20250' then 'El Paso'
		   when substring(identificacion_predio_numero_predial,1,5) = '70713' then 'San Onofre'
		   when substring(identificacion_predio_numero_predial,1,5) = '70823' then 'Toluviejo'
		   else 'Error'
		   end) municipio		
	,submittername tableta
from serladmcampo."MAPHURRICANE"
where substring(identificacion_predio_numero_predial, 1, 5) in ('20250','70713','70823')
) t1
group by 1, 2, 3
order by 3
) fuente
pivot
(
sum(sincronizacion_tableta)
for tableta in (
'TAB0011_L6@seresco.co'
,'TAB0012_L6@seresco.co'
,'TAB0013_L6@seresco.co'
,'TAB0014_L6@seresco.co'
,'TAB0015_L6@seresco.co'
,'TAB0016_L6@seresco.co'
,'TAB0017_L6@seresco.co'
,'TAB0018_L6@seresco.co'
,'TAB0019_L6@seresco.co'
,'TAB0020_L6@seresco.co'
,'TAB0021_L6@seresco.co'
,'TAB0023_L6@seresco.co'
,'TAB0024_L6@seresco.co'
,'TAB0025_L6@seresco.co'
,'TAB0027_L6@seresco.co'
,'TAB0028_L6@seresco.co'
,'TAB0029_L6@seresco.co'
,'TAB0030_L6@seresco.co'
,'TAB0031_L6@seresco.co'
,'TAB0032_L6@seresco.co'
,'TAB0033_L6@seresco.co'
,'TAB0034_L6@seresco.co'
,'TAB0035_L6@seresco.co'
,'TAB0036_L6@seresco.co'
,'TAB0037_L6@seresco.co'
,'TAB0039_L6@seresco.co'
,'TAB0040_L6@seresco.co'
,'TAB0041_L6@seresco.co'
,'TAB0042_L6@seresco.co'
,'TAB0044_L6@seresco.co'
,'TAB0045_L6@seresco.co'
,'TAB0046_L6@seresco.co'
,'TAB0050_L6@seresco.co'
,'TAB0051_L6@seresco.co'
) as pivote;






