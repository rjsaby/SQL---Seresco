-- Número de emergencias reportadas anualmente en la jurisdicción de la CAR
select [Código Municipio
Divipola] codigo_municipio
	,count(*) total_emergencias
into total_emergecias_por_municipio_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where Año in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009')
group by [Código Municipio
Divipola]


select [Código Municipio
Divipola] codigo_municipio
	,count(*) total_emergencias
into total_emergecias_por_municipio_2010_2022
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where Año in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020')
group by [Código Municipio
Divipola]

-- Número de personas afectadas a causa de fenómenos de origen natural
-- 1999 - 2009 
select *
into #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Tipo de evento] in ('Hidrometeorológico','Geológico') 
	and ([Personas Afectadas] is not null)) and (Año in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009'))
	
select [Código Municipio
Divipola] codigo_municipio
	,sum([Personas Afectadas]) total_personas_afectadas
into total_personas_efectadas_fenomeno_natural_1999_2009
from #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
group by [Código Municipio
Divipola]

-- 2010_2020
select *
into #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Tipo de evento] in ('Hidrometeorológico','Geológico') 
	and ([Personas Afectadas] is not null)) and (Año in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020'))
	
select [Código Municipio
Divipola] codigo_municipio
	,sum([Personas Afectadas]) total_personas_afectadas
into total_personas_efectadas_fenomeno_natural_2010_2020
from #emergencias_hidrometerologicas_geologicas_personas_afectadas_2010_2020
group by [Código Municipio
Divipola]

-- Hectáreas afectadas productivas por desastres anualmente

-- 1999 - 2009
select [Código Municipio
Divipola] codigo_municipio
	,sum([Daños cultivos Ha]) total_ha_danios_cultivos
into total_ha_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Daños cultivos Ha] is not null and [Daños cultivos Ha] <> 0) and (Año in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009'))
group by [Código Municipio
Divipola];

-- 2010 - 2020
select [Código Municipio
Divipola] codigo_municipio
	,sum([Daños cultivos Ha]) total_ha_danios_cultivos
into total_ha_afectadas_2010_2020
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Daños cultivos Ha] is not null and [Daños cultivos Ha] <> 0) and (Año in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020'))
group by [Código Municipio
Divipola];

