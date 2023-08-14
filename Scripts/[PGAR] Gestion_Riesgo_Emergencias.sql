-- N�mero de emergencias reportadas anualmente en la jurisdicci�n de la CAR
select [C�digo Municipio
Divipola] codigo_municipio
	,count(*) total_emergencias
into total_emergecias_por_municipio_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where A�o in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009')
group by [C�digo Municipio
Divipola]


select [C�digo Municipio
Divipola] codigo_municipio
	,count(*) total_emergencias
into total_emergecias_por_municipio_2010_2022
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where A�o in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020')
group by [C�digo Municipio
Divipola]

-- N�mero de personas afectadas a causa de fen�menos de origen natural
-- 1999 - 2009 
select *
into #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Tipo de evento] in ('Hidrometeorol�gico','Geol�gico') 
	and ([Personas Afectadas] is not null)) and (A�o in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009'))
	
select [C�digo Municipio
Divipola] codigo_municipio
	,sum([Personas Afectadas]) total_personas_afectadas
into total_personas_efectadas_fenomeno_natural_1999_2009
from #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
group by [C�digo Municipio
Divipola]

-- 2010_2020
select *
into #emergencias_hidrometerologicas_geologicas_personas_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Tipo de evento] in ('Hidrometeorol�gico','Geol�gico') 
	and ([Personas Afectadas] is not null)) and (A�o in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020'))
	
select [C�digo Municipio
Divipola] codigo_municipio
	,sum([Personas Afectadas]) total_personas_afectadas
into total_personas_efectadas_fenomeno_natural_2010_2020
from #emergencias_hidrometerologicas_geologicas_personas_afectadas_2010_2020
group by [C�digo Municipio
Divipola]

-- Hect�reas afectadas productivas por desastres anualmente

-- 1999 - 2009
select [C�digo Municipio
Divipola] codigo_municipio
	,sum([Da�os cultivos Ha]) total_ha_danios_cultivos
into total_ha_afectadas_1999_2009
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Da�os cultivos Ha] is not null and [Da�os cultivos Ha] <> 0) and (A�o in ('1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009'))
group by [C�digo Municipio
Divipola];

-- 2010 - 2020
select [C�digo Municipio
Divipola] codigo_municipio
	,sum([Da�os cultivos Ha]) total_ha_danios_cultivos
into total_ha_afectadas_2010_2020
from SERESCO_Lote_6.dbo.reporte_emergencias_UNGRD
where ([Da�os cultivos Ha] is not null and [Da�os cultivos Ha] <> 0) and (A�o in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020'))
group by [C�digo Municipio
Divipola];

