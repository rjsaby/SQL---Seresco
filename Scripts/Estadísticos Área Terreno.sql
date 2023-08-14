
select t1.codigo_unidad_intervencion
	,t1.intervalos
	,count(*) 'conteo_intervalos'
	into relacion_unidad_intervencion_intervalos_area_terreno_elpaso
	from(
	select  gc_terrenos_pto_AddSpatialJoi_2 'codigo_unidad_intervencion',
		case
			when gc_terrenos_pto_area_ha <= 1 then 'Área Terreno <= 1'
			when gc_terrenos_pto_area_ha > 0 and gc_terrenos_pto_area_ha < 5 then '0 < Área Terreno < 5'
			when gc_terrenos_pto_area_ha >= 5 then 'Área Terreno >= 5'
			else 'Sin Cruce'
		end as 'intervalos'
	from SERESCO_Lote_6.dbo.GC_TERRENOS_PTO
	) t1
group by codigo_unidad_intervencion, t1.intervalos
order by 1;



select *
from SERESCO_Lote_6.dbo.GC_TERRENOS_PTO