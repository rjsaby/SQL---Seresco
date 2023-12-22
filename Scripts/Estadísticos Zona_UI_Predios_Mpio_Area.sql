select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Espacolo35');
call colombiaseg_lote6.dblink_bd_maphurricane();

select distinct clase.descripcion clase_suelo
	,terreno.codigo_unidad_intervencion
	,count(predio.numero_predial) total_predios 
	,(case when predio.municipio = '250' then 'El Paso'
		when predio.municipio = '713' then 'San Onofre'
		when predio.municipio = '823' then 'Toluviejo'
		else 'Sin información'
		end) nombre_municipio	
	,sum(st_area(terreno.geometria)/10000) area_terreno_ha	
from dblink_ua_predio predio
inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terreno on predio.t_id = terreno.predio_t_id
inner join dblink_tc_clasesuelotipo clase on predio.id_clasesuelotipo = clase.t_id
where predio.municipio in ('250','713','823')
group by 1, 2, 4
order by 1, 4;


drop table if exists terrenos_destinacion_economica_lote_6;
create table terrenos_destinacion_economica_lote_6 as
(
	select distinct predio.t_id predio_t_id
		,terreno.terreno_t_id	
		,terreno.codigo_unidad_intervencion
		,predio.numero_predial
		,clase.descripcion clase_suelo
		,desti.descripcion destinacion_economica
		,(case when predio.municipio = '250' then 'El Paso'
			when predio.municipio = '713' then 'San Onofre'
			when predio.municipio = '823' then 'Toluviejo'
			else 'Sin información'
			end) nombre_municipio	
		,st_area(terreno.geometria)/10000 area_terreno_ha
		,terreno.geometria
	from dblink_ua_predio predio
	inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terreno on predio.t_id = terreno.predio_t_id
	inner join dblink_tc_clasesuelotipo clase on predio.id_clasesuelotipo = clase.t_id
	inner join dblink_tc_destinacioneconomicatipo desti on predio.id_destinacioneconomicatipo = desti.t_id
	where predio.municipio in ('250','713','823')
	order by 3, 2
);
