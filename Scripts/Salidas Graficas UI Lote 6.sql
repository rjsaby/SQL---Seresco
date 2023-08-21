
drop table if exists temp_terrenos_digitalizados_con_priorizacion;
drop table if exists unidad_intervencion_salidas_graficas;
drop table if exists terrenos_salidas_graficas;

create temp table temp_terrenos_digitalizados_con_priorizacion as
(
	select (case when substring(t1.predio_numero_predial,1,5) = '20250' then 'El Paso'
		when substring(t1.predio_numero_predial,1,5) = '70713' then 'San Onofre'
		when substring(t1.predio_numero_predial,1,5) = '70823' then 'Toluviejo'
		else 'Error'
		end) municipio 
		,t1.predio_t_id
		,t1.predio_numero_predial
		,t1.codigo_unidad_intervencion
		,concat(substring(t1.predio_numero_predial,1,5),t1.codigo_unidad_intervencion) enlaceui
		,t1.seleccion_metodo
		,t1.geometria
	from colombiaseg_lote6.vw_terrenos_priorizacion_predio_metodo_lote_6 t1
	where substring(predio_numero_predial,1,5) <> '70823'
);

-- Geometrías unidades de intervención bajo análisis
create table unidad_intervencion_salidas_graficas as
(
	select t1.enlaceui
		,t2.codigo_municipio
		,t2.nombre_municipio
		,t2.codigo_unidad_intervencion
		,T2.geometria 
	from
	(
	select distinct codigo_mun
		,codigo_uni
		,concat(codigo_mun,codigo_uni) enlaceui
	from colombiaseg_lote6.grilla_ui_salidas_graficas
	) t1 inner join 
	(
	select *
		,concat(codigo_municipio, codigo_unidad_intervencion) enlaceui
	from colombiaseg_lote6.unidades_intervencion_mh_lote_6
	) t2 
	on t1.enlaceui = t2.enlaceui
);

create table terrenos_salidas_graficas as
(
	select distinct t1.predio_t_id
			,t1.predio_numero_predial
			,t1.seleccion_metodo
			,t2.nombre_municipio
			,t2.codigo_unidad_intervencion
			,t1.geometria geom
			,ST_Intersects(t1.geometria, t2.geometria) interseccion		
	from temp_terrenos_digitalizados_con_priorizacion t1, unidad_intervencion_salidas_graficas t2
	where ST_Intersects(t1.geometria, t2.geometria) is true
);