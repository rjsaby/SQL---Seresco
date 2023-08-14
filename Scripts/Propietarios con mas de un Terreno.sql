drop table if exists temp_promedio_area_terreno_calculada_x_ui_lote_6;
drop table if exists temp_terrenos_digitalizados_lote_6_w_area_promedio_x_ui;
drop table if exists conteo_propietarios;
drop table if exists propietarios_verificados;
drop table if exists onteo_propietarios_sin_filtro;
drop table if exists temp_verificacion_tamanio_predio_lote_6;
drop table if exists propietarios_verificados_con_mas_1_terreno_sin_filtro_lote_6;

-- Mapeo de terrenos que superen el área promedio por unidad de intervención
create temp table temp_promedio_area_terreno_calculada_x_ui_lote_6 as
(
	select t1.codigo_municipio
		,t1.codigo_unidad_intervencion
		,round(avg(t1.area_calculada_m2)) promedio_area_calculada_m2_x_ui
	from
	(
		select distinct substring(predio_numero_predial,1,5) codigo_municipio 
			,predio_numero_predial
			,codigo_unidad_intervencion
			,ST_Area(geometria) area_calculada_m2
		from public.terrenos_digitalizados_lote_6
	) t1
	-- En el corto plazo revisar la necesidad de para este análisis, filtrar los resultados por Municipio
	where t1.codigo_municipio = '20250'
	group by codigo_municipio, codigo_unidad_intervencion
);

-- Asignación de valores promedio de área calculada a capa terrenos y filtrado por municipio y ui
create temp table temp_terrenos_digitalizados_lote_6_w_area_promedio_x_ui as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_id_operacion
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.terreno_t_id
		,terrenosdigitalizadoslote6.terreno_area_terreno
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,terrenosdigitalizadoslote6.geometria
		,temppromedioareaterrenocalculadaxuilote6.codigo_municipio
		,temppromedioareaterrenocalculadaxuilote6.codigo_unidad_intervencion temp_codigo_unidad_intervencion
		,temppromedioareaterrenocalculadaxuilote6.promedio_area_calculada_m2_x_ui	
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 inner join temp_promedio_area_terreno_calculada_x_ui_lote_6 temppromedioareaterrenocalculadaxuilote6
	on terrenosdigitalizadoslote6.codigo_unidad_intervencion = temppromedioareaterrenocalculadaxuilote6.codigo_unidad_intervencion
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) = '20250'
);

-- Marcación de Terrenos que superan el promedio de área por unidad de intervención
create temp table temp_verificacion_tamanio_predio_lote_6 as
(
	select distinct predio_numero_predial
			--,terreno_t_id
			--,codigo_unidad_intervencion
			--,codigo_municipio
			--,temp_codigo_unidad_intervencion
			--,promedio_area_calculada_m2_x_ui
			,(case when ST_Area(geometria) >= promedio_area_calculada_m2_x_ui then 'Supera promedio área UI'
				when ST_Area(geometria) < promedio_area_calculada_m2_x_ui then 'No supera promedio área UI'
				else 'Error'
				end) verificacion_tamanio_predio
	from temp_terrenos_digitalizados_lote_6_w_area_promedio_x_ui
);

-- Propietarios verificados (revisados)
create temp table propietarios_verificados as
(
	select distinct numero_predial
		,nombre_completo
	from public.analisis_areas_fmi_lote_6
	where nombre_completo <> 'REVISAR'
);

-- Conteo de propietarios, fin filtro 
create temp table conteo_propietarios_sin_filtro as
(
	select distinct nombre_completo
		,count(*) conteo_propietarios
	from public.analisis_areas_fmi_lote_6
	where nombre_completo <> 'REVISAR'
	group by 1
);

-- Conteo de propietarios, filtrando aquellos con más de un terreno
create temp table conteo_propietarios as
(
	select distinct nombre_completo
		,count(*) conteo_propietarios
	from public.analisis_areas_fmi_lote_6
	where nombre_completo <> 'REVISAR'
	group by 1
	having count(*) > 1
);

-- Creación de Tabla con la verificación de nombres de propietario con más de un terreno 

drop table if exists propietarios_verificados_con_mas_1_terreno_lote_6;

create table propietarios_verificados_con_mas_1_terreno_lote_6 as
(
	select distinct terrenosdigitalizadoslote6.predio_numero_predial
		,propietariosverificados.nombre_completo
		,conteopropietarios.conteo_propietarios
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
	inner join propietarios_verificados propietariosverificados on terrenosdigitalizadoslote6.predio_numero_predial = propietariosverificados.numero_predial
	inner join conteo_propietarios conteopropietarios on propietariosverificados.nombre_completo = conteopropietarios.nombre_completo
);

-- Creación de propietarios sin filtro, previo al enlace con los terrenos que superan el área de la UI - TABLA TEMPORAL
create temp table propietarios_verificados_con_mas_1_terreno_sin_filtro_lote_6 as
(
	select distinct terrenosdigitalizadoslote6.predio_numero_predial
		,propietariosverificados.nombre_completo
		,conteopropietariossinfiltro.conteo_propietarios
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
	inner join propietarios_verificados propietariosverificados on terrenosdigitalizadoslote6.predio_numero_predial = propietariosverificados.numero_predial
	inner join conteo_propietarios_sin_filtro conteopropietariossinfiltro on propietariosverificados.nombre_completo = conteopropietariossinfiltro.nombre_completo
);

-- Unificación de tabla entre propietarios y terrenos que superan el promedio de la UI
drop table if exists propietarios_tamanio_terreno_lote_6;

create table propietarios_tamanio_terreno_lote_6 as
(
select distinct tempverificaciontamanioprediolote6.predio_numero_predial
	,tempverificaciontamanioprediolote6.verificacion_tamanio_predio
	,propietariosverificadosconmas1terrenosinfiltrolote6.nombre_completo
from temp_verificacion_tamanio_predio_lote_6 tempverificaciontamanioprediolote6 
	inner join propietarios_verificados_con_mas_1_terreno_sin_filtro_lote_6 propietariosverificadosconmas1terrenosinfiltrolote6
	on tempverificaciontamanioprediolote6.predio_numero_predial = propietariosverificadosconmas1terrenosinfiltrolote6.predio_numero_predial
);

-- Vista -vw_propietarios_con_mas_1_terreno-
select terrenosdigitalizadoslote6.predio_t_id predio_t_id
		,terrenosdigitalizadoslote6.predio_id_operacion predio_id_operacion
		,terrenosdigitalizadoslote6.predio_numero_predial predio_numero_predial
		,terrenosdigitalizadoslote6.terreno_t_id terreno_t_id
		,terrenosdigitalizadoslote6.geometria geometria
	,propietariosverificadosconmas1terrenolote6.nombre_completo nombre_completo
	,propietariosverificadosconmas1terrenolote6.conteo_propietarios conteo_propietarios
from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 inner join propietarios_verificados_con_mas_1_terreno_lote_6 propietariosverificadosconmas1terrenolote6
																		on terrenosdigitalizadoslote6.predio_numero_predial = propietariosverificadosconmas1terrenolote6.predio_numero_predial

-- Vista Propietario\Terreno que supera el promedio de la UI -vw_propietario_tamanio_terreno_lote_6-
select propietariostamanioterrenolote6.predio_numero_predial predio_numero_predial
	,propietariostamanioterrenolote6.verificacion_tamanio_predio verificacion_tamanio_predio
	,propietariostamanioterrenolote6.nombre_completo nombre_completo
	,terrenosdigitalizadoslote6.geometria																	
from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 inner join propietarios_tamanio_terreno_lote_6 propietariostamanioterrenolote6
																		on terrenosdigitalizadoslote6.predio_numero_predial = propietariostamanioterrenolote6.predio_numero_predial
