-- Ejecutado primero el proceso de actualización de capas
-- Si se crea procesamiento almanenado se debe parametrizar todo sitio donde ocurra ****

-- Llamado a Procedimiento Almacenado
call actualizacion_datos_lote_6(); 

drop table if exists temp_condicion_predio_por_ui_lote_6;
drop table if exists temp_clase_suelo_predio_por_ui;
drop table if exists temp_clase_suelo_predio_por_ui_lote_6;
drop table if exists temp_folios_cerrado_abierto_lote_6;
drop table if exists temp_terrenos_propietarios;
drop table if exists temp_centroides_terrenos_lote_6;
drop table if exists temp_vereda_municipio_lote_6;
drop table if exists temp_terrenos_ui_vereda;
drop table if exists temp_gc_centroides_construcciones_lote_6;
drop table if exists temp_gc_construcciones_ui_lote_6;

-- Tabla Física
-- ****
drop table if exists tbl_terrenos_toluviejo_veredas;
drop table if exists tbl_gc_construccion_ui;

-- Condición del Predio
create temp table temp_condicion_predio_por_ui_lote_6 as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
		,condicionpredioporladmlote6.descripcion condicion_predio
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6
	on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
	-- Para Toluviejo 
	-- ****
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70823')
);

-- Clase Suelo
create temp table temp_clase_suelo_predio_por_ui as
(
	select uapredio.t_id predio_t_id
		,uapredio.numero_predial predio_numero_predial
		,tcclasesuelotipo.descripcion 
	from serladm.ua_predio uapredio
	inner join serladm.tc_clasesuelotipo tcclasesuelotipo on uapredio.id_clasesuelotipo = tcclasesuelotipo.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

-- Predio \ Vereda \ UI:
-- (a) Generación de Centroides a Terrenos Digitalizados
create temp table temp_centroides_terrenos_lote_6 as
(
select predio_t_id
	,predio_id_operacion
	,predio_numero_predial
	,terreno_t_id
	,terreno_area_terreno
	,codigo_unidad_intervencion
	,ST_Centroid((st_transform(geometria),9377)) geometry
from public.terrenos_digitalizados_lote_6
-- ****
where predio_numero_predial like ('70823%')
);

-- (b) Selección de Vereda dependiendo del Municipio Lote 6
create temp table temp_vereda_municipio_lote_6 as
(
	select codigo
		,nombre
		,geometria
	from serladm.gc_vereda
	-- ****
	where codigo like ('70823%')
);

-- (c) Construcción de Join Espacial Entre Centroides\Veredas
create temp table temp_terrenos_ui_vereda as
(
select tempcentroidesterrenoslote6.predio_t_id
	,tempcentroidesterrenoslote6.predio_id_operacion
	,tempcentroidesterrenoslote6.predio_numero_predial
	,tempcentroidesterrenoslote6.terreno_t_id
	,tempcentroidesterrenoslote6.terreno_area_terreno
	,tempcentroidesterrenoslote6.codigo_unidad_intervencion
	,tempveredamunicipiolote6.codigo
	,tempveredamunicipiolote6.nombre
from temp_centroides_terrenos_lote_6 tempcentroidesterrenoslote6
left join temp_vereda_municipio_lote_6 tempveredamunicipiolote6 on ST_Intersects(tempcentroidesterrenoslote6.geometry, tempveredamunicipiolote6.geometria)
);

-- (d) Eliminación de nulos
begin transaction;
update temp_terrenos_ui_vereda 
set codigo = 'Urbano\Expansión'
where codigo is null;
commit;

-- (e) Creacion Tabla 
-- ****
create table tbl_terrenos_toluviejo_veredas as
(
	select predio_t_id
		,predio_id_operacion
		,predio_numero_predial
		,terreno_t_id
		,terreno_area_terreno
		,codigo_unidad_intervencion
		,codigo codigo_vereda
		,nombre nombre_vereda
	from temp_terrenos_ui_vereda
);

-- (f) Relación Clase de suelo, Predio y UI, asociado información Veredal
create temp table temp_clase_suelo_predio_por_ui_lote_6 as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		-- ****
		,tblterrenosveredas.codigo_vereda
		,terrenosdigitalizadoslote6.geometria
		,clasesuelopredioporui.descripcion clase_suelo
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join temp_clase_suelo_predio_por_ui clasesuelopredioporui on terrenosdigitalizadoslote6.predio_t_id = clasesuelopredioporui.predio_t_id
	-- ****
	inner join public.tbl_terrenos_toluviejo_veredas tblterrenosveredas on terrenosdigitalizadoslote6.predio_t_id = tblterrenosveredas.predio_t_id 
	-- Para Municipio
	-- ****
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70823')
);

--Estado de Folios
-- Folios Activos\Cerrados
create temp table temp_folios_cerrado_abierto_lote_6 as
(
select terrenosdigitalizadoslote6.predio_t_id
	,terrenosdigitalizadoslote6.predio_numero_predial
	,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
	,estadofoliosmatriculalote6.estado_folio
from public.estado_folios_matricula_lote_6 estadofoliosmatriculalote6
inner join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
	on estadofoliosmatriculalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
-- ****
where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70823')
);

-- Interesados:
-- (1) Interesados Municipio
create temp table temp_terrenos_propietarios as
(
	select clasesuelopredioporui.clase_suelo 
		,clasesuelopredioporui.predio_t_id
		,clasesuelopredioporui.predio_numero_predial
		,clasesuelopredioporui.codigo_unidad_intervencion
		,interesadoslote6.tipo_persona
		,interesadoslote6.documento_identidad
	from temp_clase_suelo_predio_por_ui_lote_6 clasesuelopredioporui inner join public.interesados_lote_6 interesadoslote6
	on clasesuelopredioporui.predio_t_id = interesadoslote6.predio_t_id
);

-- Construcciones Gestor Catastral
-- (1) Preparación de construcciones gestor, generación de centroides y cálculo de área real en [ha]
create temp table temp_gc_centroides_construcciones_lote_6 as
(
	select gcconstruccion.t_id gcconstruccion_t_id
		,gcconstruccion.identificador
		,gcconstruccion.tipo_construccion
		,gcconstruccion.tipo_dominio
		,gcconstruccion.numero_pisos
		,gcconstruccion.numero_sotanos
		,gcconstruccion.numero_mezanines
		,gcconstruccion.numero_semisotanos
		,gcconstruccion.codigo_edificacion
		,gcconstruccion.codigo_terreno
		,gcconstruccion.area_construida
		,gcconstruccion.gc_predio
		,gcprediocatastro.t_id gcprediocatastro_t_id
		,gcprediocatastro.numero_predial
		,gcprediocatastro.numero_predial_anterior
		,(ST_Area(geometria)/10000) area_calculada_construccion_gestor
		,ST_Centroid(gcconstruccion.geometria) geometry
	from serladm.gc_construccion gcconstruccion inner join serladm.gc_prediocatastro gcprediocatastro
	on gcconstruccion.gc_predio = gcprediocatastro.t_id
	where substring(gcprediocatastro.numero_predial,1,5) in ('20250','70713','70823')
);

-- (2) Asignación de UI
create temp table temp_gc_construcciones_ui_lote_6 as
(
	select tempgccentroidesconstruccioneslote6.gcconstruccion_t_id
			,tempgccentroidesconstruccioneslote6.identificador
			,tempgccentroidesconstruccioneslote6.tipo_construccion
			,tempgccentroidesconstruccioneslote6.tipo_dominio
			,tempgccentroidesconstruccioneslote6.numero_pisos
			,tempgccentroidesconstruccioneslote6.numero_sotanos
			,tempgccentroidesconstruccioneslote6.numero_mezanines
			,tempgccentroidesconstruccioneslote6.numero_semisotanos
			,tempgccentroidesconstruccioneslote6.codigo_edificacion
			,tempgccentroidesconstruccioneslote6.codigo_terreno
			,tempgccentroidesconstruccioneslote6.area_construida
			,tempgccentroidesconstruccioneslote6.gc_predio
			,tempgccentroidesconstruccioneslote6.gcprediocatastro_t_id
			,tempgccentroidesconstruccioneslote6.numero_predial
			,tempgccentroidesconstruccioneslote6.numero_predial_anterior
			,tempgccentroidesconstruccioneslote6.area_calculada_construccion_gestor
			,unidadesintervencionmhlote6.unidadintervencion_t_id
			,unidadesintervencionmhlote6.codigo_municipio
			,unidadesintervencionmhlote6.nombre_municipio
			,unidadesintervencionmhlote6.codigo_unidad_intervencion
			,unidadesintervencionmhlote6.descripcion
			,unidadesintervencionmhlote6.estado
			,tempgccentroidesconstruccioneslote6.geometry
	from temp_gc_centroides_construcciones_lote_6 tempgccentroidesconstruccioneslote6
	inner join public.unidades_intervencion_mh_lote_6 unidadesintervencionmhlote6
	on ST_Intersects(tempgccentroidesconstruccioneslote6.geometry, unidadesintervencionmhlote6.geometria)
);

-- (3) Construcción de Tabla -tbl_gc_construccion_ui-
create table tbl_gc_construccion_ui as
(
	select gcprediocatastro_t_id
		,numero_predial
		,numero_predial_anterior
		,gcconstruccion_t_id
		,tipo_construccion
		,area_construida
		,area_calculada_construccion_gestor
		,unidadintervencion_t_id
		,codigo_municipio
		,nombre_municipio
		,codigo_unidad_intervencion
		,descripcion
		,estado
	from temp_gc_construcciones_ui_lote_6
);

-- ************************************************* Estadísticos *************************************************

-- ESTADÍSTICO (2)
-- Estadístico: Total General Predios por su Condición
select condicion_predio
	,count(*) total_predios
from temp_condicion_predio_por_ui_lote_6
group by 1
order by 1;

-- Estadístico: Condición Predio Zona UI
select condicionpredioporuilote6.codigo_unidad_intervencion
	,clasesuelopredioporui.descripcion ui_zona
	,condicionpredioporuilote6.condicion_predio
	,count(*) total_predios
from temp_condicion_predio_por_ui_lote_6 condicionpredioporuilote6 inner join temp_clase_suelo_predio_por_ui clasesuelopredioporui on condicionpredioporuilote6.predio_t_id = clasesuelopredioporui.predio_t_id
group by 1,2,3
order by 1;

-- ESTADÍSTICO (3)
-- Estadístico: Zona UI Vereda Predio
select codigo_unidad_intervencion
	,clase_suelo
	,codigo_vereda
	,count(*) total_predios
	,round(sum(ST_Area(geometria)/10000)) area_total_ha
from temp_clase_suelo_predio_por_ui_lote_6
group by 1, 2, 3
order by 1;

-- ESTADISTICO (4)
select codigo_unidad_intervencion
	,estado_folio
	,count(*) total
from temp_folios_cerrado_abierto_lote_6
where estado_folio <> 'Sin información por BD'
group by 1, 2
order by 1;

-- ESTADISTICO (5)
-- (5.1) Total Predios por UI y Clase de Suelo
select codigo_unidad_intervencion
	,clase_suelo
	,count(*) total
from temp_terrenos_propietarios
group by 1, 2
order by 1;

-- (5.2) Total Interesados por UI y Clase de Suelo
select t1.clase_suelo 
	,t1.codigo_unidad_intervencion
	,count(*) total_interesados
from (
	select clase_suelo
		,codigo_unidad_intervencion
		,predio_t_id
		,documento_identidad
	from temp_terrenos_propietarios
	where documento_identidad <> ' ' and documento_identidad is not null and documento_identidad <> '0'
) t1
group by 1, 2
order by 2;

-- ESTADISTICO (7)
-- (7.1) Construcciones Gestor

-- Cantidad Construcciones
select codigo_unidad_intervencion
	,count(*) total
	from public.tbl_gc_construccion_ui
	-- ****
	where substring(numero_predial,1,5) in ('70823')
group by 1;

-- (7.2) Construcciones Digitalizadas
-- Cantidad de construcciones actualizadas
select codigo_unidad_intervencion
	,count(*) total
	from public.construcciones_digitalizadas_lote_6
-- ****	
where substring(predio_numero_predial,1,5) in ('70823')
group by 1;

-- Sumatoria de áreas de construcciones nuevas por unidades de intervención
select t1.codigo_unidad_intervencion
	,sum(area_calculada)
from (
select codigo_unidad_intervencion
	,(ST_Area(geometria)/10000) area_calculada
from public.construcciones_digitalizadas_lote_6
where substring(predio_numero_predial,1,5) in ('70823')
) t1
group by 1
order by 1;




