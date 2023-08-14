drop table if exists condicion_predio_por_ui_lote_6;
drop table if exists clase_suelo_predio_por_ui;
drop table if exists clase_suelo_predio_por_ui_lote_6;
drop table if exists folios_cerrado_abierto_lote_6;
drop table if exists terrenos_propietarios_el_paso;
drop table if exists terrenos_propietarios_san_onofre;

--

drop table if exists condicion_predio_por_ladm_lote_6;

create table public.condicion_predio_por_ladm_lote_6 as
(
	select distinct uapredio.t_id predio_t_id
		,uapredio.numero_predial predio_numero_predial
		,tccondicionprediotipo.descripcion 
	from serladm.ua_predio uapredio
	inner join serladm.tc_condicionprediotipo tccondicionprediotipo on uapredio.id_condicionpredio = tccondicionprediotipo.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

create temp table condicion_predio_por_ui_lote_6 as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
		,condicionpredioporladmlote6.descripcion condicion_predio
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6
	on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
	-- Para San Onofre
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70713')
);

-- Estadísticos
select condicion_predio
	,count(*) total_predios
from condicion_predio_por_ui_lote_6
group by 1
order by 1;

-- Clase Suelo
create temp table clase_suelo_predio_por_ui as
(
	select uapredio.t_id predio_t_id
		,uapredio.numero_predial predio_numero_predial
		,tcclasesuelotipo.descripcion 
	from serladm.ua_predio uapredio
	inner join serladm.tc_clasesuelotipo tcclasesuelotipo on uapredio.id_clasesuelotipo = tcclasesuelotipo.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

-- Estadístico: Condición Predio Zona UI
select condicionpredioporuilote6.codigo_unidad_intervencion
	,clasesuelopredioporui.descripcion ui_zona
	,condicionpredioporuilote6.condicion_predio
	,count(*) total_predios
from condicion_predio_por_ui_lote_6 condicionpredioporuilote6 inner join clase_suelo_predio_por_ui clasesuelopredioporui on condicionpredioporuilote6.predio_t_id = clasesuelopredioporui.predio_t_id
group by 1,2,3
order by 1;
 
-- Estadístico: Zona UI Vereda Predio
create temp table clase_suelo_predio_por_ui_lote_6 as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,tblterrenossanonofreveredas.codigo_vereda
		,terrenosdigitalizadoslote6.geometria
		,clasesuelopredioporui.descripcion clase_suelo
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join clase_suelo_predio_por_ui clasesuelopredioporui on terrenosdigitalizadoslote6.predio_t_id = clasesuelopredioporui.predio_t_id
	inner join public.tbl_terrenos_san_onofre_veredas tblterrenossanonofreveredas on terrenosdigitalizadoslote6.predio_t_id = tblterrenossanonofreveredas.predio_t_id 
	--inner join public.terrenos_el_paso_veredas_onu terrenoselpasoveredasonu on terrenosdigitalizadoslote6.predio_t_id = terrenoselpasoveredasonu.predio_t_id 
	-- Para San Onofre
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70713')
);

-- Estadísticos
select codigo_unidad_intervencion
	,clase_suelo
	,codigo_vereda
	,count(*) total_predios
	,round(sum(ST_Area(geometria)/10000)) area_total_ha
from clase_suelo_predio_por_ui_lote_6
group by 1, 2, 3
order by 1;

-- Folios Activos\Cerrados
create temp table folios_cerrado_abierto_lote_6 as
(
select terrenosdigitalizadoslote6.predio_t_id
	,terrenosdigitalizadoslote6.predio_numero_predial
	,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
	,estadofoliosmatriculalote6.estado_folio
from public.estado_folios_matricula_lote_6 estadofoliosmatriculalote6
inner join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on estadofoliosmatriculalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id 
where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in ('70713')
);

select codigo_unidad_intervencion
	,estado_folio
	,count(*) total
from folios_cerrado_abierto_lote_6
where estado_folio <> 'Sin información por BD'
group by 1, 2
order by 1;

-- Interesados - El Paso
create temp table terrenos_propietarios_el_paso as
(
	select clasesuelopredioporui.clase_suelo 
		,clasesuelopredioporui.predio_t_id
		,clasesuelopredioporui.predio_numero_predial
		,clasesuelopredioporui.codigo_unidad_intervencion
		,interesadoslote6.tipo_persona
		,interesadoslote6.documento_identidad
	from clase_suelo_predio_por_ui_lote_6 clasesuelopredioporui inner join public.interesados_lote_6 interesadoslote6
	on clasesuelopredioporui.predio_t_id = interesadoslote6.predio_t_id
);

-- Interesados - San Onofre
create temp table terrenos_propietarios_san_onofre as
(
	select clasesuelopredioporui.clase_suelo 
		,clasesuelopredioporui.predio_t_id
		,clasesuelopredioporui.predio_numero_predial
		,clasesuelopredioporui.codigo_unidad_intervencion
		,interesadoslote6.tipo_persona
		,interesadoslote6.documento_identidad
	from clase_suelo_predio_por_ui_lote_6 clasesuelopredioporui inner join public.interesados_lote_6 interesadoslote6
	on clasesuelopredioporui.predio_t_id = interesadoslote6.predio_t_id
);

-- Estadísticos UI\Clase Suelo (El Paso)
select codigo_unidad_intervencion
	,clase_suelo
	,count(*) total
from terrenos_propietarios_el_paso
group by 1, 2
order by 1;

-- Estadísticos UI\Clase Suelo (San Onofre)
select codigo_unidad_intervencion
	,clase_suelo
	,count(*) total
from terrenos_propietarios_san_onofre
group by 1, 2
order by 1;

-- Estadístico ZONA, UI, NPN, Interesados (El Paso)
select t1.clase_suelo 
	,t1.codigo_unidad_intervencion
	,count(*) total_interesados
from (
	select clase_suelo
		,codigo_unidad_intervencion
		,documento_identidad
	from terrenos_propietarios_el_paso
	where documento_identidad <> ' ' and documento_identidad is not null and documento_identidad <> '0'
) t1
group by 1, 2
order by 2;

-- Estadístico ZONA, UI, NPN, Interesados (San Onofre)
select t1.clase_suelo 
	,t1.codigo_unidad_intervencion
	,count(*) total_interesados
from (
	select clase_suelo
		,codigo_unidad_intervencion
		,predio_t_id
		,documento_identidad
	from terrenos_propietarios_san_onofre
	where documento_identidad <> ' ' and documento_identidad is not null and documento_identidad <> '0'
) t1
group by 1, 2
order by 2;

-- Analisis de Áreas Calculadas Vs Registrales (Solo existe análisis para 001 y 006 de El Paso)

select *
from public.verificacion_fisico_juridica_areas_lote_6

-- Construcciones:

-- Cantidad Construcciones
select codigo
	,count(*) total
	from public.tbl_gc_construccion_ui
group by 1;

-- Sumatoria de áreas de gc_construcciones nuevas por unidades de intervención
select t1.codigo
	,sum(area_ha)
from (
select codigo
	,area_ha
from public.tbl_gc_construccion_ui
) t1
group by 1
order by 1;

-- Cantidad de construcciones actualizadas
select codigo_unidad_intervencion
	,count(*) total
	from public.construcciones_digitalizadas_lote_6
where substring(predio_numero_predial,1,5) in ('70713')
group by 1;

-- Sumatoria de áreas de construcciones nuevas por unidades de intervención
select t1.codigo_unidad_intervencion
	,sum(area_calculada)
from (
select codigo_unidad_intervencion
	,(ST_Area(geometria)/10000) area_calculada
from public.construcciones_digitalizadas_lote_6
where substring(predio_numero_predial,1,5) in ('70713')
) t1
group by 1
order by 1;

-- 
select t1.codigo_unidad_intervencion
	,count(*)
from
(
select codigo_unidad_intervencion 
from public.terrenos_digitalizados_lote_6 
where substring(predio_numero_predial,1,5) in ('20250') and codigo_unidad_intervencion in ('001','006')
) t1
group by 1;
