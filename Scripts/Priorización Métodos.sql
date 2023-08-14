drop table if exists terreno_predio_gc;
drop table if exists terreno_digitalizado_area_calculada;
drop table if exists resultados_diferencias_areas_gc_dig_terrenos;
drop table if exists base_analisis_area;
drop table if exists terrenos_predio_w_id_interesado;
drop table if exists terrenos_predio_w_interesado_sin_estandarizar;
drop table if exists vericacion_area_juridica;
drop table if exists predios_con_derivados;
drop table if exists areas_digitalizadas;
drop table if exists reporte_sin_estandarizar;
drop table if exists posibles_informalidades_lote_6;
drop table if exists construccion_validadores_reporte;
drop table if exists reporte_estandarizado;

-- Análisis Áreas Digitalizadas
create temp table terreno_predio_gc as
(
	select gcprediocatastro.numero_predial
		,gcterreno.area_terreno_alfanumerica
		,gcterreno.area_terreno_digital
		,ST_Area(geometria) area_terreno_calculada_gc
		,gcterreno.geometria 
	from serladm.gc_prediocatastro gcprediocatastro
	left join serladm.gc_terreno gcterreno on gcprediocatastro.t_id = gcterreno.gc_predio
	where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
);

create temp table terreno_digitalizado_area_calculada as
(
	select predio_t_id
		,predio_numero_predial
		,codigo_unidad_intervencion
		,ST_Area(geometria) area_terreno_calculada_dig
		--,geometria 
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
);

create temp table resultados_diferencias_areas_gc_dig_terrenos as
(
select terrenosdigitalizadoslote6.predio_t_id
	,terrenosdigitalizadoslote6.predio_numero_predial
	,terrenosdigitalizadoslote6.area_terreno_calculada_dig
	,terrenoprediogc.area_terreno_calculada_gc
	,round(abs(area_terreno_calculada_gc-area_terreno_calculada_dig)) diferencia
	,(round(abs(area_terreno_calculada_gc-area_terreno_calculada_dig))*100)/terrenoprediogc.area_terreno_digital porcentaje_diferencia
	--,terrenosdigitalizadoslote6.geometria
from terreno_digitalizado_area_calculada terrenosdigitalizadoslote6
inner join terreno_predio_gc terrenoprediogc 
on terrenosdigitalizadoslote6.predio_numero_predial = terrenoprediogc.numero_predial
);

-- Análisis Terrenos Superiores en Área o Nacidos en la digitalizacion
create temp table base_analisis_area as
(
	select distinct predio_t_id
		,predio_numero_predial
		,area_terreno_calculada_dig
		,area_terreno_calculada_gc
		,diferencia
		,porcentaje_diferencia
		,(case when porcentaje_diferencia > 9 then 'Si'
			   when porcentaje_diferencia <= 9 then 'No'
			   else 'No Aplica'
			   end) superior_en_area_estandar
		,(case when area_terreno_calculada_gc is null then 'Si'
			   when area_terreno_calculada_gc is not null then 'No'
			   else 'No Aplica'
			   end) terrenos_nuevos_digitalizacion
	from resultados_diferencias_areas_gc_dig_terrenos
);

-- **** Sin Interesados
-- Terrenos\Predios Vs Relación Interesados Predio
create temp table terrenos_predio_w_id_interesado as
(
	select terrenosdigitalizadoslote6.*
		,relprediointeresado.id_interesado 
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join serladm.rel_prediointeresado  relprediointeresado
	on terrenosdigitalizadoslote6.predio_t_id = relprediointeresado.id_predio
);

-- Terrenos\Predios Vs Interesados
create temp table terrenos_predio_w_interesado_sin_estandarizar as
(
	select terrenosprediowidinteresado.predio_t_id
		,terrenosprediowidinteresado.terreno_t_id
		,intinteresado.t_id t_id_interesado
		,intinteresado.id_interesadotipo
		,intinteresado.id_interesadodocumentotipo
		,intinteresado.documento_identidad
		,intinteresado.primer_nombre
		,intinteresado.segundo_nombre
		,intinteresado.primer_apellido
		,intinteresado.segundo_apellido
		,intinteresado.id_sexotipo
		,intinteresado.id_grupoetnico
		,intinteresado.razon_social
		,intinteresado.nombre
		,intinteresado.grupo_etnico2
		,intinteresado.id_estadociviltipo
	from terrenos_predio_w_id_interesado terrenosprediowidinteresado
	inner join serladm.int_interesado intinteresado
	on terrenosprediowidinteresado.id_interesado = intinteresado.t_id
);

-- Posibles Informalidades
drop table if exists condicion_predio_por_npn_lote_6;

create table condicion_predio_por_npn_lote_6 as
(
	select predio_t_id
		,predio_numero_predial
		,(case when substring(predio_numero_predial,21,1) = '0' then 'NPN'
			   when substring(predio_numero_predial,21,1) = '3' then 'Bienes de Uso Público'
			   when substring(predio_numero_predial,21,1) = '4' then 'Vías'
			   when substring(predio_numero_predial,21,1) = '5' then 'Mejoras'
			   when substring(predio_numero_predial,21,1) = '7' then 'Parque Cementerio'
			   when substring(predio_numero_predial,21,1) = '8' then 'Condominio'
			   when substring(predio_numero_predial,21,1) = '9' then 'PH'
			   else 'Sin información'
			   end
		) condicion_predio
	from public.terrenos_digitalizados_lote_6
);

create temp table posibles_informalidades_lote_6 as
(
	select predio_t_id
		,predio_numero_predial
		,'Si' posible_informalidad
	from condicion_predio_por_npn_lote_6
	where condicion_predio = 'Mejoras'
);

-- Verificaciones frente a áreas registrales
create temp table vericacion_area_juridica as
(
select distinct predio_t_id
	,predio_numero_predial
	,'Si' verificacion_area_registral
from public.verificacion_fisico_juridica_areas_lote_6
where verificacion_linderos_o_info_juridica = 'Requiere'
);

-- Verificaciones frente a derivados
create temp table predios_con_derivados as
(
	select distinct uapredio.t_id predio_t_id
		,uapredio.numero_predial predio_numero_predial
		,'Si' predios_con_derivados
	from public.folio_matriz_folio_derivado_lote_6 foliomatrizfolioderivadolote6
	inner join serladm.ua_predio uapredio
	on foliomatrizfolioderivadolote6.folio_matricula = uapredio.matricula_inmobiliaria
	where uapredio.matricula_inmobiliaria is not null
);

drop table if exists temp_preparacion_construcciones_gestor;

-- 7mo INDICADOR: Construcciones
-- (1) Se prepara la información básica y se interrelaciona cada construcción con su respectivo predio
-- Todo esto solo para Lote 6
create temp table temp_preparacion_construcciones_gestor as
	(
	select distinct gcconstruccion.identificador
		,gcconstruccion.tipo_construccion
		,gcconstruccion.codigo_terreno 
		,gcprediocatastro.numero_predial
		,gcprediocatastro.t_id predio_t_id
	from serladm.gc_construccion gcconstruccion
	inner join serladm.gc_prediocatastro gcprediocatastro on gcconstruccion.gc_predio = gcprediocatastro.t_id
	where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823') 
);

drop table if exists temp_conteo_construcciones_gestor_por_predio;

-- (2) Cálculo de total de construcciones gestor por predio
create temp table temp_conteo_construcciones_gestor_por_predio as
(
select numero_predial
	,count(*) total_construcciones_gestor
from temp_preparacion_construcciones_gestor
group by 1
);

drop table if exists temp_construccion_digitalizada_con_id_predio;

-- (3) Preparación Construcciones Digitalización, asignación de id_predio a cada construcción
create temp table temp_construccion_digitalizada_con_id_predio as
(
	select distinct ueconstruccion.identificador
		,ueconstruccion.id_tipoconstruccion
		,reluepredio.id_predio predio_t_id
	from serladm.ue_construccion ueconstruccion
	inner join serladm.rel_uepredio reluepredio on ueconstruccion.t_id = reluepredio.id_construccion
);

drop table if exists temp_construcciones_digitalizadas_filtro_lote_6;

-- (4) Filtro de construcciones por Lote + asignación de NPN
create temp table temp_construcciones_digitalizadas_filtro_lote_6 as
(
	select tempconstrucciondigitalizadaconidpredio.*
		,uapredio.numero_predial
	from temp_construccion_digitalizada_con_id_predio tempconstrucciondigitalizadaconidpredio
	inner join serladm.ua_predio uapredio on tempconstrucciondigitalizadaconidpredio.predio_t_id = uapredio.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

-- (5) Conteo de construcciones digitalizadas por predio
create temp table temp_conteo_construcciones_digitalizada_por_predio as
(
	select numero_predial
		,count(*) total_construcciones_digitalizadas
	from temp_construcciones_digitalizadas_filtro_lote_6
	group by 1
);

drop table if exists temp_paramatrizacion_nulos_cruce_construcciones;

-- (6) Parametrización de nulos (no cruce) entre la relación de construcciones gestor\digitalización
create temp table temp_paramatrizacion_nulos_cruce_construcciones as
(
	select tempconteoconstruccionesdigitalizadaporpredio.numero_predial numero_predial_digitalizacion
		,tempconteoconstruccionesdigitalizadaporpredio.total_construcciones_digitalizadas
		,tempconteoconstruccionesgestorporpredio.numero_predial numero_predial_gestor
		,tempconteoconstruccionesgestorporpredio.total_construcciones_gestor
		,(case when tempconteoconstruccionesgestorporpredio.total_construcciones_gestor is null then 0
			   else tempconteoconstruccionesgestorporpredio.total_construcciones_gestor
			   end) parametrizacion_conteo_gestor
	from temp_conteo_construcciones_digitalizada_por_predio tempconteoconstruccionesdigitalizadaporpredio
	left join temp_conteo_construcciones_gestor_por_predio tempconteoconstruccionesgestorporpredio
	on tempconteoconstruccionesdigitalizadaporpredio.numero_predial = 
		tempconteoconstruccionesgestorporpredio.numero_predial
);

drop table if exists temp_parametrizacion_construcciones;

-- (7) Parametrización de indicador dependiendo la comparación en número de construcciones
create temp table temp_parametrizacion_construcciones as
(
	select numero_predial_digitalizacion
		,total_construcciones_digitalizadas
		,numero_predial_gestor
		,total_construcciones_gestor
		,parametrizacion_conteo_gestor
		,(case when total_construcciones_digitalizadas = parametrizacion_conteo_gestor then 'No'
		       when total_construcciones_digitalizadas > parametrizacion_conteo_gestor then 'Si'
		       when total_construcciones_digitalizadas < parametrizacion_conteo_gestor then 'No'
		       else 'Sin parametrización'
		       end) terrenos_diferencia_construcciones
		,null clasificacion_terrenos_diferencia_construcciones
	from temp_paramatrizacion_nulos_cruce_construcciones
);

update temp_parametrizacion_construcciones
set clasificacion_terrenos_diferencia_construcciones = 1
where terrenos_diferencia_construcciones = 'Si';

update temp_parametrizacion_construcciones
set clasificacion_terrenos_diferencia_construcciones = 0
where terrenos_diferencia_construcciones = 'No';


drop table if exists temp_terrenos_diferencia_construcciones;

-- (8) Tabla final
create temp table temp_terrenos_diferencia_construcciones as
(
	select uapredio.t_id predio_t_id
		,numero_predial_digitalizacion numero_predial
		,terrenos_diferencia_construcciones
		,clasificacion_terrenos_diferencia_construcciones
	from temp_parametrizacion_construcciones tempparametrizacionconstrucciones
	inner join serladm.ua_predio uapredio
	on tempparametrizacionconstrucciones.numero_predial_gestor = uapredio.numero_predial
);

-- ******************** Construcción de Matriz Indicativa Método Directo\Indirecto ********************

-- Áreas digitalizadas
create temp table areas_digitalizadas as
(
	select predio_t_id
		,predio_numero_predial
		,superior_en_area_estandar
		,terrenos_nuevos_digitalizacion
	from base_analisis_area
);

-- Posibles informalidades
select *
from posibles_informalidades_lote_6;

-- Verificaciones frente a áreas registrales
select *
from vericacion_area_juridica;

-- Verificaciones frente a derivados
select *
from predios_con_derivados;

-- Temporal sin estandar
create temp table reporte_sin_estandarizar as 
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,areasdigitalizadas.superior_en_area_estandar
		,areasdigitalizadas.terrenos_nuevos_digitalizacion
		,posiblesinformalidadeslote6.posible_informalidad
		,vericacionareajuridica.verificacion_area_registral
		,prediosconderivados.predios_con_derivados
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	left join areas_digitalizadas areasdigitalizadas on terrenosdigitalizadoslote6.predio_t_id = areasdigitalizadas.predio_t_id
	left join posibles_informalidades_lote_6 posiblesinformalidadeslote6 on terrenosdigitalizadoslote6.predio_t_id = posiblesinformalidadeslote6.predio_t_id
	left join vericacion_area_juridica vericacionareajuridica on terrenosdigitalizadoslote6.predio_t_id = vericacionareajuridica.predio_t_id
	left join predios_con_derivados prediosconderivados on terrenosdigitalizadoslote6.predio_t_id = prediosconderivados.predio_t_id
);

-- Estandarización
update reporte_sin_estandarizar
set posible_informalidad = 'No'
where posible_informalidad is null;

update reporte_sin_estandarizar
set verificacion_area_registral = 'No'
where verificacion_area_registral is null;

update reporte_sin_estandarizar
set predios_con_derivados = 'No'
where predios_con_derivados is null;

update reporte_sin_estandarizar
set superior_en_area_estandar = 'No'
where superior_en_area_estandar is null;

update reporte_sin_estandarizar
set terrenos_nuevos_digitalizacion = 'No'
where terrenos_nuevos_digitalizacion is null;

-- Construcción de validadores
create temp table construccion_validadores_reporte as
(
	select predio_t_id
		,predio_numero_predial
		,superior_en_area_estandar
		,(case when superior_en_area_estandar = 'Si' then 1
			   when superior_en_area_estandar = 'No Aplica' then 1
			   else 0
			   end) categorizacion_superior_en_area_estandar
		,terrenos_nuevos_digitalizacion
		,(case when terrenos_nuevos_digitalizacion = 'Si' then 1
		   else 0
		   end) categorizacion_terrenos_nuevos_digitalizacion	
		,posible_informalidad
		,(case when posible_informalidad = 'Si' then 1
		   else 0
		   end) categorizacion_posible_informalidad	
		,verificacion_area_registral
		,(case when verificacion_area_registral = 'Si' then 1
		   else 0
		   end) categorizacion_verificacion_area_registral
		,predios_con_derivados
		,(case when predios_con_derivados = 'Si' then 1
		   else 0
		   end) categorizacion_predios_con_derivados	
	from reporte_sin_estandarizar
);

create temp table reporte_estandarizado as
(
	select predio_t_id
		,predio_numero_predial
		,superior_en_area_estandar
		,categorizacion_superior_en_area_estandar
		,terrenos_nuevos_digitalizacion
		,categorizacion_terrenos_nuevos_digitalizacion
		,posible_informalidad
		,categorizacion_posible_informalidad
		,verificacion_area_registral
		,categorizacion_verificacion_area_registral
		,predios_con_derivados
		,categorizacion_predios_con_derivados
		,(categorizacion_superior_en_area_estandar + categorizacion_terrenos_nuevos_digitalizacion 
			+ categorizacion_posible_informalidad + categorizacion_verificacion_area_registral 
			+ categorizacion_predios_con_derivados) totalizacion_categorias
	from construccion_validadores_reporte
);

drop table if exists priorizacion_predio_metodo_lote_6;

create table priorizacion_predio_metodo_lote_6 as
(
	select predio_t_id
			,predio_numero_predial
			,superior_en_area_estandar
			,categorizacion_superior_en_area_estandar
			,terrenos_nuevos_digitalizacion
			,categorizacion_terrenos_nuevos_digitalizacion
			,posible_informalidad
			,categorizacion_posible_informalidad
			,verificacion_area_registral
			,categorizacion_verificacion_area_registral
			,predios_con_derivados
			,categorizacion_predios_con_derivados
			,totalizacion_categorias
			,(case when totalizacion_categorias <> 0 then 'Método Directo'
				   else 'Método Indirecto'
				   end) seleccion_metodo
			,(case when totalizacion_categorias >= 3 then 'Alto'
			       when totalizacion_categorias = 2 then 'Medio'
			       when totalizacion_categorias = 1 then 'Bajo'
			       else 'No Aplica'
			       end) priorizacion_metodo_indirecto
	from reporte_estandarizado
);

-- Analisis

--Estadisticos:

-- (1) Totales de Predios
select *
from serladm.ua_predio
--where substring(numero_predial,1,5) in ('20250','70713','70823') 
--where substring(numero_predial,1,5) in ('20250') 
--where substring(numero_predial,1,5) in ('70713') 
where substring(numero_predial,1,5) in ('70823') 

-- (2) Totales de Terrenos
select *
from public.terrenos_digitalizados_lote_6
--where substring(predio_numero_predial,1,5) in ('20250','70713','70823') 
where substring(predio_numero_predial,1,5) in ('20250') 
-- where substring(predio_numero_predial,1,5) in ('70713') 
--where substring(predio_numero_predial,1,5) in ('70823')

-- Lote 6, análisis por método
select seleccion_metodo
	,count(*) total_predios_por_metodo
from priorizacion_predio_metodo_lote_6
group by seleccion_metodo;
	
-- Lote 6, por Municipio
select substring(predio_numero_predial,1,5) municipio
	,seleccion_metodo
	,count(*)
from priorizacion_predio_metodo_lote_6
group by substring(predio_numero_predial,1,5), seleccion_metodo;

-- Lote 6, Priorizaciones por Método Directo
select substring(predio_numero_predial,1,5) municipio
	,priorizacion_metodo_indirecto
	,count(*)
from priorizacion_predio_metodo_lote_6
where priorizacion_metodo_indirecto <> 'No Aplica'
group by substring(predio_numero_predial,1,5), priorizacion_metodo_indirecto
order by 1, 2;

-- Creación Vista
create view vw_terrenos_priorizacion_predio_metodo_lote_6 as
(
	select priorizacionprediometodolote6.predio_t_id
			,priorizacionprediometodolote6.predio_numero_predial
			,priorizacionprediometodolote6.superior_en_area_estandar
			,priorizacionprediometodolote6.categorizacion_superior_en_area_estandar
			,priorizacionprediometodolote6.terrenos_nuevos_digitalizacion
			,priorizacionprediometodolote6.categorizacion_terrenos_nuevos_digitalizacion
			,priorizacionprediometodolote6.posible_informalidad
			,priorizacionprediometodolote6.categorizacion_posible_informalidad
			,priorizacionprediometodolote6.verificacion_area_registral
			,priorizacionprediometodolote6.categorizacion_verificacion_area_registral
			,priorizacionprediometodolote6.predios_con_derivados
			,priorizacionprediometodolote6.categorizacion_predios_con_derivados
			,priorizacionprediometodolote6.totalizacion_categorias
			,priorizacionprediometodolote6.seleccion_metodo
			,priorizacionprediometodolote6.priorizacion_metodo_indirecto
			,geometria
	from priorizacion_predio_metodo_lote_6 priorizacionprediometodolote6
	inner join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	on terrenosdigitalizadoslote6.predio_t_id = priorizacionprediometodolote6.predio_t_id
);
