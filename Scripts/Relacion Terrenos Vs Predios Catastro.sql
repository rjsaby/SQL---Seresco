-- TABLAS
drop table if exists temp_terrenos_lote_6;
drop table if exists temp_construcciones_lote_6;
drop table if exists temp_unidad_intervencion;
drop table if exists temp_conteo_derivados_lote_6;

-- ******************************** \\MAPHURRICANE\\ INTERRELACIÓN PREDIO\TERRENO\CONSTRUCCION ********************************

-- Terrenos MapHurricane - serladm
create temp table temp_terrenos_lote_6 as
(
	select uapredio.t_id predio_t_id
		,uapredio.id_operacion predio_id_operacion
		,uapredio.numero_predial predio_numero_predial
		,ueterreno.t_id terreno_t_id
		,ueterreno.area_terreno terreno_area_terreno
		,ueterreno.geometria
		from serladm.rel_uepredio reluepredio left join serladm.ua_predio uapredio on reluepredio.id_predio = uapredio.t_id											  
										  	  inner join serladm.ue_terreno ueterreno on reluepredio.id_terreno = ueterreno.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823') 
);

-- Construcciones MapHurricane - serladm
create temp table temp_construcciones_lote_6 as
(
	select uapredio.t_id predio_t_id
		,uapredio.id_operacion predio_id_operacion
		,uapredio.numero_predial predio_numero_predial		
		,ueconstruccion.t_id construccion_t_id
		,ueconstruccion.id_tipoconstruccion construccion_idtipoconstruccion
		,ueconstruccion.area_construccion construccion_areaconstruccion
		,ueconstruccion.geometria
		from serladm.rel_uepredio reluepredio left join serladm.ua_predio uapredio on reluepredio.id_predio = uapredio.t_id											  
										  	  inner join serladm.ue_construccion ueconstruccion on reluepredio.id_construccion = ueconstruccion.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823') 
);

-- *** Relación Unidad Intervención ***
create temp table temp_unidad_intervencion as
(
	select relunidadintervencionpredio.t_id
		,relunidadintervencionpredio.id_unidadintervencion
		,relunidadintervencionpredio.id_predio
		,mhunidadintervencion.t_id unidadintervencion_t_id
		,mhunidadintervencion.codigo
		from serladm.rel_unidadintervencion_predio relunidadintervencionpredio inner join serladm.mh_unidadintervencion mhunidadintervencion
																			on relunidadintervencionpredio.id_unidadintervencion = mhunidadintervencion.t_id
);	

-- Generación Capa -Terrenos Digitalizados-

drop table if exists terrenos_digitalizados_lote_6;

create table terrenos_digitalizados_lote_6 as
(
	select predio_t_id
			,predio_id_operacion
			,predio_numero_predial
			,terreno_t_id
			,terreno_area_terreno
			,tempunidadintervencion.codigo codigo_unidad_intervencion
			,geometria	
	from temp_terrenos_lote_6 tempterrenoslote6 left join temp_unidad_intervencion tempunidadintervencion 
													on tempterrenoslote6.predio_t_id = tempunidadintervencion.id_predio
);

-- Generación Capa -Construcciones Digitalizadas-

drop table if exists construcciones_digitalizadas_lote_6;

create table construcciones_digitalizadas_lote_6 as
(
	select predio_t_id
		,predio_id_operacion
		,predio_numero_predial		
		,construccion_t_id
		,construccion_idtipoconstruccion
		,construccion_areaconstruccion
		,tempunidadintervencion.codigo codigo_unidad_intervencion
		,geometria	
	from temp_construcciones_lote_6 tempconstruccioneslote6 left join temp_unidad_intervencion tempunidadintervencion 
													on tempconstruccioneslote6.predio_t_id = tempunidadintervencion.id_predio
);

-- ******************************** \\COLOMBIAINI\\ INTERRELACIÓN PREDIO\TERRENO\CONSTRUCCION ********************************
/*
drop table if exists gestor_terrenos_lote_6;

-- GC Temporal Terrenos
create table gestor_terrenos_lote_6 as
(
	select gcprediocatastro.t_id gcprediocatastro_t_id
		,gcprediocatastro.numero_predial
		,gcprediocatastro.numero_predial_anterior
		,gcterreno.t_id gcterreno_t_id
		,gcterreno.area_terreno_alfanumerica
		,gcterreno.area_terreno_digital
		,gcterreno.manzana_vereda_codigo
		,gcterreno.numero_subterraneos
		,gcterreno.geometria
	from serladm.rel_uepredio reluepredio inner join serladm.gc_prediocatastro gcprediocatastro on reluepredio.id_predio = gcprediocatastro.t_id
										  left join serladm.gc_terreno gcterreno on reluepredio.id_terreno = gcterreno.t_id
	where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
);

drop table if exists gestor_construcciones_lote_6;

-- GC Temporal Construcciones
create table gestor_construcciones_lote_6 as
(
	select gcprediocatastro.t_id gcprediocatastro_t_id
		,gcprediocatastro.numero_predial
		,gcprediocatastro.numero_predial_anterior
		,gcconstruccion.t_id gcconstruccion_t_id
		,gcconstruccion.tipo_construccion
		,gcconstruccion.area_construida
		,gcconstruccion.geometria
	from serladm.gc_prediocatastro gcprediocatastro left join serladm.gc_construccion gcconstruccion on gcprediocatastro.t_id = gcconstruccion.gc_predio
	where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
);

*/

-- ******************************** \\MAPHURRICANE\\ESTADO FOLIO\\DERIVADOS ********************************

-- Folios (Predio Cruce Levantamiento Catastral)
select distinct t_id predio_t_id, matricula_inmobiliaria from serladm.ua_predio
where matricula_inmobiliaria is not null and matricula_inmobiliaria <> '0' and matricula_inmobiliaria <> '';

-- Folios de Matricula (Anotaciones)
select distinct folio_matricula, estado_folio from public.anotaciones_lote_6

-- ** Estado
drop table if exists estado_folios_matricula_lote_6;

create table estado_folios_matricula_lote_6 as
(
select distinct uapredio.t_id predio_t_id
	,uapredio.matricula_inmobiliaria
	,(case when anotacioneslote6.estado_folio is not null then anotacioneslote6.estado_folio
		   when anotacioneslote6.estado_folio is null then 'Sin información por BD'
		   else 'Error'
		   end) estado_folio
from serladm.ua_predio uapredio left join public.anotaciones_lote_6 anotacioneslote6 
									on uapredio.matricula_inmobiliaria = anotacioneslote6.folio_matricula
where uapredio.matricula_inmobiliaria is not null and uapredio.matricula_inmobiliaria <> '0' and uapredio.matricula_inmobiliaria <> ''
);

-- ** Derivado
create temp table temp_conteo_derivados_lote_6 as
(
select t1.folio_matricula
	,count(*) numero_derivados_x_folio_matriz
	from
	(
	select distinct folio_matricula, folio_derivado 
	from public.anotaciones_lote_6
	where folio_derivado is not null
	) t1
	group by t1.folio_matricula
	order by 1
);

drop table if exists folio_matriz_folio_derivado_lote_6;

create table folio_matriz_folio_derivado_lote_6 as
(
	select distinct anotacioneslote6.folio_matricula
		,anotacioneslote6.folio_derivado
		,tempconteoderivadoslote6.numero_derivados_x_folio_matriz
	from public.anotaciones_lote_6 anotacioneslote6
	inner join temp_conteo_derivados_lote_6 tempconteoderivadoslote6 on anotacioneslote6.folio_matricula = tempconteoderivadoslote6.folio_matricula
	where anotacioneslote6.folio_derivado is not null
	order by 1
);

-- Vista Terrenos_Estado_Folio_Lote_6
select *
from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
inner join estado_folios_matricula_lote_6 estadofoliosmatriculalote6
on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id

-- Vista Terrenos_Folio_Matriz_Folio_Derivado_Lote_6
select *
from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
inner join estado_folios_matricula_lote_6 estadofoliosmatriculalote6 on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
inner join folio_matriz_folio_derivado_lote_6 foliomatrizfolioderivadolote6 on foliomatrizfolioderivadolote6.folio_matricula = estadofoliosmatriculalote6.matricula_inmobiliaria


