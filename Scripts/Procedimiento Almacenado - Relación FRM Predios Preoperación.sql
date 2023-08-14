-- Procedimiento Almacenado
/*
 * Los procesos que se ejecutan en este PA son:
 * Generación de enlance entre los frm sincronizados y la base de predios preoperativos precargados
 */

/*
 * Requiere:
 * Terrenos Digitalizados Lote 6 Actualizado
 * */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-06-08
 * */
create or replace procedure relacion_formularios_predios_preoperativos()
language plpgsql
as $$
begin

	-- Temporales
	drop table if exists temp_frm_sincronizados_con_predio_t_id;
	drop table if exists temp_frm_sincronizados_con_geometria;
	drop table if exists temp_frm_sincronizados_sin_geometria;
	drop table if exists temp_frm_sincronizados_sin_predio_t_id;
	drop table if exists temp_cruce_con_geometrias_frm;
	drop table if exists temp_cruce_sin_geometrias_frm;
	
	-- Físicas
	drop table if exists estado_enlace_formularios_predios_preoperativo_lote_6;

	--****************************** CONEXIÓN ******************************

	call colombiaseg_lote6.dblink_bd_maphurricane();

	-- ******************************** EJECUCIÓN ********************************
	
	-- (0) Preparación Información Sincronización
	create temp table temp_frm_sincronizados_con_predio_t_id as
	(
		select identificacion_predio_numero_predial
			,identificacion_predio_numero_predial_precarga
			,identificacion_predio_usando
			,identificacion_predio_pre_t_id
			--,lc_resultadovisitatipo
			,lcresultadovisitatipo.dispname resultado_visita
		from dblink_maphurricane frmmaphurricane
		inner join dblink_lc_resultadovisitatipo lcresultadovisitatipo
		on cast(frmmaphurricane.lc_resultadovisitatipo as int) = lcresultadovisitatipo.itfcode
		where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823')
			and identificacion_predio_pre_t_id is not null
	);
	
	-- (1) Predios sincronizados con predio_t_id y con geometria
	create temp table temp_frm_sincronizados_con_geometria as
	(
	select identificacion_predio_numero_predial
			,cast(identificacion_predio_pre_t_id as int) precarga_predio_t_id
			--,lc_resultadovisitatipo
			,resultado_visita
			--,terrenosdigitalizadoslote6.geometria
			,'Si' con_geometria
	from temp_frm_sincronizados_con_predio_t_id tempfrmsincronizadosconprediotid
	left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	on cast(tempfrmsincronizadosconprediotid.identificacion_predio_pre_t_id as int) = terrenosdigitalizadoslote6.predio_t_id
	where terrenosdigitalizadoslote6.geometria is not null
	);
	
	-- (2) Predios sincronizados con predio_t_id y sin geometria
	create temp table temp_frm_sincronizados_sin_geometria as
	(
	select identificacion_predio_numero_predial
			,cast(identificacion_predio_pre_t_id as int) precarga_predio_t_id
			--,lc_resultadovisitatipo
			,resultado_visita
			--,terrenosdigitalizadoslote6.geometria
			,'No' con_geometria
	from temp_frm_sincronizados_con_predio_t_id tempfrmsincronizadosconprediotid
	left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	on cast(tempfrmsincronizadosconprediotid.identificacion_predio_pre_t_id as int) = terrenosdigitalizadoslote6.predio_t_id
	where terrenosdigitalizadoslote6.geometria is null
	);
	
	-- (3) Predios sincronizados sin predio_t_id y sin geometria
	-- Tabla temporal, no hace parte de la unificación, por medio de esta se cruza la información
	-- georreferenciada por parte del prediador en campo (FRM)
	create temp table temp_frm_sincronizados_sin_predio_t_id as
	(
		select identificacion_predio_numero_predial
			,cast(identificacion_predio_pre_t_id as int) precarga_predio_t_id
			--,lc_resultadovisitatipo
			,lcresultadovisitatipo.dispname resultado_visita
			,'No' con_geometria 
		from dblink_maphurricane frmmaphurricane
		inner join dblink_lc_resultadovisitatipo lcresultadovisitatipo
		on cast(frmmaphurricane.lc_resultadovisitatipo as int) = lcresultadovisitatipo.itfcode
		where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823')
			and identificacion_predio_pre_t_id is null
	);

	-- (4) Predios con geometría referenciada desde FRM (Segregados)
	create temp table temp_cruce_con_geometrias_frm as
	(
		select tempfrmsincronizadossinprediotid.identificacion_predio_numero_predial
			,tempfrmsincronizadossinprediotid.precarga_predio_t_id
			,tempfrmsincronizadossinprediotid.resultado_visita
			,'Si' con_geometria
		from temp_frm_sincronizados_sin_predio_t_id tempfrmsincronizadossinprediotid
		-- REVISAR
		inner join dblink_rel_uepredio_campo reluepredio on tempfrmsincronizadossinprediotid.identificacion_predio_numero_predial = reluepredio.numero_predial
		inner join dblink_ue_terreno_campo ueterreno on reluepredio.id_terreno = ueterreno.t_id
	);

	create temp table temp_cruce_sin_geometrias_frm as
	(
		select tempfrmsincronizadossinprediotid.identificacion_predio_numero_predial
			,tempfrmsincronizadossinprediotid.precarga_predio_t_id
			,tempfrmsincronizadossinprediotid.resultado_visita
			,'No' con_geometria
		from temp_frm_sincronizados_sin_predio_t_id tempfrmsincronizadossinprediotid
		left join dblink_rel_uepredio_campo reluepredio on tempfrmsincronizadossinprediotid.identificacion_predio_numero_predial = reluepredio.numero_predial
		left join dblink_ue_terreno_campo ueterreno on reluepredio.id_terreno = ueterreno.t_id
		where geom is null
	);
	
	-- (4) Creación de Tabla
	create table estado_enlace_formularios_predios_preoperativo_lote_6 as
	(
		select *
		from
		(
		(
			select *
			from temp_frm_sincronizados_con_geometria
		)
		union
		(
			select *
			from temp_frm_sincronizados_sin_geometria
		)
		union
		(
			select *
			from temp_cruce_con_geometrias_frm
		)
		union
		(
			select *
			from temp_cruce_con_geometrias_frm
		)
		union
		(
			select *
			from temp_cruce_sin_geometrias_frm
		)
		) T1
	);

end;$$

