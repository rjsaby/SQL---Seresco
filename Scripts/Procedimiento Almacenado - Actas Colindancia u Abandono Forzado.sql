-- Procedimiento Almacenado
/*
 * Los procesos que se ejecutan en este PA son:
 * Identificación de predios donde se selección, desde campo, alguno de los estados posibles para Actas de Colindancia o Abandono Forzado
 */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-11-30
 * */

create or replace procedure actas_colindancia_abandono_forzado_lote_6()
language plpgsql
as $$
begin
	
	-- *** Conexión BDLINK ***
	call colombiaseg_lote6.dblink_bd_maphurricane();
	
-- *** Conexión BDLINK ***
	call colombiaseg_lote6.dblink_bd_maphurricane();
	
	-- Temporales
	drop table if exists temp_predio_acta_o_reclamacion;
	drop table if exists temp_rel_ua_predio_campo;
	drop table if exists temp_terrenos_acta_reclamacion;
	drop table if exists temp_unidades_intervencion_mh;
	drop table if exists temp_centroid_terrenos_acta_reclamacion;
	drop table if exists temp_interseccion_rel_terrenoacta_w_ui;
	drop table if exists temp_terreno_acta_w_ui;
	drop table if exists terrenos_acta_colindancia_o_reclamacion_lote_6;
	
 	-- Física
	drop table if exists tbl_predio_acta_reclamacion;
	
	-- Vista
	drop table if exists vw_terrenos_acta_reclamacion;
	
	-- Generación de tabla física para la creación de la vista
	-- Ádemas, esta consulta constituirá el archivo CSV que se compartirá diariamente a modo de informe
	create temp table temp_predio_acta_o_reclamacion as
	(
	select --meta_instanceid
		identificacion_predio_numero_predial predio_numero_predial
		,(case when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '1' then 'Si'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '0' then 'No'
			else 'Verificar FRM'
			end) suscribe_acta_colindancia
		,(case when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit03 = '1' then 'Si'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '0' then 'No'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 is null then 'Registro nulo desde FRM'
			else 'Verificar FRM'
			end) reclamacion_abandono_forzado_reclamacion 
	from dblink_maphurricane
	where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823') and
		grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 is not null
	);
	
	create temp table temp_rel_ua_predio_campo as
	(
		-- Se generan los únicos dado que dependiendo de las C o las UC un id_terreno puede estar repetido
		select distinct id_terreno
			,numero_predial
			--,instance_id
		from dblink_rel_uepredio_campo
		where substring(numero_predial,1,5) in ('20250','70713','70823')
	);
	
	-- Tabla que se debe exportar a CSV
	-- Se migra físicamente para construir la vista con la capa de terrenos campo.
	create table tbl_predio_acta_reclamacion as
	(
	select distinct (case when id_terreno is null then 'FRM Sin Geometria'
		else id_terreno
		end) id_terreno
		,t2.predio_numero_predial
		,t2.suscribe_acta_colindancia
		,t2.reclamacion_abandono_forzado_reclamacion
	from temp_rel_ua_predio_campo t1
	right join temp_predio_acta_o_reclamacion t2
	on t1.numero_predial = t2.predio_numero_predial
	);
	
	create temp table temp_terrenos_acta_reclamacion as
	(
	select row_number () over (order by t1.id_terreno) id  
		,t1.*
	from (
		select t1.id_terreno
			,t1.predio_numero_predial
			,t1.suscribe_acta_colindancia
			,t1.reclamacion_abandono_forzado_reclamacion
			-- Existe una dificultad en los datos a la hora de realizar procesos de transformación
			--,st_transform(t2.geom, 9377) geometria
			,t2.geom
			from colombiaseg_lote6.tbl_predio_acta_reclamacion t1
			inner join colombiaseg_lote6.dblink_ue_terreno_campo t2
			on t1.id_terreno = t2.t_id
			where t1.id_terreno <> 'FRM Sin Geometria'
			) t1
	);

	create temp table temp_centroid_terrenos_acta_reclamacion as
	(
	select id
		,id_terreno
		,predio_numero_predial
		,suscribe_acta_colindancia
		,reclamacion_abandono_forzado_reclamacion
		,ST_Centroid(geom) geom
	from temp_terrenos_acta_reclamacion
	);

	create temp table temp_unidades_intervencion_mh as
	(
		select mhunidadintervencion.t_id unidadintervencion_t_id
		--,mhunidadintervencion.id_limite_administrativo
		,mhlimiteadministrativo.codigo codigo_municipio
		,mhlimiteadministrativo.nombre nombre_municipio
		,mhunidadintervencion.codigo codigo_unidad_intervencion
		,mhunidadintervencion.descripcion
		--,mhunidadintervencion.id_estado
		,mhunidadintervencionestado.descripcion estado
		,st_transform(ST_MakeValid(mhunidadintervencion.geometria),9377) geometria
		from dblink_mh_unidadintervencion  mhunidadintervencion
		inner join dblink_mh_unidadintervencionestado mhunidadintervencionestado on mhunidadintervencion.id_estado = mhunidadintervencionestado.t_id 
		inner join dblink_mh_limite_administrativo mhlimiteadministrativo on mhunidadintervencion.id_limite_administrativo = mhlimiteadministrativo.t_id 
		where mhlimiteadministrativo.codigo in ('20250', '70713', '70823')
	);

	create temp table temp_interseccion_rel_terrenoacta_w_ui as
	(
		select distinct t2.nombre_municipio
				,t1.id
				,t1.id_terreno
				,t1.predio_numero_predial
				,t1.suscribe_acta_colindancia
				,t1.reclamacion_abandono_forzado_reclamacion
				,t2.codigo_unidad_intervencion
				,ST_Intersects(st_transform(t1.geom, 9377), t2.geometria) geom
		from temp_centroid_terrenos_acta_reclamacion t1, temp_unidades_intervencion_mh t2
		where ST_Intersects(st_transform(t1.geom, 9377), t2.geometria) is true
	);

	create temp table temp_terreno_acta_w_ui as
	(
	select nombre_municipio
		,id
		,id_terreno
		,predio_numero_predial
		,suscribe_acta_colindancia
		,reclamacion_abandono_forzado_reclamacion
		,codigo_unidad_intervencion
	from temp_interseccion_rel_terrenoacta_w_ui
	);

	create table terrenos_acta_colindancia_o_reclamacion_lote_6 as
	(
		select t1.nombre_municipio
			,t1.id
			,t1.id_terreno
			,t1.predio_numero_predial
			,t1.suscribe_acta_colindancia
			,t1.reclamacion_abandono_forzado_reclamacion
			,t1.codigo_unidad_intervencion
			,st_transform(t2.geom, 9377) geom
		from temp_terreno_acta_w_ui t1
		inner join colombiaseg_lote6.dblink_ue_terreno_campo t2
		on t1.id_terreno = t2.t_id
		where t1.id_terreno <> 'FRM Sin Geometria'
	);

end;$$