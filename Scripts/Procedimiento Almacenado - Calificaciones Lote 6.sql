-- Procedimiento Almacenado
create or replace procedure calidad_calificacion_ue_lote_6(ejecucion text)
language plpgsql
as $$
	declare
		consulta_1 text;
		nombre_archivo text;
		ruta_archivo text;
	
begin
	
	if ejecucion = 'SI' then		
		nombre_archivo := 'calificaciones_unidades_construccion_lote_6';
		ruta_archivo := 'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\10_Calidad_UC_Calificaciones\' || nombre_archivo || '.csv';

		-- *** Conexión DBLINK ***
        call colombiaseg_lote6.dblink_bd_maphurricane();
       
		-- Borrrado tablas
		-- Temporales
		drop table if exists temp_maphurricane;
		drop table if exists temp_reluepredio;
		drop table if exists temp_construcciones_campo;
		drop table if exists temp_fmr_relacion;
		drop table if exists temp_frm_relacion_construccion;
		drop table if exists temp_unidad_construccion_campo;
		drop table if exists temp_frm_relacion_construccion_unidadconstruccion;
		drop table if exists temp_terrenos_campo;
		drop table if exists temp_rel_terreno_predio;
		drop table if exists temp_unidades_intervencion_mh;
		drop table if exists temp_rel_predio_terreno;
		drop table if exists temp_centroides_terreno_predio;
		drop table if exists temp_interseccion_rel_terrenopredio_w_ui;
		drop table if exists rel_terreno_predio_ui;
		drop table if exists temp_relacion_construccion_unidadconstruccion_ui;
		
		create temp table temp_maphurricane as
		(
		select distinct submissiondate
		,identificacion_predio_numero_predial
		,meta_instanceid
		,"key"
		,submittername
		from dblink_maphurricane
		where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823')
		);
		
		create temp table temp_reluepredio as
		(
		select id_construccion 
			,id_unidadconstruccion
			,id_terreno
			,numero_predial
			,numero_predial_anterior
			,concat('uuid:',instance_id) instance_id
		from dblink_rel_uepredio_campo
		where substring(numero_predial,1,5) in ('20250','70713','70823') and (id_construccion is not null)
		);
		
		create temp table temp_construcciones_campo as
		(
			select mapconstruccion.numero_predial_18 numero_predial_construccion
				--,mapconstruccion.construccion_general_man_ue_construccion_id_tipoconstruccion id_tipo_construccion
				,domtipoconstruccion.descripcion tipo_construccion
				--,mapconstruccion.construccion_general_ue_construccion_id_dominioconstrucciontipo id_dominio_construccion
				,domdominiotipo.descripcion tipo_dominio_construccion
				,mapconstruccion.construccion_general_pre_ue_construccion_numero_pisos numero_pisos_precarga
				,mapconstruccion.construccion_general_corr_pre_ue_construccion_numero_pisos numero_pisos_campo
				,mapconstruccion.construccion_general_ue_construccion_anio_construccion anio_construccion
				,mapconstruccion.parent_key
				,mapconstruccion."key"
			from dblink_ue_construccion_campo mapconstruccion
			left join dblink_tc_construcciontipo domtipoconstruccion 
				on domtipoconstruccion.codigo = cast(mapconstruccion.construccion_general_man_ue_construccion_id_tipoconstruccion as int8)
			left join dblink_tc_dominioconstrucciontipo domdominiotipo 
				on domdominiotipo.codigo = cast(mapconstruccion.construccion_general_ue_construccion_id_dominioconstrucciontipo as int8)
			where substring(mapconstruccion.numero_predial_18,1,5) in ('20250','70713','70823')
		);
		
		create temp table temp_unidad_construccion_campo as
		(
			select numero_predial_19 numero_predial_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_uso uso_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_habitac total_habitaciones_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_banios total_banios_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_locales total_locales_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_altura unidad_construccion_altura
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_anio_construc anio_construccion_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_area_privada_ area_privada_unidad_construccion
				,ue_unidadconstruccion_datos_ue_unidadconstruccion_observaciones observaciones
				,ue_calificacionconvencional_id_calificartipo id_tipo_calificacion
				,tccalificartipo.descripcion tipo_calificacion
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados_u fachada
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados00 puntos_fachada
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados01 cubrimiento_muros
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados02 puntos_cubrimiento_muros
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados03 pisos
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados04 puntos_pisos
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados05 conservacion
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados06 puntos_conservacion_acabados
				,grupo_calificacion_convencional_ue_grupocalificacion_acabados07 subtotal_acabados
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_o tamanio_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue00 puntos_tamanio_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue01 enchapes_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue02 puntos_enchape_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue03 mobiliario_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue04 puntos_mobiliario_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_g conservacion_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue05 puntos_conservacion_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue06 subtotal_banio
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_ue_ tamanio_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u00 puntos_tamanio_conina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u01 enchapes_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u02 puntos_enchapes_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u03 mobiliario_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u04 puntos_mobiliario_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u05 conservacion_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u06 puntos_conservacion_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u07 subtotal_cocina
				,grupo_calificacion_convencional_ue_grupocalificacion_complement cerchas
				,grupo_calificacion_convencional_ue_grupocalificacion_compleme00 puntos_cerchas
				,grupo_calificacion_convencional_ue_grupocalificacion_compleme01 subtotal_cerchas
				,grupo_calificacion_convencional_total_calificacion_industrial total_calificacion_industria
				,grupo_calificacion_convencional_total_calificacion_no_industria total_calificacion_no_industrial
				,grupo_calificacion_convencional_lc_calconve_total_calificacion total_calificacion
				,grupo_calificacion_no_convencional_lc_anexotipo_tipo_anexo tipo_anexo
				,parent_key
				,"key"
			from dblink_ue_unidad_construccion_campo unidadconstruccion
			left join dblink_tc_calificartipo tccalificartipo
			on cast(unidadconstruccion.ue_calificacionconvencional_id_calificartipo as int8) = tccalificartipo.codigo	
			where substring(numero_predial_19,1,5) in ('20250','70713','70823')
		);
		
		-- (1) Unión FRM\Relación
		create temp table temp_fmr_relacion as
		(
			select distinct maphurricane.submissiondate
				,reluepredio.numero_predial
				,reluepredio.numero_predial_anterior numero_predial_precarga
				,maphurricane.meta_instanceid
				,maphurricane.submittername
				,reluepredio.id_terreno
				,reluepredio.id_construccion 
				,reluepredio.id_unidadconstruccion
			from temp_maphurricane maphurricane
			inner join temp_reluepredio reluepredio on maphurricane.meta_instanceid = reluepredio.instance_id
		);
		
		-- (2) Unión FRM\Relación con Construccion
		create temp table temp_frm_relacion_construccion as
		(
			select distinct relacion_1.submissiondate
					,relacion_1.numero_predial
					,relacion_1.numero_predial_precarga
					,relacion_1.meta_instanceid
					,relacion_1.submittername
					,relacion_1.id_terreno
					,relacion_1.id_construccion 
					,relacion_1.id_unidadconstruccion		
					,construccioncampo.tipo_construccion
					,construccioncampo.tipo_dominio_construccion
					,construccioncampo.numero_pisos_precarga
					,construccioncampo.numero_pisos_campo
					,construccioncampo.anio_construccion
					,construccioncampo."key" id_instance_construccion		
			from temp_fmr_relacion relacion_1
			inner join temp_construcciones_campo construccioncampo
			on relacion_1.meta_instanceid = construccioncampo.parent_key
		);
		
		-- (3) Unión FRM\Relación\Construcción con Unidades de Construcción
		
		create temp table temp_frm_relacion_construccion_unidadconstruccion as
		(
		select distinct (case when substring(relacion_2.numero_predial,1,5)='20250' then 'El Paso'
						      when substring(relacion_2.numero_predial,1,5)='70713' then 'San Onofre'
						      when substring(relacion_2.numero_predial,1,5)='70823' then 'Toluviejo'
						      else 'Erro codificacion'
						      end) municipio 
			    ,relacion_2.submissiondate fecha_sincronizacion
				,relacion_2.numero_predial
				,relacion_2.numero_predial_precarga
				--,relacion_2.meta_instanceid
				,substring(relacion_2.submittername,1,7) tableta
				,relacion_2.id_terreno
				,relacion_2.id_construccion 
				,relacion_2.id_unidadconstruccion		
				,relacion_2.tipo_construccion
				,relacion_2.tipo_dominio_construccion
				,relacion_2.numero_pisos_precarga
				,relacion_2.numero_pisos_campo
				,relacion_2.anio_construccion
				--,relacion_2.id_instance_construccion
				--,unidadconstruccioncampo.numero_predial_unidad_construccion
				,unidadconstruccioncampo.uso_unidad_construccion
				,unidadconstruccioncampo.total_habitaciones_unidad_construccion
				,unidadconstruccioncampo.total_banios_unidad_construccion
				,unidadconstruccioncampo.total_locales_unidad_construccion
				,unidadconstruccioncampo.unidad_construccion_altura
				,unidadconstruccioncampo.anio_construccion_unidad_construccion
				,unidadconstruccioncampo.area_privada_unidad_construccion
				,unidadconstruccioncampo.observaciones
				,unidadconstruccioncampo.id_tipo_calificacion
				,unidadconstruccioncampo.tipo_calificacion
				,unidadconstruccioncampo.fachada
				,unidadconstruccioncampo.puntos_fachada
				,unidadconstruccioncampo.cubrimiento_muros
				,unidadconstruccioncampo.puntos_cubrimiento_muros
				,unidadconstruccioncampo.pisos
				,unidadconstruccioncampo.puntos_pisos
				,unidadconstruccioncampo.conservacion
				,unidadconstruccioncampo.puntos_conservacion_acabados
				,unidadconstruccioncampo.subtotal_acabados
				,unidadconstruccioncampo.tamanio_banio
				,unidadconstruccioncampo.puntos_tamanio_banio
				,unidadconstruccioncampo.enchapes_banio
				,unidadconstruccioncampo.puntos_enchape_banio
				,unidadconstruccioncampo.mobiliario_banio
				,unidadconstruccioncampo.puntos_mobiliario_banio
				,unidadconstruccioncampo.conservacion_banio
				,unidadconstruccioncampo.puntos_conservacion_banio
				,unidadconstruccioncampo.subtotal_banio
				,unidadconstruccioncampo.tamanio_cocina
				,unidadconstruccioncampo.puntos_tamanio_conina
				,unidadconstruccioncampo.enchapes_cocina
				,unidadconstruccioncampo.puntos_enchapes_cocina
				,unidadconstruccioncampo.mobiliario_cocina
				,unidadconstruccioncampo.puntos_mobiliario_cocina
				,unidadconstruccioncampo.conservacion_cocina
				,unidadconstruccioncampo.puntos_conservacion_cocina
				,unidadconstruccioncampo.subtotal_cocina
				,unidadconstruccioncampo.cerchas
				,unidadconstruccioncampo.puntos_cerchas
				,unidadconstruccioncampo.subtotal_cerchas
				,unidadconstruccioncampo.total_calificacion_industria
				,unidadconstruccioncampo.total_calificacion_no_industrial
				,unidadconstruccioncampo.total_calificacion
				,unidadconstruccioncampo.tipo_anexo
				--,unidadconstruccioncampo.parent_key
				--,unidadconstruccioncampo."key"
		from temp_frm_relacion_construccion relacion_2
		inner join temp_unidad_construccion_campo unidadconstruccioncampo 
		on relacion_2.id_instance_construccion = unidadconstruccioncampo.parent_key
		--where unidadconstruccioncampo.numero_predial_unidad_construccion = '202500001000000080021000000000'
		);	
	
		create temp table temp_terrenos_campo as
		(
			select t_id t_id_terreno
				,local_id
				,etiqueta
				,fecha_edicion
				,fecha_alta
				,geom
			from dblink_ue_terreno_campo
			where substring(local_id,1,5) in ('20250','70713','70823') 
			order by 1
		);
		
		create temp table temp_rel_terreno_predio as
		(
			select id_terreno
				,numero_predial
				,numero_predial_anterior numero_predial_precarga
				,accion_novedad
				,fecha_mod fecha_modificacion
			from dblink_rel_uepredio_campo
			where substring(numero_predial,1,5) in ('20250','70713','70823')
			order by 1
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
			,st_transform(mhunidadintervencion.geometria,9377) geometria
			from dblink_mh_unidadintervencion  mhunidadintervencion
			inner join dblink_mh_unidadintervencionestado mhunidadintervencionestado on mhunidadintervencion.id_estado = mhunidadintervencionestado.t_id 
			inner join dblink_mh_limite_administrativo mhlimiteadministrativo on mhunidadintervencion.id_limite_administrativo = mhlimiteadministrativo.t_id 
			where mhlimiteadministrativo.codigo in ('20250', '70713', '70823')
		);
		
		-- (1) Unificación Terreno con Predio
		create temp table temp_rel_predio_terreno as
		(
			select distinct terrenopredio.id_terreno
					,terrenopredio.numero_predial
					,terrenopredio.numero_predial_precarga
					,terrenopredio.accion_novedad
					,terrenopredio.fecha_modificacion
					,terrenoscampo.t_id_terreno
					,terrenoscampo.local_id
					,terrenoscampo.etiqueta
					,terrenoscampo.fecha_edicion
					,terrenoscampo.fecha_alta
					,geom
			from temp_rel_terreno_predio terrenopredio
			inner join temp_terrenos_campo terrenoscampo on terrenopredio.id_terreno = terrenoscampo.t_id_terreno
		);
		
		-- (2) Generación de Centroides
		create temp table temp_centroides_terreno_predio as
		(
			select id_terreno
						,numero_predial
						,numero_predial_precarga
						,accion_novedad
						,fecha_modificacion
						,t_id_terreno
						,local_id
						,etiqueta
						,fecha_edicion
						,fecha_alta
						,ST_Centroid(geom) geom
			from temp_rel_predio_terreno
		);
		
		create temp table temp_interseccion_rel_terrenopredio_w_ui as
		(
			select distinct t1.id_terreno
					,t1.numero_predial
					,t1.numero_predial_precarga
					,t1.accion_novedad
					,t1.fecha_modificacion
					,t1.t_id_terreno
					,t1.local_id
					,t1.etiqueta
					,t1.fecha_edicion
					,t1.fecha_alta
					,t2.nombre_municipio
					,t2.codigo_unidad_intervencion
					,ST_Intersects(st_transform(t1.geom, 9377), t2.geometria) geom
			from temp_centroides_terreno_predio t1, temp_unidades_intervencion_mh t2
			where ST_Intersects(st_transform(t1.geom, 9377), t2.geometria) is true
		);
		
		create temp table rel_terreno_predio_ui as
		(
			select id_terreno
				,numero_predial
				,numero_predial_precarga
				,accion_novedad
				,fecha_modificacion
				--,t_id_terreno
				--,local_id
				,etiqueta
				,fecha_edicion
				,fecha_alta
				,nombre_municipio
				,codigo_unidad_intervencion
			from temp_interseccion_rel_terrenopredio_w_ui
		);
	
		create temp table temp_relacion_construccion_unidadconstruccion_ui as
		(
		select distinct construccionunidadconstruccion.municipio 
					    ,construccionunidadconstruccion.fecha_sincronizacion
						,construccionunidadconstruccion.numero_predial
						,construccionunidadconstruccion.numero_predial_precarga
						,construccionunidadconstruccion.tableta
						,construccionunidadconstruccion.id_terreno
						,construccionunidadconstruccion.id_construccion 
						,construccionunidadconstruccion.id_unidadconstruccion		
						,construccionunidadconstruccion.tipo_construccion
						,construccionunidadconstruccion.tipo_dominio_construccion
						,construccionunidadconstruccion.numero_pisos_precarga
						,construccionunidadconstruccion.numero_pisos_campo
						,construccionunidadconstruccion.anio_construccion
						,construccionunidadconstruccion.uso_unidad_construccion
						,construccionunidadconstruccion.total_habitaciones_unidad_construccion
						,construccionunidadconstruccion.total_banios_unidad_construccion
						,construccionunidadconstruccion.total_locales_unidad_construccion
						,construccionunidadconstruccion.unidad_construccion_altura
						,construccionunidadconstruccion.anio_construccion_unidad_construccion
						,construccionunidadconstruccion.area_privada_unidad_construccion
						,construccionunidadconstruccion.observaciones
						,construccionunidadconstruccion.id_tipo_calificacion
						,construccionunidadconstruccion.tipo_calificacion
						,construccionunidadconstruccion.fachada
						,construccionunidadconstruccion.puntos_fachada
						,construccionunidadconstruccion.cubrimiento_muros
						,construccionunidadconstruccion.puntos_cubrimiento_muros
						,construccionunidadconstruccion.pisos
						,construccionunidadconstruccion.puntos_pisos
						,construccionunidadconstruccion.conservacion
						,construccionunidadconstruccion.puntos_conservacion_acabados
						,construccionunidadconstruccion.subtotal_acabados
						,construccionunidadconstruccion.tamanio_banio
						,construccionunidadconstruccion.puntos_tamanio_banio
						,construccionunidadconstruccion.enchapes_banio
						,construccionunidadconstruccion.puntos_enchape_banio
						,construccionunidadconstruccion.mobiliario_banio
						,construccionunidadconstruccion.puntos_mobiliario_banio
						,construccionunidadconstruccion.conservacion_banio
						,construccionunidadconstruccion.puntos_conservacion_banio
						,construccionunidadconstruccion.subtotal_banio
						,construccionunidadconstruccion.tamanio_cocina
						,construccionunidadconstruccion.puntos_tamanio_conina
						,construccionunidadconstruccion.enchapes_cocina
						,construccionunidadconstruccion.puntos_enchapes_cocina
						,construccionunidadconstruccion.mobiliario_cocina
						,construccionunidadconstruccion.puntos_mobiliario_cocina
						,construccionunidadconstruccion.conservacion_cocina
						,construccionunidadconstruccion.puntos_conservacion_cocina
						,construccionunidadconstruccion.subtotal_cocina
						,construccionunidadconstruccion.cerchas
						,construccionunidadconstruccion.puntos_cerchas
						,construccionunidadconstruccion.subtotal_cerchas
						,construccionunidadconstruccion.total_calificacion_industria
						,construccionunidadconstruccion.total_calificacion_no_industrial
						,construccionunidadconstruccion.total_calificacion
						,construccionunidadconstruccion.tipo_anexo	
						,(case when terrenopredioui.nombre_municipio is null then 'Posible terreno sin geometria'
							else terrenopredioui.nombre_municipio
							end) nombre_municipio
						,(case when terrenopredioui.codigo_unidad_intervencion is null then 'Posible terreno sin geometria'
							else terrenopredioui.codigo_unidad_intervencion
							end) codigo_unidad_intervencion
		from temp_frm_relacion_construccion_unidadconstruccion construccionunidadconstruccion
		left join rel_terreno_predio_ui terrenopredioui
		on construccionunidadconstruccion.numero_predial = terrenopredioui.numero_predial
		);	

		consulta_1 := format('copy (select *
									from temp_relacion_construccion_unidadconstruccion_ui) to %L CSV HEADER', ruta_archivo);
								
		execute consulta_1;
	
	else
		raise notice 'No se ejecuta';
	end if;

end;$$
