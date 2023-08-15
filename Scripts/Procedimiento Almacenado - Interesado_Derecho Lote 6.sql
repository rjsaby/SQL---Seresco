-- Procedimiento Almacenado
create or replace procedure calidad_interesados_derechos_lote_6(ejecucion text)
language plpgsql
as $$
	declare
		consulta_1 text;
		nombre_archivo text;
		ruta_archivo text;

begin
	
	if ejecucion = 'SI' then		
		nombre_archivo := 'interesados_derechos_lote_6';
		ruta_archivo := 'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\11_Calidad_Interesados_Derecho\' || nombre_archivo || '.csv';

		-- *** Conexión DBLINK ***
		call colombiaseg_lote6.dblink_bd_maphurricane();
		
		-- Borrrado tablas
		-- Temporales
		drop table if exists temp_maphurricane;
		drop table if exists temp_interesado_campo;
		drop table if exists temp_frm_interesado;
		drop table if exists temp_derecho_campo;
		drop table if exists temp_frm_interesado_derecho;
		drop table if exists temp_terrenos_campo;
		drop table if exists temp_rel_terreno_predio;
		drop table if exists temp_unidades_intervencion_mh;
		drop table if exists temp_rel_predio_terreno;
		drop table if exists temp_centroides_terreno_predio;
		drop table if exists temp_interseccion_rel_terrenopredio_w_ui;
		drop table if exists rel_terreno_predio_ui;
		drop table if exists temp_frm_interesado_derecho_w_ui;
		
		create temp table temp_maphurricane as
		(
		select submissiondate fecha_sincronizacion
				,identificacion_predio_numero_predial
				,meta_instanceid
				,substring(submittername,1,7) tableta
				,domrelacionprediotipo.dispname tipo_contacto_visita		
				,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta13 numero_dni_precarga
				,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta17 primer_nombre_contacto_visita
				,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta21 segundo_nombre_contacto_visita
				,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta25 primer_apellido_contacto_visita
				,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta29 segundo_apellido_concacto_visita
				,tcsexotipo.descripcion sexo
				,tcgrupoetnico.descripcion grupo_etnico
				,grupo_encuesta_grupo_sec_iii_interesados_total_interesados total_interesados
				,intagrupacioninteresados.nombre id_agrupacion
				,grupo_encuesta_grupo_sec_iii_interesados_int_agrupacionintere00 nombre_agrupacion
		from dblink_maphurricane maphurricane
		left join dblink_lc_relacionprediotipo domrelacionprediotipo 
			on cast(maphurricane.grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_preguntas_ as int8) = domrelacionprediotipo.itfcode
		left join dblink_tc_sexotipo tcsexotipo
			on cast(maphurricane.grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta30 as int8) = tcsexotipo.codigo
		left join dblink_tc_grupoetnicotipo tcgrupoetnico
			on cast(maphurricane.grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta31 as int8) = tcgrupoetnico.codigo
		left join dblink_int_agrupacioninteresados intagrupacioninteresados
			on cast(maphurricane.grupo_encuesta_grupo_sec_iii_interesados_int_agrupacioninteresa as int8) = intagrupacioninteresados.id_grupointeresadotipo
		where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823')
		);
		
		create temp table temp_interesado_campo as
		(
			select interesadocampo.int_interesado_datos_contacto_numero_predial_14 numero_predial_interesado
				,tcinteresadotipo.descripcion interesado_tipo
				,tcinteresadodocumentotipo.descripcion tipo_interesado_documento
				,interesadocampo.int_interesado_datos_contacto_int_interesado_documento_identida dni_interesado
				,interesadocampo.int_interesado_datos_contacto_int_interesado_razon_social razon_social_interesado
				,interesadocampo.int_interesado_datos_contacto_nombre interesado_nombre_completo
				,tcsexotipo.descripcion sexo_interesado
				,tcgrupoetnicotipo.descripcion grupo_etnico_interesado
				,tcestadociviltipo.descripcion estado_civil_interesado
				,interesadocampo.parent_key
				,interesadocampo."key"
			from dblink_int_interesado_campo interesadocampo
			left join dblink_tc_grupoetnicotipo tcgrupoetnicotipo
				on cast(interesadocampo.int_interesado_datos_contacto_int_interesado_id_grupoetnico as int8) = tcgrupoetnicotipo.codigo
			left join dblink_tc_sexotipo tcsexotipo
				on cast(interesadocampo.int_interesado_datos_contacto_int_interesado_id_sexotipo as int8) = tcsexotipo.codigo
			left join dblink_tc_estadociviltipo tcestadociviltipo
				on cast(interesadocampo.int_interesado_datos_contacto_contactovisita_id_estadociviltipo as int8) = tcestadociviltipo.codigo
			left join dblink_tc_interesadotipo tcinteresadotipo
				on cast(interesadocampo.int_interesado_datos_contacto_int_interesado_id_interesadotipo as int8) = tcinteresadotipo.codigo
			left join dblink_tc_interesadodocumentotipo tcinteresadodocumentotipo
				on cast(interesadocampo.int_interesado_datos_contacto_int_interesado_id_interesadodocum as int8) = tcinteresadodocumentotipo.codigo
			where substring(interesadocampo.int_interesado_datos_contacto_numero_predial_14,1,5) in ('20250','70713','70823') or interesadocampo.int_interesado_datos_contacto_numero_predial_14 is null
		);
		
		create temp table temp_derecho_campo as
		(
			select tcderechotipo.descripcion tipo_derecho
			,tempderechocampo.rrr_derecho_grupo_rrr_derecho_fraccion_derecho fraccion_derecho
			,tempderechocampo.rrr_derecho_grupo_rrr_derecho_fecha_inicio_tenencia fecha_inicio_tenencia
			,tempderechocampo.rrr_derecho_grupo_rrr_derecho_descripcion descripcion_derecho
			,tempderechocampo.parent_key
			,tempderechocampo."key"
			from dblink_rrr_derecho_campo tempderechocampo
			left join dblink_tc_derechotipo tcderechotipo
				on cast(tempderechocampo.rrr_derecho_grupo_rrr_derecho_tipo_valor_defecto as int8) = tcderechotipo.codigo
		);
		
		-- (1) Relación FRM y Interesado
		create temp table temp_frm_interesado as
		(
			select tempmaphurricane.fecha_sincronizacion
					,tempmaphurricane.identificacion_predio_numero_predial
					,tempmaphurricane.meta_instanceid
					,tempmaphurricane.tableta
					,tempmaphurricane.tipo_contacto_visita		
					,tempmaphurricane.numero_dni_precarga
					,tempmaphurricane.primer_nombre_contacto_visita
					,tempmaphurricane.segundo_nombre_contacto_visita
					,tempmaphurricane.primer_apellido_contacto_visita
					,tempmaphurricane.segundo_apellido_concacto_visita
					,tempmaphurricane.sexo
					,tempmaphurricane.grupo_etnico
					,tempmaphurricane.total_interesados
					,tempmaphurricane.id_agrupacion
					,tempmaphurricane.nombre_agrupacion
					,tempinteresadocampo.numero_predial_interesado
					,tempinteresadocampo.interesado_tipo
					,tempinteresadocampo.tipo_interesado_documento
					,tempinteresadocampo.dni_interesado
					,tempinteresadocampo.razon_social_interesado
					,tempinteresadocampo.interesado_nombre_completo
					,tempinteresadocampo.sexo_interesado
					,tempinteresadocampo.grupo_etnico_interesado
					,tempinteresadocampo.estado_civil_interesado
					,tempinteresadocampo.parent_key
					,tempinteresadocampo."key"
			from temp_maphurricane tempmaphurricane
			inner join temp_interesado_campo tempinteresadocampo
			on tempmaphurricane.meta_instanceid = tempinteresadocampo.parent_key
		);
		
		--(2) Relación FRM\Interesado y Derecho
		create temp table temp_frm_interesado_derecho as
		(
			select tempfrminteresado.fecha_sincronizacion
				,(case when substring(tempfrminteresado.identificacion_predio_numero_predial, 1,5) = '20250' then 'El Paso'
					when substring(tempfrminteresado.identificacion_predio_numero_predial, 1,5) = '70713' then 'San Onofre'
					when substring(tempfrminteresado.identificacion_predio_numero_predial, 1,5) = '70823' then 'Toluviejo'
					else 'Error codificacion'
				end) municipio
				,tempfrminteresado.identificacion_predio_numero_predial
				,tempfrminteresado.tableta
				,tempfrminteresado.tipo_contacto_visita		
				,tempfrminteresado.numero_dni_precarga
				,tempfrminteresado.primer_nombre_contacto_visita
				,tempfrminteresado.segundo_nombre_contacto_visita
				,tempfrminteresado.primer_apellido_contacto_visita
				,tempfrminteresado.segundo_apellido_concacto_visita
				,tempfrminteresado.sexo
				,tempfrminteresado.grupo_etnico
				,tempfrminteresado.total_interesados
				,tempfrminteresado.id_agrupacion
				,tempfrminteresado.nombre_agrupacion
				,tempfrminteresado.interesado_tipo
				,tempfrminteresado.tipo_interesado_documento
				,tempfrminteresado.dni_interesado
				,tempfrminteresado.razon_social_interesado
				,tempfrminteresado.interesado_nombre_completo
				,tempfrminteresado.sexo_interesado
				,tempfrminteresado.grupo_etnico_interesado
				,tempfrminteresado.estado_civil_interesado
				,tempderechocampo.tipo_derecho
				,tempderechocampo.fraccion_derecho
				,tempderechocampo.fecha_inicio_tenencia
				,tempderechocampo.descripcion_derecho
			from temp_frm_interesado tempfrminteresado
			inner join temp_derecho_campo tempderechocampo
			on tempfrminteresado."key" = tempderechocampo.parent_key
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
		
		create temp table temp_frm_interesado_derecho_w_ui as
		(
		select distinct interesadoderecho.fecha_sincronizacion
				,interesadoderecho.municipio
				,interesadoderecho.identificacion_predio_numero_predial
				,interesadoderecho.tableta
				,interesadoderecho.tipo_contacto_visita		
				,interesadoderecho.numero_dni_precarga
				,interesadoderecho.primer_nombre_contacto_visita
				,interesadoderecho.segundo_nombre_contacto_visita
				,interesadoderecho.primer_apellido_contacto_visita
				,interesadoderecho.segundo_apellido_concacto_visita
				,interesadoderecho.sexo
				,interesadoderecho.grupo_etnico
				,interesadoderecho.total_interesados
				,interesadoderecho.id_agrupacion
				,interesadoderecho.nombre_agrupacion
				,interesadoderecho.interesado_tipo
				,interesadoderecho.tipo_interesado_documento
				,interesadoderecho.dni_interesado
				,interesadoderecho.razon_social_interesado
				,interesadoderecho.interesado_nombre_completo
				,interesadoderecho.sexo_interesado
				,interesadoderecho.grupo_etnico_interesado
				,interesadoderecho.estado_civil_interesado
				,interesadoderecho.tipo_derecho
				,interesadoderecho.fraccion_derecho
				,interesadoderecho.fecha_inicio_tenencia
				,interesadoderecho.descripcion_derecho
				,(case when terrenopredioui.nombre_municipio is null then 'Posible terreno sin geometria'
					else terrenopredioui.nombre_municipio
					end) nombre_municipio
				,(case when terrenopredioui.codigo_unidad_intervencion is null then 'Posible terreno sin geometria'
					else terrenopredioui.codigo_unidad_intervencion
					end) codigo_unidad_intervencion
		from temp_frm_interesado_derecho interesadoderecho
		left join rel_terreno_predio_ui terrenopredioui
		on interesadoderecho.identificacion_predio_numero_predial = terrenopredioui.numero_predial
		);	
	
		consulta_1 := format('copy (select *
									from temp_frm_interesado_derecho_w_ui) to %L CSV HEADER', ruta_archivo);
								
		execute consulta_1;
	
		else
			raise notice 'No se ejecuta';
		end if;
		
end;$$
