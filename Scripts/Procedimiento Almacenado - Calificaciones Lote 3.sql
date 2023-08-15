-- Procedimiento Almacenado
create or replace procedure calidad_calificacion_ue_lote_3(ejecucion text)
language plpgsql
as $$
	declare
		consulta_1 text;
		nombre_archivo text;
		ruta_archivo text;
	
begin
	
	if ejecucion = 'SI' then		
		nombre_archivo := 'calificaciones_unidades_construccion_lote_3';
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
		
		create temp table temp_maphurricane as
		(
		select distinct submissiondate
		,identificacion_predio_numero_predial
		,meta_instanceid
		,"key"
		,submittername
		from dblink_maphurricane
		where substring(identificacion_predio_numero_predial,1,5) in ('13458','20787','20032', '13268')
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
		where substring(numero_predial,1,5) in ('13458','20787','20032', '13268') and (id_construccion is not null)
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
			where substring(mapconstruccion.numero_predial_18,1,5) in ('13458','20787','20032', '13268')
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
			where substring(numero_predial_19,1,5) in ('13458','20787','20032', '13268')
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
		select distinct (case when substring(relacion_2.numero_predial,1,5)= '13458' then 'Montecristo'
						      when substring(relacion_2.numero_predial,1,5)= '20787' then 'Tamalameque'
						      when substring(relacion_2.numero_predial,1,5)= '20032' then 'Astrea'
						      when substring(relacion_2.numero_predial,1,5)= '13268' then 'El Peniol'
						      else 'Error codificacion'
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

		consulta_1 := format('copy (select *
									from temp_frm_relacion_construccion_unidadconstruccion) to %L CSV HEADER', ruta_archivo);
								
		execute consulta_1;
	
	else
		raise notice 'No se ejecuta';
	end if;

end;$$
