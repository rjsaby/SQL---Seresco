-- Procedimiento Almacenado
--create procedure actualizacion_datos_lote_6()

/*
 * Los procesos que se ejecutan en este PA son:
 * Unidad de Intervención
 * Digitalización Terrenos Lote 6
 * Digitalización Construcciones Lote 6
 * Clase Suelo
 * Estado de Folios
 * Derivados
 * Interesados
 * Derecho
 * Destinación Económica
 * Condición del Predio
 * Predios Socialización Nivel 3
 * Actualización Saldos de Conservación
 * Órdenes de Trabajo
 * Estados de Sincronización FRM Lote 6
 * Vistas
 */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-06-06
 * */

create or replace procedure actualizacion_datos_lote_6()
language plpgsql
as $$
begin
	
	-- Borrado de Tablas
	-- Temporales
	drop table if exists temp_terrenos_lote_6;
	drop table if exists temp_construcciones_lote_6;
	drop table if exists temp_unidad_intervencion;
	drop table if exists temp_conteo_derivados_lote_6;
	drop table if exists terrenos_predio_w_id_interesado;
	drop table if exists terrenos_predio_w_interesado_sin_estandarizar;
	drop table if exists terrenos_predio_w_interesado_estandarizado;
	drop table if exists temp_terreno_orden_trabajo_lote_6;
	drop table if exists temp_relacion_orden_predio_lote_6;
	drop table if exists temp_relacion_predios_orden_sincronizacion_lote_6;
	drop table if exists temp_predios_con_formulario_sincronizado_lote_6;
	drop table if exists estado_predios_lote_6 cascade;
	drop table if exists temp_formularios_sincronizados_lote_6;
	drop table if exists temp_resumen_orden_tableta_npn;
	drop table if exists temp_parametrizacion_estado_predios;
	
	-- Físicas
	drop table if exists terrenos_digitalizados_lote_6 cascade;
	drop table if exists terrenos_gestor_lote_6;
	drop table if exists construcciones_digitalizadas_lote_6;
	drop table if exists estado_folios_matricula_lote_6;
	drop table if exists folio_matriz_folio_derivado_lote_6;
	drop table if exists interesados_lote_6;
	drop table if exists derecho_lote_6;
	drop table if exists destinacion_economica_lote_6;
	drop table if exists condicion_predio_por_ladm_lote_6;
	drop table if exists predio_socializacion_nivel_3;
	drop table if exists unidades_intervencion_mh_lote_6;
	drop table if exists terreno_orden_trabajo_lote_6;
	drop table if exists clase_suelo_lote_6;
	drop table if exists saldos_conservacion_lote_6;
	drop table if exists estado_sincronizacion_lote_6 cascade;
	drop table if exists resumen_orden_tableta_npn;
	drop table if exists saldos_conservacion_gestor_lote_6;
	
	--Vistas
	drop table if exists vw_terrenos_estado_folios_lote_6;
	drop table if exists vw_terrenos_folio_matriz_folio_derivado_lote_6;
	drop table if exists vw_terrenos_saldos_conservacion_lote_6;
	drop table if exists vw_predios_estado_sincronizacion_frm_lote_6;
	drop table if exists vw_estado_predios_lote_6;
	drop table if exists vw_terrenos_priorizacion_predio_metodo_lote_6;

	--****************************** CONEXIÓN ******************************

	call colombiaseg_lote6.dblink_bd_maphurricane();

	-- ******************************** \\MAPHURRICANE\\ INTERRELACIÓN PREDIO\TERRENO\CONSTRUCCION ********************************
	
	-- Terrenos MapHurricane - serladm
	create temp table temp_terrenos_lote_6 as
	(
		select distinct uapredio.t_id predio_t_id
			,uapredio.id_operacion predio_id_operacion
			,uapredio.numero_predial predio_numero_predial
			,ueterreno.t_id terreno_t_id
			,ueterreno.area_terreno terreno_area_terreno
			,st_transform(ueterreno.geometria,9377) geometria
			from dblink_rel_uepredio reluepredio left join dblink_ua_predio uapredio on reluepredio.id_predio = uapredio.t_id											  
											  	  inner join dblink_ue_terreno ueterreno on reluepredio.id_terreno = ueterreno.t_id
			where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823') 
	);

	-- Construcciones MapHurricane - serladm
	create temp table temp_construcciones_lote_6 as
	(
		select distinct uapredio.t_id predio_t_id
			,uapredio.id_operacion predio_id_operacion
			,uapredio.numero_predial predio_numero_predial		
			,ueconstruccion.t_id construccion_t_id
			,ueconstruccion.id_tipoconstruccion construccion_idtipoconstruccion
			,ueconstruccion.area_construccion construccion_areaconstruccion
			,st_transform(ueconstruccion.geometria,9377) geometria
			from dblink_rel_uepredio reluepredio left join dblink_ua_predio uapredio on reluepredio.id_predio = uapredio.t_id											  
											  	  inner join dblink_ue_construccion ueconstruccion on reluepredio.id_construccion = ueconstruccion.t_id
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
			from dblink_rel_unidadintervencion_predio relunidadintervencionpredio inner join dblink_mh_unidadintervencion mhunidadintervencion
																				on relunidadintervencionpredio.id_unidadintervencion = mhunidadintervencion.t_id
	);	
	
	-- Generación Capa -Terrenos Digitalizados-
	create table terrenos_digitalizados_lote_6 as
	(
		select distinct predio_t_id
				,predio_id_operacion
				,predio_numero_predial
				,terreno_t_id
				,terreno_area_terreno
				,tempunidadintervencion.codigo codigo_unidad_intervencion
				,ST_Transform(geometria,9377) geometria	
		from temp_terrenos_lote_6 tempterrenoslote6 left join temp_unidad_intervencion tempunidadintervencion 
														on tempterrenoslote6.predio_t_id = tempunidadintervencion.id_predio
	);
	
	-- Generación Capa -Construcciones Digitalizadas-
	create table construcciones_digitalizadas_lote_6 as
	(
		select distinct predio_t_id
			,predio_id_operacion
			,predio_numero_predial		
			,construccion_t_id
			,construccion_idtipoconstruccion
			,construccion_areaconstruccion
			,tempunidadintervencion.codigo codigo_unidad_intervencion
			,st_transform(geometria,9377) geometria
		from temp_construcciones_lote_6 tempconstruccioneslote6 left join temp_unidad_intervencion tempunidadintervencion 
														on tempconstruccioneslote6.predio_t_id = tempunidadintervencion.id_predio
	);
	
	-- Generación Capa -Unidades Intervención Lote 6-
	create table unidades_intervencion_mh_lote_6 as
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
		from dblink_mh_unidadintervencion mhunidadintervencion
		inner join dblink_mh_unidadintervencionestado mhunidadintervencionestado on mhunidadintervencion.id_estado = mhunidadintervencionestado.t_id 
		inner join dblink_mh_limite_administrativo mhlimiteadministrativo on mhunidadintervencion.id_limite_administrativo = mhlimiteadministrativo.t_id 
		where mhlimiteadministrativo.codigo in ('20250', '70713', '70823')
	);

	-- **************************** INFORMACIÓN GESTOR ****************************

	create table terrenos_gestor_lote_6 as
	(
		select dblinkgcprediocatastro.numero_predial
			,dblinkgcprediocatastro.matricula_inmobiliaria_catastro
			,st_transform(dblinkgcterreno.geometria,9377) geometria
		from dblink_gc_terreno dblinkgcterreno
		inner join dblink_gc_prediocatastro dblinkgcprediocatastro on dblinkgcterreno.gc_predio = dblinkgcprediocatastro.t_id
		where substring(dblinkgcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
	);

-- ******************************************************************************************************************************************************
	
	-- ** Clase de Suelo
	create table clase_suelo_lote_6 as
	(
		select uapredio.t_id predio_t_id
			,uapredio.numero_predial predio_numero_predial
			,tcclasesuelotipo.descripcion 
		from dblink_ua_predio uapredio
		inner join dblink_tc_clasesuelotipo tcclasesuelotipo on uapredio.id_clasesuelotipo = tcclasesuelotipo.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- ** Estado Folios
	-- Requiere la tabla (fuera de modelo) anotacioneslote6
	create table estado_folios_matricula_lote_6 as
	(
	select distinct uapredio.t_id predio_t_id
		,uapredio.matricula_inmobiliaria
		,(case when anotacioneslote6.estado_folio is not null then anotacioneslote6.estado_folio
			   when anotacioneslote6.estado_folio is null then 'Sin información por BD'
			   else 'Error'
			   end) estado_folio
	from dblink_ua_predio uapredio left join colombiaseg_lote6.anotaciones_lote_6 anotacioneslote6 
										on uapredio.matricula_inmobiliaria = anotacioneslote6.folio_matricula
	where uapredio.matricula_inmobiliaria is not null and uapredio.matricula_inmobiliaria <> '0' and uapredio.matricula_inmobiliaria <> ''
	);
	
	-- ** Derivados
	-- Requiere la tabla (fuera de modelo) anotacioneslote6
	create temp table temp_conteo_derivados_lote_6 as
	(
	select t1.folio_matricula
		,count(*) numero_derivados_x_folio_matriz
		from
		(
		select distinct folio_matricula, folio_derivado 
		from colombiaseg_lote6.anotaciones_lote_6
		where folio_derivado is not null
		) t1
		group by t1.folio_matricula
		order by 1
	);
	
	create table folio_matriz_folio_derivado_lote_6 as
	(
		select distinct anotacioneslote6.folio_matricula
			,anotacioneslote6.folio_derivado
			,tempconteoderivadoslote6.numero_derivados_x_folio_matriz
		from colombiaseg_lote6.anotaciones_lote_6 anotacioneslote6
		inner join temp_conteo_derivados_lote_6 tempconteoderivadoslote6 on anotacioneslote6.folio_matricula = tempconteoderivadoslote6.folio_matricula
		where anotacioneslote6.folio_derivado is not null
		order by 1
	);
	
	-- ** Interesados
	-- (1) Terrenos\Predios Vs Relación Interesados Predio
	create temp table terrenos_predio_w_id_interesado as
	(
		select terrenosdigitalizadoslote6.*
			,relprediointeresado.id_interesado 
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		inner join dblink_rel_prediointeresado  relprediointeresado
		on terrenosdigitalizadoslote6.predio_t_id = relprediointeresado.id_predio
	);
	
	-- (2) Terrenos\Predios Vs Interesados
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
		inner join dblink_int_interesado intinteresado
		on terrenosprediowidinteresado.id_interesado = intinteresado.t_id
	);
	
	-- (3) Parametrizacion de dominios
	create temp table terrenos_predio_w_interesado_estandarizado as
	(
	select terrenosprediowinteresadosinestandarizar.predio_t_id
			,terrenosprediowinteresadosinestandarizar.terreno_t_id
			,terrenosprediowinteresadosinestandarizar.t_id_interesado
			,tcinteresadotipo.descripcion tipo_persona
			,tcinteresadodocumentotipo.descripcion tipo_documento
			,terrenosprediowinteresadosinestandarizar.documento_identidad
			,terrenosprediowinteresadosinestandarizar.primer_nombre
			,terrenosprediowinteresadosinestandarizar.segundo_nombre
			,terrenosprediowinteresadosinestandarizar.primer_apellido
			,terrenosprediowinteresadosinestandarizar.segundo_apellido
			,tcsexotipo.descripcion sexo
			,tcgrupoetnicotipo.descripcion grupo_etnico
			,terrenosprediowinteresadosinestandarizar.razon_social
			,terrenosprediowinteresadosinestandarizar.nombre
			,terrenosprediowinteresadosinestandarizar.grupo_etnico2
			,tcestadociviltipo.descripcion estado_civil
	from terrenos_predio_w_interesado_sin_estandarizar terrenosprediowinteresadosinestandarizar
	left join dblink_tc_interesadotipo tcinteresadotipo on terrenosprediowinteresadosinestandarizar.id_interesadotipo = tcinteresadotipo.t_id
	left join dblink_tc_interesadodocumentotipo tcinteresadodocumentotipo on terrenosprediowinteresadosinestandarizar.id_interesadodocumentotipo = tcinteresadodocumentotipo.t_id
	left join dblink_tc_sexotipo tcsexotipo on terrenosprediowinteresadosinestandarizar.id_sexotipo = tcsexotipo.t_id
	left join dblink_tc_grupoetnicotipo tcgrupoetnicotipo on terrenosprediowinteresadosinestandarizar.id_grupoetnico = tcgrupoetnicotipo.t_id
	left join dblink_tc_estadociviltipo tcestadociviltipo on terrenosprediowinteresadosinestandarizar.id_estadociviltipo = tcestadociviltipo.t_id
	);
	
	-- Consulta para exportar tabla de interesados, relacionados con terrenos y predios para todo Lote 6
	-- Requiere temporales (1), (2) y (3)
	create table interesados_lote_6 as
	(
		select predio_t_id
				,terreno_t_id
				,t_id_interesado
				,tipo_persona
				,tipo_documento
				,documento_identidad
				,primer_nombre
				,segundo_nombre
				,primer_apellido
				,segundo_apellido
				,sexo
				,grupo_etnico
				,razon_social
				,nombre
				,grupo_etnico2
				,estado_civil
		from terrenos_predio_w_interesado_estandarizado
	);
	
	-- ** Derecho
	create table derecho_lote_6 as
	(
	select rrrderecho.t_id derecho_t_id
		,rrrderecho.id_predio predio_t_id
		,rrrderecho.id_interesado interesado_t_id
		,uapredio.numero_predial
		,tcderechotipo.descripcion tipo_derecho
		,rrrderecho.fraccion_derecho fraccion_derecho 
		,rrrderecho.fecha_inicio_tenencia
	from dblink_rrr_derecho rrrderecho
	inner join dblink_tc_derechotipo tcderechotipo on rrrderecho.tipo = tcderechotipo.t_id
	left join dblink_ua_predio uapredio on rrrderecho.id_predio = uapredio.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- ** Destinación Económica
	create table destinacion_economica_lote_6 as
	(
		select distinct uapredio.t_id predio_t_id
			,tcdestinacioneconomicatipo.descripcion destinacion_economica
		from dblink_ua_predio uapredio
		inner join dblink_tc_destinacioneconomicatipo tcdestinacioneconomicatipo on uapredio.id_destinacioneconomicatipo = tcdestinacioneconomicatipo.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- ** Condición del Predio
	create table condicion_predio_por_ladm_lote_6 as
	(
		select distinct uapredio.t_id predio_t_id
			,uapredio.numero_predial predio_numero_predial
			,tccondicionprediotipo.descripcion 
		from dblink_ua_predio uapredio
		inner join dblink_tc_condicionprediotipo tccondicionprediotipo on uapredio.id_condicionpredio = tccondicionprediotipo.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- ** Predios para Socializaciones Nivel 3
	create table predio_socializacion_nivel_3 as
	(
		select terrenosdigitalizadoslote6.predio_t_id predio_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial predio_numero_predial
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion codigo_unidad_intervencion
			,estadofoliosmatriculalote6.matricula_inmobiliaria matricula_inmobiliaria
			,estadofoliosmatriculalote6.estado_folio
			,derecholote6.tipo_derecho tipo_derecho
			,derecholote6.fraccion_derecho fraccion_derecho
			,derecholote6.fecha_inicio_tenencia fecha_inicio_tenencia
			,condicionpredioporladmlote6.descripcion condicion_predio
			,destinacioneconomicalote6.destinacion_economica destinacion_economica
			,interesadoslote6.primer_nombre primer_nombre
			,interesadoslote6.segundo_nombre segundo_nombre
			,interesadoslote6.primer_apellido primer_apellido
			,interesadoslote6.segundo_apellido segundo_apellido
			,interesadoslote6.razon_social 
			,interesadoslote6.tipo_persona tipo_persona
			,interesadoslote6.tipo_documento tipo_documento
			,interesadoslote6.documento_identidad documento_identidad
		from terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		left join estado_folios_matricula_lote_6 estadofoliosmatriculalote6 on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
		left join condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6 on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
		left join derecho_lote_6 derecholote6 on terrenosdigitalizadoslote6.predio_t_id = derecholote6.predio_t_id 
		left join interesados_lote_6 interesadoslote6 on interesadoslote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
		left join destinacion_economica_lote_6 destinacioneconomicalote6 on destinacioneconomicalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
	);

	-- ** Actualización Saldos de Conservación	[Digitalización]
	create table saldos_conservacion_lote_6 as
	(
		select terrenosdigitalizadoslote6.predio_t_id 
			,saldosconservacion.*
		from colombiaseg_lote6.saldos_conservacion saldosconservacion
		inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on saldosconservacion.numero_predial = terrenosdigitalizadoslote6.predio_numero_predial
		where saldosconservacion.vigencia_seresco = 'Vigente'
	);


	-- ** Actualización Saldos de Conservación	[Gestor]
	create table saldos_conservacion_gestor_lote_6 as
	(
		select terrenosgestorlote6.numero_predial predio_numero_predial
			,terrenosgestorlote6.matricula_inmobiliaria_catastro
			,terrenosgestorlote6.geometria 
			,saldosconservacion.*
		from colombiaseg_lote6.saldos_conservacion saldosconservacion
		inner join colombiaseg_lote6.terrenos_gestor_lote_6 terrenosgestorlote6 on saldosconservacion.numero_predial = terrenosgestorlote6.numero_predial
		where saldosconservacion.vigencia_seresco = 'Vigente'
	);
	
	-- ** Ordenes de Trabajo
	-- Terrenos \ Ordenes de Trabajo
	create temp table temp_terreno_orden_trabajo_lote_6 as
	(
		select distinct relordenpredio.t_id relacionorden_t_id
			,mhordentrabajo.t_id ordentrabajo_t_id
			,mhordentrabajo.orden_tipo
			,mhordentrabajo.estado_orden
			,mhordentrabajo.codigo_orden
			,mhordentrabajo.id_formulario
			,mhordentrabajo.usuario_asignado
			,mhordentrabajo.usuario_supervisor
			,terrenosdigitalizadoslote6.predio_t_id
			,terrenosdigitalizadoslote6.terreno_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion
			,st_transform(terrenosdigitalizadoslote6.geometria,9377) geometria
		from dblink_rel_ordenpredio relordenpredio
		inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on relordenpredio.id_predio = terrenosdigitalizadoslote6.predio_t_id
		inner join dblink_mh_orden_trabajo mhordentrabajo on mhordentrabajo.t_id = relordenpredio.id_ordentrabajo
	);
	
	-- Asignación de usuarios a la anterior tabla
	create table terreno_orden_trabajo_lote_6 as 
	(
		select distinct tempterrenoordentrabajolote6.relacionorden_t_id
				,tempterrenoordentrabajolote6.ordentrabajo_t_id
				,tempterrenoordentrabajolote6.orden_tipo
				,tcordentipo.descripcion tipo_orden
				--,tempterrenoordentrabajolote6.estado_orden
				,tcestadoorden.descripcion estado_orden
				,tempterrenoordentrabajolote6.codigo_orden
				--,mhformulario.nombre nombre_formulario
				--,tempterrenoordentrabajolote6.usuario_asignado
				,mhusuario.id_rol
				,mhusuariorol.descripcion rol 
				,concat_ws(' ',mhusuario.primer_nombre, mhusuario.segundo_nombre, mhusuario.primer_apellido, mhusuario.segundo_apellido) usuario_asignado
				,mhusuario.email
				,mhusuario."password" 
				,mhusuario.token_odk
				,mhusuario.id_odk 
				--,tempterrenoordentrabajolote6.usuario_supervisor
				,concat_ws(' ',mhusuario.primer_nombre, mhusuario.segundo_nombre, mhusuario.primer_apellido, mhusuario.segundo_apellido) usuario_supervisor
				,tempterrenoordentrabajolote6.predio_t_id
				,tempterrenoordentrabajolote6.predio_numero_predial
				,tempterrenoordentrabajolote6.codigo_unidad_intervencion
				,st_transform(tempterrenoordentrabajolote6.geometria,9377) geometria
		from temp_terreno_orden_trabajo_lote_6 tempterrenoordentrabajolote6
		inner join dblink_mh_usuario mhusuario on tempterrenoordentrabajolote6.usuario_asignado = mhusuario.t_id
		-- Usuario Supervisor
		--inner join dblink_mh_formulario mhformulario on mhformulario.t_id = tempterrenoordentrabajolote6.id_formulario
		inner join dblink_mh_usuario_rol mhusuariorol on mhusuariorol.t_id = mhusuario.id_rol
		inner join dblink_tc_estadoorden tcestadoorden on tcestadoorden.t_id = tempterrenoordentrabajolote6.estado_orden
		inner join dblink_tc_ordentipo tcordentipo on tcordentipo.t_id = tempterrenoordentrabajolote6.orden_tipo
	);

	-- ************************************************* AVANCE SINCRONIZACIÓN FRM *************************************************

	-- (1) Relación Orden\Predio
	create temp table temp_relacion_orden_predio_lote_6 as
	(
		select T1.id_orden	
			,T1.autor
			,T1.max_fecha
			,relordenpredio.id_ordentrabajo
			,relordenpredio.id_predio
		from (
		-- Ordenes Enviadas
		select id_orden
			,autor
			,max(fecha) max_fecha
		from dblink_mh_subidas_campo
		where autor like ('%L6%')
		group by 1, 2
		) T1 inner join dblink_rel_ordenpredio relordenpredio
		on T1.id_orden = relordenpredio.id_ordentrabajo 
	);
	
	-- (2) FRM Sincronizados Lote 6
	-- De esta tabla quedan nulos aquellos npn que no se encuentren en la base de datos original pero que 
	-- si se registraron en campo 
	create temp table temp_formularios_sincronizados_lote_6 as
	(
		select identificacion_predio_numero_predial
			,identificacion_predio_numero_predial_precarga
			,meta_instanceid
			,meta_instancename
			,"key"
			,submitterid
			,submittername
			,status
			,terrenosdigitalizadoslote6.predio_t_id
		from dblink_maphurricane maphurricane
		left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		-- Revisar
		on maphurricane.identificacion_predio_numero_predial_precarga = terrenosdigitalizadoslote6.predio_numero_predial
		where substring(maphurricane.identificacion_predio_numero_predial_precarga,1,5) in ('20250', '70823', '70713')
	);
	
	-- (3) Asignación de orden a cada FRM Sincronizado que presentó cruce
	create temp table temp_predios_con_formulario_sincronizado_lote_6 as
	(
	select tempformulariossincronizadoslote6.identificacion_predio_numero_predial
				,tempformulariossincronizadoslote6.identificacion_predio_numero_predial_precarga
				,tempformulariossincronizadoslote6.meta_instanceid
				,tempformulariossincronizadoslote6.meta_instancename
				,tempformulariossincronizadoslote6."key"
				,tempformulariossincronizadoslote6.submitterid
				,tempformulariossincronizadoslote6.submittername
				,tempformulariossincronizadoslote6.status
				,relordenpredio.id_predio
				,relordenpredio.id_ordentrabajo 
		from temp_formularios_sincronizados_lote_6 tempformulariossincronizadoslote6
		-- FRM sin Orden
		left join dblink_rel_ordenpredio relordenpredio
		on relordenpredio.id_predio = tempformulariossincronizadoslote6.predio_t_id
	);
	
	-- (4) Relación entre todos los predios asociados a ordenes contra los ya sincronizados
	-- Predios no nuevos
	create temp table temp_relacion_predios_orden_sincronizacion_lote_6 as
	(
		select temprelacionordenprediolote6.id_orden	
				,temprelacionordenprediolote6.autor
				,temprelacionordenprediolote6.max_fecha
				,temprelacionordenprediolote6.id_ordentrabajo
				,temprelacionordenprediolote6.id_predio
				,tempprediosconformulariosincronizadolote.submittername usuario_sincronizador
				,null as estado_sincronizacion
		from temp_relacion_orden_predio_lote_6 temprelacionordenprediolote6
		left join temp_predios_con_formulario_sincronizado_lote_6 tempprediosconformulariosincronizadolote
		on temprelacionordenprediolote6.id_predio = tempprediosconformulariosincronizadolote.id_predio
	);
	
	-- (5) Asignación de categoría
	update temp_relacion_predios_orden_sincronizacion_lote_6
	set estado_sincronizacion = 'FRM Sincronizado'
	where usuario_sincronizador is not null;
	
	update temp_relacion_predios_orden_sincronizacion_lote_6
	set estado_sincronizacion = 'FRM NO Sincronizado'
	where usuario_sincronizador is null;
	
	-- (6) Construcción de Tabla Sincronizada
	create table estado_sincronizacion_lote_6 as
	(
		select distinct autor
				,max_fecha
				,id_ordentrabajo
				,id_predio
				,usuario_sincronizador
				,estado_sincronizacion
		from temp_relacion_predios_orden_sincronizacion_lote_6
	);

	-- Verificación Tipificación Predios, relacionado con Sincronización de FRM
	create table estado_predios_lote_6 as
	(
		select distinct T1.predio_t_id
			,T1.estado_predio
			,reluepredio.id_terreno t_id_terreno
		from 
		(
			select distinct uapredio.t_id predio_t_id
				,uapredio.id_estadopredio
				,tcestadopredio.descripcion estado_predio
				from dblink_ua_predio uapredio
				inner join dblink_tc_estadopredio tcestadopredio on uapredio.id_estadopredio = tcestadopredio.t_id
				where substring(uapredio.numero_predial,1,5) in ('20250','70713','70823')
		) T1 inner join dblink_rel_uepredio reluepredio on T1.predio_t_id = reluepredio.id_predio
	);	

	-- ************************************************************* CREACIÓN VISTAS *************************************************************

	-- Vista Terrenos_Estado_Folio_Lote_6
	create view vw_terrenos_estado_folios_lote_6 as
	(
		select distinct row_number () over (order by terrenosdigitalizadoslote6.predio_t_id) id
		,terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.terreno_t_id
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,estadofoliosmatriculalote6.matricula_inmobiliaria
		,estadofoliosmatriculalote6.estado_folio
		,st_transform(terrenosdigitalizadoslote6.geometria,9377) geometria	
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
		inner join estado_folios_matricula_lote_6 estadofoliosmatriculalote6
		on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
	);

	-- Vista Terrenos_Folio_Matriz_Folio_Derivado_Lote_6
	create view vw_terrenos_folio_matriz_folio_derivado_lote_6 as
	(
	select distinct row_number () over (order by terrenosdigitalizadoslote6.predio_t_id) id
		,terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.terreno_t_id
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,foliomatrizfolioderivadolote6.folio_matricula
		,foliomatrizfolioderivadolote6.folio_derivado
		,foliomatrizfolioderivadolote6.numero_derivados_x_folio_matriz
		,st_transform(terrenosdigitalizadoslote6.geometria,9377) geometria
	from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join estado_folios_matricula_lote_6 estadofoliosmatriculalote6 on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
	inner join folio_matriz_folio_derivado_lote_6 foliomatrizfolioderivadolote6 
		on foliomatrizfolioderivadolote6.folio_matricula = estadofoliosmatriculalote6.matricula_inmobiliaria
	);

	-- Vista Terrenos_Saldos_Conservacion_Lote_6
	create view vw_terrenos_saldos_conservacion_lote_6 as
	(
	select distinct row_number () over (order by terrenosdigitalizadoslote6.predio_t_id) id
		,terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.terreno_t_id
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,saldosconservacionlote6.estado_proceso	
		,st_transform(terrenosdigitalizadoslote6.geometria,9377) geometria
	from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join colombiaseg_lote6.saldos_conservacion_lote_6 saldosconservacionlote6
	on terrenosdigitalizadoslote6.predio_t_id = saldosconservacionlote6.predio_t_id
	);

	-- Creación de Vista\Enlazada con Geometrías de Terrenos Digitalizados
	create view vw_predios_estado_sincronizacion_frm_lote_6 as
	(
		select distinct row_number () over (order by terrenosdigitalizados.predio_t_id) id 
			,terrenosdigitalizados.predio_t_id
			,estadosincronizacionlote6.id_ordentrabajo
			,terrenosdigitalizados.predio_numero_predial
			,estadosincronizacionlote6.estado_sincronizacion
			,st_transform(geometria,9377) geometria
			from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizados
		inner join estado_sincronizacion_lote_6 estadosincronizacionlote6 
		on terrenosdigitalizados.predio_t_id = estadosincronizacionlote6.id_predio
	);

-- Creación de Vista Geometrías con Estado asociado a FRM Predios
	create view vw_estado_predios_lote_6 as
	(
	select row_number () over (order by terrenosdigitalizadoslote6.predio_t_id) id 
		,estadopredioslote6.predio_t_id
		,estadopredioslote6.estado_predio
		,estadopredioslote6.t_id_terreno
		,st_transform(geometria,9377) geometria
		from estado_predios_lote_6 estadopredioslote6
		inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		on terrenosdigitalizadoslote6.terreno_t_id = estadopredioslote6.t_id_terreno
	);

	create temp table temp_resumen_orden_tableta_npn as
	(
		select ordentrabajo_t_id orden_trabajo 
	    ,estado_orden 
	    ,codigo_orden
	    ,substring(usuario_asignado,1,7) usuario_asignado
	    ,predio_t_id
	    ,predio_numero_predial
	    ,codigo_unidad_intervencion
	    from terreno_orden_trabajo_lote_6
	);

	create temp table temp_parametrizacion_estado_predios as
	(
		select uapredio.numero_predial 
			,tcestadopredio.descripcion 
		from dblink_ua_predio uapredio
		inner join dblink_tc_estadopredio tcestadopredio
		on uapredio.id_estadopredio = tcestadopredio.t_id
	);

	create table resumen_orden_tableta_npn as
	(
		select tempresumenordentabletanpn.*
			,tempparametrizacionestadopredios.descripcion estado_predio
		from temp_resumen_orden_tableta_npn tempresumenordentabletanpn
		inner join temp_parametrizacion_estado_predios tempparametrizacionestadopredios
		on tempresumenordentabletanpn.predio_numero_predial = tempparametrizacionestadopredios.numero_predial
	);

end;$$