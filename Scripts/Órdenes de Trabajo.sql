drop table if exists temp_terreno_orden_trabajo_lote_6;
drop table if exists temp_formularios_sincronizados_lote_6;
drop table if exists temp_relacion_orden_predio_lote_6;
drop table if exists temp_predios_con_formulario_sincronizado_lote;
drop table if exists temp_relacion_predios_orden_sincronizacion_lote_6;
drop table if exists temp_predios_con_formulario_sincronizado_lote_6;

-- Fisicas
drop table if exists estado_sincronizacion_lote_6;

-- Vista
drop table if exists vw_predios_estado_sincronizacion_frm_lote_6;

-- Terrenos \ Ordenes de Trabajo

create temp table temp_terreno_orden_trabajo_lote_6 as
(
	select relordenpredio.t_id relacionorden_t_id
		,mhordentrabajo.t_id ordentrabajo_t_id
		,mhordentrabajo.orden_tipo
		,mhordentrabajo.estado_orden
		,mhordentrabajo.codigo_orden
		,mhordentrabajo.id_formulario
		,mhordentrabajo.usuario_asignado
		,mhordentrabajo.usuario_supervisor
		,terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,terrenosdigitalizadoslote6.geometria 
	from serladm.rel_ordenpredio relordenpredio
	inner join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on relordenpredio.id_predio = terrenosdigitalizadoslote6.predio_t_id
	inner join serladm.mh_orden_trabajo mhordentrabajo on mhordentrabajo.t_id = relordenpredio.id_ordentrabajo
);

-- Asignación de usuarios a la anterior tabla

drop table if exists terreno_orden_trabajo_lote_6;

create table terreno_orden_trabajo_lote_6 as 
(
	select tempterrenoordentrabajolote6.relacionorden_t_id
			,tempterrenoordentrabajolote6.ordentrabajo_t_id
			--,tempterrenoordentrabajolote6.orden_tipo
			,tcordentipo.descripcion tipo_orden
			--,tempterrenoordentrabajolote6.estado_orden
			,tcestadoorden.descripcion estado_orden
			,tempterrenoordentrabajolote6.codigo_orden
			,mhformulario.nombre nombre_formulario
			--,tempterrenoordentrabajolote6.usuario_asignado
			--,mhusuario.id_rol
			,mhusuariorol.descripcion rol 
			,concat_ws(' ',mhusuario.primer_nombre, mhusuario.segundo_nombre, mhusuario.primer_apellido, mhusuario.segundo_apellido) usuario_asignado
			,mhusuario.email
			,mhusuario."password" 
			,mhusuario.token_odk
			,mhusuario.id_odk 
			--,tempterrenoordentrabajolote6.usuario_supervisor
			,concat_ws(' ',mhusuario_.primer_nombre, mhusuario_.segundo_nombre, mhusuario_.primer_apellido, mhusuario_.segundo_apellido) usuario_supervisor
			,tempterrenoordentrabajolote6.predio_t_id
			,tempterrenoordentrabajolote6.predio_numero_predial
			,tempterrenoordentrabajolote6.codigo_unidad_intervencion
			,tempterrenoordentrabajolote6.geometria 
	from temp_terreno_orden_trabajo_lote_6 tempterrenoordentrabajolote6
	inner join serladm.mh_usuario mhusuario on tempterrenoordentrabajolote6.usuario_asignado = mhusuario.t_id
	-- Usuario Supervisor
	inner join serladm.mh_usuario mhusuario_ on tempterrenoordentrabajolote6.usuario_supervisor = mhusuario_.t_id 
	inner join serladm.mh_formulario mhformulario on mhformulario.t_id = tempterrenoordentrabajolote6.id_formulario
	inner join serladm.mh_usuario_rol mhusuariorol on mhusuariorol.t_id = mhusuario.id_rol
	inner join serladm.tc_estadoorden tcestadoorden on tcestadoorden.t_id = tempterrenoordentrabajolote6.estado_orden
	inner join serladm.tc_ordentipo tcordentipo on tcordentipo.t_id = tempterrenoordentrabajolote6.orden_tipo
);

-- Avance Geo (por definir)
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
		--,max(fecha) max_fecha
	from serladm.mh_subidas_campo
	where autor like ('%L6%')
	group by 1, 2
	) T1 inner join serladm.rel_ordenpredio relordenpredio
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
	from serladmcampo."MAPHURRICANE" maphurricane
	left join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	on maphurricane.identificacion_predio_numero_predial_precarga = terrenosdigitalizadoslote6.predio_numero_predial
	where substring(maphurricane.identificacion_predio_numero_predial_precarga,1,5) in ('20250', '70823', '70713')
		-- and maphurricane.identificacion_predio_numero_predial_precarga = '20250040100000016A001200000000'
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
	left join serladm.rel_ordenpredio relordenpredio
	on relordenpredio.id_predio = tempformulariossincronizadoslote6.predio_t_id
	-- where tempformulariossincronizadoslote6.identificacion_predio_numero_predial_precarga = '20250040100000016A001200000000'
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
	-- where temprelacionordenprediolote6.id_predio = '18752'
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
	select autor
			,max_fecha
			,id_ordentrabajo
			,id_predio
			,usuario_sincronizador
			,estado_sincronizacion
	from temp_relacion_predios_orden_sincronizacion_lote_6
);

-- (7) Creación de Vista\Enlazada con Geometrías de Terrenos Digitalizados
create view vw_predios_estado_sincronizacion_frm_lote_6 as
(
	select terrenosdigitalizados.predio_t_id
		,estadosincronizacionlote6.id_orden
		,terrenosdigitalizados.predio_numero_predial
		,estadosincronizacionlote6.estado_sincronizacion
		,geometria
		from public.terrenos_digitalizados_lote_6 terrenosdigitalizados
	inner join estado_sincronizacion_lote_6 estadosincronizacionlote6 
	on terrenosdigitalizados.predio_t_id = estadosincronizacionlote6.id_predio
);

-- EJECUCIÓN DE ANALISIS

drop table if exists temp_rel_uepredio;
drop table if exists ue_terreno_campo;

create temp table temp_rel_uepredio as
(
select distinct id_terreno
	,numero_predial
	,instance_id
	,enviado
from serladmcampo.rel_uepredio 
where substring(numero_predial,1,5) in ('20250','70713','70823')
);

create table ue_terreno_campo as
(
select *
from serladmcampo.ue_terreno
where substring(local_id,1,5) in ('20250','70713','70823')
);

select * 
from ue_terreno_campo
where local_id = '2025020250040100000018N001N001';

create table ue_polygons as
(
select *
from serladmcampo.ue_polygons
where substring(predio,1,5) in ('20250','70713','70823')
);

create table ue_divide_lines as
(
select *
from serladmcampo.ue_divide_lines
where substring(predio,1,5) in ('20250','70713','70823')


select *
from serladm.ua_predio
where numero_predial = '20250040100000016A001200000000'

select *
from serladm.tc_estadopredio;

);

	/*
	-- Tableta\NPN
	DO
	$$DECLARE	
    	datestr text := to_char(current_timestamp, 'YYYY-MM-DD-HH24_MI_SS');   
    begin
	    execute format(  
		'COPY ' 
		'(select ordentrabajo_t_id orden_trabajo 
	    ,estado_orden 
	    ,codigo_orden
	    ,rol
	    ,predio_t_id
	    ,predio_numero_predial
	    ,codigo_unidad_intervencion
	    from terreno_orden_trabajo_lote_6) '
		'TO %L CSV HEADER',
	    'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\3_Reporte_Orden_Tableta_NPN\' || datestr || '.csv' 
		);
	end;$$;
	*/

-- SE DEBE VERIFICAR COMO SE REPRESENTAN LOS PREDIOS NUEVOS

select * from serladm.tc_ordentipo
select * from serladm.mh_orto
select * from serladm.mh_subidas_campo
select * from serladm.mh_unidadintervencionestado
select * from serladm.mh_zona
select * from serladm.tc_estadopredio
select * from serladm.tc_estadoorden
select * from serladm.ua_predio

select * from serladm.mh_orden_trabajo


select * from serladm.rel_ordenpredio T1
inner join serladm.ua_predio T2
on T1.id_predio = T2.t_id 
where T1.id_ordentrabajo = 58778;
