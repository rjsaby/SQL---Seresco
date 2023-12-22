-- Procedimiento Almacenado
/*
* Actualiza los registros nuevos dentro de la tabla de validación geográfica
*/

/*
 * Requiere:
 * Terrenos Campo
 * */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-11-30
 * */

create or replace procedure actualizacion_validador_geografico_lote_6()
language plpgsql
as $proc_body$
	declare	
		nombre_tabla text;

begin
	
	nombre_tabla := 'bk_validacion_geografica_lote_6_' || to_char(current_date, 'YYYYMMDD');

	--****************************** CONEXIÓN ******************************

	call colombiaseg_lote6.dblink_bd_maphurricane();
	
	--********************************************************************** 
	execute 'drop table if exists ' || nombre_tabla || ';';	
	execute 'create table ' || nombre_tabla || ' as (select * from validacion_geografica_lote_6);';

	insert into validacion_geografica_lote_6 (seguimiento_t_id, local_id, fecha_validacion, validado_por, estado_validacion, geom)
	select -- T1.seguimiento_t_id
		T2.t_id seguimiento_t_id
		,T2.local_id
		,T1.fecha_validacion 
		,T1.validado_por
		,'Sin_Validar' estado_validacion
		,T2.geom
	from validacion_geografica_lote_6 T1
	right join dblink_ue_terreno_campo T2 on T1.seguimiento_t_id = T2.t_id 
	where T1.seguimiento_t_id is null;

	update validacion_geografica_lote_6 as T1
	set geom = T2.geom 
	from (
		select seguimiento_t_id
			,T2.geom
		from validacion_geografica_lote_6 T1
		right join dblink_ue_terreno_campo T2 on T1.geom = T2.geom
		where T1.geom is null
	) T2
	where T1.seguimiento_t_id = T2.seguimiento_t_id;

	update validacion_geografica_lote_6
	set estado_validacion = 'Ajustado'
	where estado_validacion = 'ajustado';
	
	update validacion_geografica_lote_6
	set estado_validacion = 'Aprobado'
	where estado_validacion = 'aprobado';

	update validacion_geografica_lote_6
	set estado_validacion = 'Rechazado'
	where estado_validacion = 'rechazado';

	--alter table validacion_geografica_lote_6 add primary key (seguimiento_t_id);

end;
$proc_body$;