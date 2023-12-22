-- Procedimiento Almacenado
--create procedure actualizacion_datos_lote_6()

/*
 * Los procesos que se ejecutan en este PA son:
 * Se construye el manzaneo, lote 6 que apoya los procesos
 * de salidas gráficas. 
 */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-11-30
 * */

create or replace procedure actualizacion_manzana_lote_6()
language plpgsql
as $body$
begin

	--****************************** CONEXIÓN ******************************
	call colombiaseg_lote6.dblink_bd_maphurricane();

	--****************************** BORRADO ******************************
	-- Borrado Tablas
	-- Temporales
	drop table if exists temp_manzanas_lote_6;
	drop table if exists temp_centroide_manzanas_lote_6;
	drop table if exists temp_centroide_manzana_con_ui_lote_6;
	drop table if exists temp_manzana_con_ui_lote_6;
	
	-- Física
	drop table if exists manzana_lote_6;

	--****************************** PROCEDIMIENTO ******************************
	
	create temp table temp_manzanas_lote_6 as
	(
	select t_id
		,codigo
		,st_transform(geometria, 9377) geometria
	from dblink_gc_manzana
	where substring(codigo, 1, 5) in ('20250','70713','70823')
	);
	
	
	create temp table temp_centroide_manzanas_lote_6 as
	(
		select t_id
			,codigo
			,ST_Centroid(geometria) geometria
		from temp_manzanas_lote_6
	);
	
	create temp table temp_centroide_manzana_con_ui_lote_6 as
	(
		select T1.t_id
			,T1.codigo
			,T1.geometria
			,T2.codigo_unidad_intervencion
			,ST_Intersects(T1.geometria, T2.geometria)
		from temp_centroide_manzanas_lote_6 T1, unidades_intervencion_mh_lote_6 T2
		where ST_Intersects(T1.geometria, T2.geometria) is true
	);
	
	create temp table temp_manzana_con_ui_lote_6 as
	(
		select T1.t_id
			,T1.codigo
			,T1.geometria
			,T2.codigo_unidad_intervencion	
		from temp_manzanas_lote_6 T1
		inner join temp_centroide_manzana_con_ui_lote_6 T2
		on T1.t_id = T2.t_id
	);
	
	create table manzana_lote_6 as
	(
		select t_id t_id_manzana
			,codigo codigo_manzana	 
			,substring(codigo,1,5) codigo_municipio
			,(case when substring(codigo,1,5) = '20250' then 'El Paso'
				when substring(codigo,1,5) = '70713' then 'San Onofre'
				when substring(codigo,1,5) = '70823' then 'Toluviejo'
				else substring(codigo,1,5)
				end) nombre_municipio
			,codigo_unidad_intervencion
			,geometria
		from temp_manzana_con_ui_lote_6
	);

end;
$body$;
