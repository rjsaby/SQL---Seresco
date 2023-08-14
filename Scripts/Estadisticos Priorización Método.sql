	-- Borrado de Tablas
	-- Temporales
	drop table if exists temp_superior_en_area_estandar;
	drop table if exists temp_terrenos_nuevos_digitalizacion;
	drop table if exists temp_posible_informalidad;
	drop table if exists temp_area_registral;
	drop table if exists temp_predios_derivados; 
	drop table if exists temp_predios_saldos;
	
	-- Físicas
	drop table if exists estadisticos_priorizacion_predio_metodo_lote_6_20250;
	
	--Vistas

	-- Categorización Superior en área a lo suministrado por el GC
	create temp table temp_superior_en_area_estandar as
	(
	select superior_en_area_estandar
		,count(*) conteo_superior_en_area_estandar
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	-- Categorización Terrenos digitalizados sin relación con terrenos GC
	create temp table temp_terrenos_nuevos_digitalizacion as
	(
	select terrenos_nuevos_digitalizacion
		,count(*) conteo_terrenos_nuevos_digitalizacion
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	-- Categorización Posibles informalidades
	create temp table temp_posible_informalidad as
	(
	select posible_informalidad
		,count(*) conteo_posible_informalidad
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	-- Categorización Verificación de áreas geométricas vs áreas registrales
	create temp table temp_area_registral as
	(
	select verificacion_area_registral
		,count(*) conteo_verificacion_area_registral
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	-- Categorización Predios con Derivados
	create temp table temp_predios_derivados as
	(
	select predios_con_derivados
		,count(*) conteo_predios_con_derivados
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	-- Categorización Predios con Saldos
	create temp table temp_predios_saldos as
	(
	select predios_con_saldos
		,count(*) conteo_predios_con_saldos
		from priorizacion_predio_metodo_lote_6
		where substring(predio_numero_predial,1,5) = '20250'
		group by 1
	);
	
	create table estadisticos_priorizacion_predio_metodo_lote_6_20250 as 
	(
	select t1.superior_en_area_estandar
		,t1.conteo_superior_en_area_estandar
		,t2.terrenos_nuevos_digitalizacion
		,t2.conteo_terrenos_nuevos_digitalizacion
		,t3.posible_informalidad
		,t3.conteo_posible_informalidad
		,t4.verificacion_area_registral
		,t4.conteo_verificacion_area_registral
		,t5.predios_con_derivados
		,t5.conteo_predios_con_derivados
		,t6.predios_con_saldos
		,t6.conteo_predios_con_saldos
	from temp_superior_en_area_estandar t1 
		left join temp_terrenos_nuevos_digitalizacion t2 on t1.superior_en_area_estandar = t2.terrenos_nuevos_digitalizacion
		left join temp_posible_informalidad t3 on t1.superior_en_area_estandar = t3.posible_informalidad
		left join temp_area_registral t4 on t1.superior_en_area_estandar = t4.verificacion_area_registral
		left join temp_predios_derivados t5 on t1.superior_en_area_estandar = t5.predios_con_derivados
		left join temp_predios_saldos t6 on t1.superior_en_area_estandar = t6.predios_con_saldos
	);