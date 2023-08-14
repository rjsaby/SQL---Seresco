create or replace procedure salidas_graficas_priorizacion_lote_6()
language plpgsql
as $$
begin

	-- Borrado Tablas Físicas
	drop table if exists tbl_terrenos_en_ui_lote_6;
	drop table if exists terrenos_en_ui_inferiores_100_terrenos;
	drop table if exists ui_con_terrenos_inferiores_100;	
	
	-- (1) Se crea la tabla que contabiliza, por cada unidad de intervención, con cuantos terrenos cuenta.	
	create table tbl_terrenos_en_ui_lote_6 as
	(
		select codigo_unidad_intervencion
			,(case when substring(predio_numero_predial,1,5) = '70713' then 'San Onofre'
				   when substring(predio_numero_predial,1,5) = '70823' then 'Toluviejo'
				   when substring(predio_numero_predial,1,5) = '20250' then 'El Paso'
				   else 'Sin informacion'
				   end) municipio
			,count(*) total_terrenos
		from colombiaseg_lote6.terrenos_digitalizados_lote_6
		-- **** Revisar, por lógica, no deben existir predios sin UI ***
		where codigo_unidad_intervencion is not null
		group by 1, 2
	);
	
	
	-- (2) Se seleccionan los terrenos, desde la vista de priorización, cuyas unidades de intervención no superan los 100 terrenos.
	create table terrenos_en_ui_inferiores_100_terrenos as
	(
	select T1.predio_t_id
		,T1.predio_numero_predial
		,T1.municipio
		,T1.codigo_unidad_intervencion
		,tblterrenoui.total_terrenos
		,T1.seleccion_metodo
		,T1.geometria
	from 
	(
		select vwpriorizacion.predio_t_id
		,vwpriorizacion.predio_numero_predial
		,(case when substring(vwpriorizacion.predio_numero_predial,1,5) = '70713' then 'San Onofre'
			   when substring(vwpriorizacion.predio_numero_predial,1,5) = '70823' then 'Toluviejo'
			   when substring(vwpriorizacion.predio_numero_predial,1,5) = '20250' then 'El Paso'
			   else 'Sin informacion'
			   end) municipio
		,vwpriorizacion.codigo_unidad_intervencion
		,vwpriorizacion.seleccion_metodo
		,vwpriorizacion.geometria
		from colombiaseg_lote6.vw_terrenos_priorizacion_predio_metodo_lote_6 vwpriorizacion
	) T1 inner join tbl_terrenos_en_ui_lote_6 tblterrenoui
	on concat(T1.municipio,T1.codigo_unidad_intervencion) = concat(tblterrenoui.municipio, tblterrenoui.codigo_unidad_intervencion)
	where tblterrenoui.total_terrenos <= 100
	);
	
	-- (3) Se filtra, desde la capa de ui del lote, las geometrias de las unidades que cumplen con el criterio de que
	-- dentro de ellas no se albergan más de 100 terrenos
	create table ui_con_terrenos_inferiores_100 as
	(
	select ui.nombre_municipio
		,ui.codigo_unidad_intervencion
		,ui.geometria
	from colombiaseg_lote6.unidades_intervencion_mh_lote_6 ui
	inner join (select distinct municipio, codigo_unidad_intervencion from terrenos_en_ui_inferiores_100_terrenos) T2 
	on concat(ui.nombre_municipio,ui.codigo_unidad_intervencion) = concat(T2.municipio, T2.codigo_unidad_intervencion)
	);

end;$$
