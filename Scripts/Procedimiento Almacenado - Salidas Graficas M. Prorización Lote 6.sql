create or replace procedure salidas_graficas_priorizacion_lote_6()
language plpgsql
as $$
begin

	-- Borrado Tablas Físicas
	drop table if exists tbl_terrenos_en_ui_lote_6;
	
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

end;$$
