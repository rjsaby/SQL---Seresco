-- Procedimiento Almacenado
--create procedure actualizacion_datos_lote_6()

/*
 * Los procesos que se ejecutan en este PA son:
 * Implementación de tabla base, creada equipo Jurídico con el análisis por predio del área registral
 */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-06-06
 * */

create or replace procedure analisis_areas_registrales_lote_6()
language plpgsql
as $$
begin
	
	-- Borrado de Tablas
	-- Temporales
	drop table if exists temp_preliminar_analisis_areas;
	
	-- Físicas
	drop table if exists verificacion_fisico_juridica_areas_lote_6;
	
	-- Vistas
	drop table if exists vw_verificacion_fisico_juridica_por_diferencia_area;

	--****************************** CONEXIÓN ******************************

	call colombiaseg_lote6.dblink_bd_maphurricane();

	--****************************** PERMISOS ******************************

    -- GRANT SELECT ON ALL TABLES IN SCHEMA colombiaseg_lote6 TO cmconsulta;
	

	-- ****************************** EJECUCION ******************************
	
	-- (1) Parametrización de diferencias de área entre lo geográfico y lo registral a partir de la 1101
	create temp table temp_preliminar_analisis_areas as 
	(
			select distinct terrenosdigitalizadoslote6.predio_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial
			,analisisareasfmilote6.folio_matricula
			,terrenosdigitalizadoslote6.terreno_area_terreno area_terreno_bd_m
			,analisisareasfmilote6.area_total_registral_m area_registral_m
			,ST_Area(st_transform(geometria,9377)) area_calculada_m2
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion
			,(case when ST_Area(st_transform(geometria,9377)) <= 2000 then 0.10
					when ST_Area(st_transform(geometria,9377)) > 2000 and ST_Area(st_transform(geometria,9377)) <= 10000 then 0.09
					when ST_Area(st_transform(geometria,9377)) > 10000 and ST_Area(st_transform(geometria,9377)) <= 100000 then 0.07
					when ST_Area(st_transform(geometria,9377)) > 100000 and ST_Area(st_transform(geometria,9377)) <= 500000 then 0.04
					when ST_Area(st_transform(geometria,9377)) > 500000 then 0.02
					else 0
					end) tolerancia
			,ROUND(abs(ST_Area(st_transform(geometria,9377)) - analisisareasfmilote6.area_total_registral_m)) diferencia_area_calculada_vs_registral
			from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
			-- Analisa el informe de los tecnico jurídicos en cuanto a áreas registrales.
			inner join colombiaseg_lote6.analisis_areas_fmi_lote_6 analisisareasfmilote6																	 	
				on terrenosdigitalizadoslote6.predio_numero_predial = analisisareasfmilote6.numero_predial
	);

		-- (2) Creación Tabla verificacion_fisico_juridica_areas_lote_6
		create table verificacion_fisico_juridica_areas_lote_6 as
		(
		select t1.predio_t_id
			,t1.predio_numero_predial
			,t1.folio_matricula
			,t1.area_terreno_bd_m
			,t1.area_registral_m
			,t1.area_calculada_m2
			,t1.codigo_unidad_intervencion
			,t1.tolerancia
			,t1.diferencia_area_calculada_vs_registral
			,t1.rango_area
			,(case when diferencia_area_calculada_vs_registral <= rango_area then 'No Requiere'
				   when diferencia_area_calculada_vs_registral > rango_area then 'Requiere'
				   else 'Sin información'
				   end) verificacion_linderos_o_info_juridica
		from 
		(
			select predio_t_id
				,predio_numero_predial
				,folio_matricula
				,area_terreno_bd_m
				,area_registral_m
				,area_calculada_m2
				,codigo_unidad_intervencion
				,tolerancia
				,diferencia_area_calculada_vs_registral
				,round((area_calculada_m2 * tolerancia)) rango_area
			from temp_preliminar_analisis_areas
		) t1
	);

	-- Creación de Vista -vw_verificacion_fisico_juridica_x_diferencia_areas-
	create view vw_verificacion_fisico_juridica_por_diferencia_area as
	(
		select row_number () over (order by terrenosdigitalizadoslote6.predio_t_id) id 
			,terrenosdigitalizadoslote6.*
			,verificacionfisicojuridicaareaslote6.area_terreno_bd_m
			,verificacionfisicojuridicaareaslote6.area_registral_m
			,verificacionfisicojuridicaareaslote6.area_calculada_m2
			,verificacionfisicojuridicaareaslote6.tolerancia
			,verificacion_linderos_o_info_juridica
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
		inner join verificacion_fisico_juridica_areas_lote_6 verificacionfisicojuridicaareaslote6
		on terrenosdigitalizadoslote6.predio_numero_predial = verificacionfisicojuridicaareaslote6.predio_numero_predial
	);	
	
end;$$
