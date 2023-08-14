-- Procedimiento Almacenado
create or replace procedure generacion_csv_lote_6(ejecucion text)
language plpgsql
as $$
	declare 
		consulta_1 text;
		consulta_2 text;
		consulta_3 text;		
	    nombre_archivo_1 text;
		nombre_archivo_3 text;
	    ruta_archivo_1 text;
		ruta_archivo_2 text;
		ruta_archivo_3 text;
			
begin
	
	if ejecucion = 'SI' then	
		nombre_archivo_1 := '_Estadisticos_Priorizacion_Predio_Metodo_Global_Lote_6_';
		nombre_archivo_3 := '_estado_enlace_formularios_predios_preoperativo_lote_6';
		ruta_archivo_1 := 'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_4_Modelo_Operacion\1_Priorizacion_Por_Metodo\Estadisticos\' || nombre_archivo_1 || to_char(current_date, 'YYYYMMDD') || '.csv';
		ruta_archivo_2 := 'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\3_Reporte_Orden_Tableta_NPN\' || to_char(current_timestamp, 'YYYY-MM-DD-HH-MI-SS') || '.csv';
	    ruta_archivo_3 := 'D:\PUBLIC\SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\4_Relacion_FRMSincronizado_EnlaceNPNPreoperativo\' || to_char(current_date, 'YYYYMMDD') || nombre_archivo_3 || '.csv';

			-- ************************************************* EXPORT CSV *************************************************
	
		-- Priorización Predio Método Lote 6
		consulta_1 := format('copy (select predio_t_id
									,predio_numero_predial
									,codigo_unidad_intervencion
									,superior_en_area_estandar
									,categorizacion_superior_en_area_estandar
									,terrenos_nuevos_digitalizacion
									,categorizacion_terrenos_nuevos_digitalizacion
									,posible_informalidad
									,categorizacion_posible_informalidad
									,verificacion_area_registral
									,categorizacion_verificacion_area_registral
									,predios_con_derivados
									,categorizacion_predios_con_derivados
									,predios_con_saldos
									,categorizacion_predios_con_saldos
									,terrenos_diferencia_construcciones
									,categorizacion_terrenos_diferencia_construcciones
									,totalizacion_categorias
									,seleccion_metodo
									,priorizacion_metodo_indirecto
									from colombiaseg_lote6.priorizacion_predio_metodo_lote_6) to %L CSV HEADER', ruta_archivo_1);
								
		-- Resumen Orden Tableta NPN
		consulta_2 := format('copy (select orden_trabajo
									,estado_orden
									,codigo_orden
									,usuario_asignado
									,predio_t_id
									,predio_numero_predial
									,codigo_unidad_intervencion
									,estado_predio
									from colombiaseg_lote6.resumen_orden_tableta_npn) to %L CSV HEADER', ruta_archivo_2);
								
		-- Estado Enlace Formularios Predios Preoperativo Lote 6 
		consulta_3 := format('copy (select identificacion_predio_numero_predial
									,precarga_predio_t_id
									,resultado_visita
									,con_geometria
									from colombiaseg_lote6.estado_enlace_formularios_predios_preoperativo_lote_6) to %L CSV HEADER', ruta_archivo_3);
								
		execute consulta_1;
		execute consulta_2;
		execute consulta_3;	
	else
		raise notice 'No se ejecuta';
	end if;
	
end;$$