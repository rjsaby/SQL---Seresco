select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');

-- Llamado a Procedimiento Almacenado
/*
call colombiaseg_lote6.actualizacion_datos_lote_6();
call colombiaseg_lote6.analisis_areas_registrales_lote_6();
call colombiaseg_lote6.priorizacion_metodos_lote_6();
call colombiaseg_lote6.relacion_formularios_predios_preoperativos();
-- ***
call estadisticos_priorizacion_predio_metodo_lote_6('20250');
call estadisticos_priorizacion_predio_metodo_lote_6('70713');
call estadisticos_priorizacion_predio_metodo_lote_6('70823');
-- ***
call salidas_graficas_priorizacion_lote_6();
*/
-- Llamado a la generación de csv
--call generacion_csv_lote_6('SI');
call calidad_calificacion_ue_lote_6('SI');
call calidad_interesados_derechos_lote_6('SI');
-- ***
/*
call generacion_csv_lote_6('NO');
call calidad_calificacion_ue_lote_6('NO');
call calidad_interesados_derechos_lote_6('NO');
*/

select dblink_disconnect('conn1');