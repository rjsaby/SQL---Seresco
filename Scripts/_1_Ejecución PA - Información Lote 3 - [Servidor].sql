select dblink_connect('conn1', 'host=172.19.8.3 dbname=seguimiento user=rodian password=rodian811');

-- Llamado a Procedimiento Almacenado

call colombiaseg_lote6.actualizacion_datos_lote_3();

-- ***
-- ***
-- ***
call estadisticos_interrelacion_catastro_registro_lote_3();

-- Llamado a la generación de csv
-- ***
call calidad_calificacion_ue_lote_3('NO');
call calidad_interesados_derechos_lote_3('NO');
-- ***

select dblink_disconnect('conn1');