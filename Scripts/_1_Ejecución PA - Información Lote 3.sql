select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');

-- Llamado a Procedimiento Almacenado

-- ***
-- ***

-- Llamado a la generación de csv
--call calidad_calificacion_ue_lote_3('SI');
call calidad_interesados_derechos_lote_3('SI');
-- ***

select dblink_disconnect('conn1');