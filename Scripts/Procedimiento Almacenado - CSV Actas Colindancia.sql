-- Procedimiento Almacenado
create or replace procedure csv_actas_colindancia_abandono_forzado_lote_6(ejecucion text)
language plpgsql
as $$
	declare
		consulta_1 text;
		nombre_archivo text;
		ruta_archivo text;

begin
	
	if ejecucion = 'SI' then		
		nombre_archivo := 'actas_colindancia_abandono_forzado_lote_6';
		ruta_archivo := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_9_Operacion\_7_9_2_Orden_Trabajo\12_Terreno_Actas_Colindancia\' || nombre_archivo || '.csv';

		-- *** Conexión DBLINK ***
		call colombiaseg_lote6.dblink_bd_maphurricane();
		
		-- Borrrado tablas
		-- Temporales
		drop table if exists temp_predio_acta_o_reclamacion;

		
	create temp table temp_predio_acta_o_reclamacion as
	(
	select --meta_instanceid
		identificacion_predio_numero_predial predio_numero_predial
		,(case when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '1' then 'Si'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '0' then 'No'
			else 'Verificar FRM'
			end) suscribe_acta_colindancia
		,(case when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit03 = '1' then 'Si'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 = '0' then 'No'
			when grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 is null then 'Registro nulo desde FRM'
			else 'Verificar FRM'
			end) reclamacion_abandono_forzado_reclamacion 
	from dblink_maphurricane
	where substring(identificacion_predio_numero_predial,1,5) in ('20250','70713','70823') and
		grupo_encuesta_grupo_sec_vi_datos_adicionales_predio_ua_visit02 is not null
	);
	
		consulta_1 := format('copy (select *
									from temp_predio_acta_o_reclamacion) to %L CSV HEADER', ruta_archivo);
								
		execute consulta_1;
	
		else
			raise notice 'No se ejecuta';
		end if;
		
end;$$