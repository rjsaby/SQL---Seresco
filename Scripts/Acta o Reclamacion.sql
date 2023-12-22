select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');

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

	
select *
from temp_predio_acta_o_reclamacion;

select dblink_disconnect('conn1');


select t_id
	,itfcode codigo_dominio
	,ilicode nombre_dominio
from serladm.lc_anexotipo