select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');

call colombiaseg_lote6.dblink_bd_maphurricane();

-- Borrado Tablas

-- Temporales:
drop table if exists temp_numeros_prediales_precarga;
drop table if exists temp_asignacion_predio_t_id;
drop table if exists temp_formularios_sincronizados_incompletos_exitosos;
drop table if exists temp_relacion_predios_geometrias;
drop table if exists temp_centroide_formularios_con_geometria_sin_ui;
drop table if exists temp_interseccion_formularios_ui;
drop table if exists temp_formularios_con_ui;
drop table if exists temp_centroide_formularios_sin_geometria_sin_ui;

-- Físicas
drop table if exists seguimiento_sincronizacion_formularios_lote_6;
drop table if exists relacion_sincronizacion_analisis_juridico_gabinete_lote_6;

-- (a) Se crea la tabla temporal asociada a los datos capturados en campo
create temp table temp_formularios_sincronizados_incompletos_exitosos as
(
	select dblinkmaphurricane.identificacion_predio_pre_t_id predio_t_id_precarga
	      ,identificacion_predio_numero_predial numero_predial_formulario
		  --,identificacion_predio_numero_predial_precarga
	      ,identificacion_predio_hay_precarga_predio hay_precarga_predio
		  ,identificacion_predio_hay_precarga_terreno hay_precarga_terreno
	      ,identificacion_predio_hay_precarga_construccion hay_precarga_construccion
		  ,identificacion_predio_hay_precarga_interesados hay_precarga_interesado
		  ,(case when se_puede_realizar_encuesta = '1' then 'Si'
		  		 when se_puede_realizar_encuesta = '0' then 'No'
		  		 else se_puede_realizar_encuesta
		  		 end) se_pudo_realizar_formulario
		  --,meta_instanceid
		  --,meta_instancename
		  --,"key"
		  ,substring(submittername, 1, 7) tableta_sincronizacion
		  --,status
		  --,lc_resultadovisitatipo
		  ,dblinklcresultadovisitatipo.ilicode resultado_visita
		  --,identificacion_predio_usando
	from dblink_maphurricane dblinkmaphurricane
	inner join dblink_lc_resultadovisitatipo dblinklcresultadovisitatipo on cast(dblinkmaphurricane.lc_resultadovisitatipo as int4) = dblinklcresultadovisitatipo.itfcode
	where substring(dblinkmaphurricane.identificacion_predio_numero_predial, 1, 5) in ('20250', '70713', '70823') and se_puede_realizar_encuesta = '1' and (dblinkmaphurricane.lc_resultadovisitatipo in ('0', '1'))
);

-- (a.1) Formularios (Exitosos e Incompletos) con UI
create temp table temp_formularios_con_ui as
(
	select distinct tempformulariossincronizadosincompletosexitosos.predio_t_id_precarga
		,tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
		,null numero_predial_anterior		
	    ,tempformulariossincronizadosincompletosexitosos.hay_precarga_predio
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_terreno
	    ,tempformulariossincronizadosincompletosexitosos.hay_precarga_construccion
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_interesado		  
		,tempformulariossincronizadosincompletosexitosos.se_pudo_realizar_formulario
		,tempformulariossincronizadosincompletosexitosos.tableta_sincronizacion
		,tempformulariossincronizadosincompletosexitosos.resultado_visita
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
	from temp_formularios_sincronizados_incompletos_exitosos tempformulariossincronizadosincompletosexitosos
	left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on terrenosdigitalizadoslote6.predio_numero_predial = tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
	where terrenosdigitalizadoslote6.codigo_unidad_intervencion is not null
);

-- (a.2) Formularios (Exitosos e Incompletos) sin UI
-- En especial son predios nuevos o segregados nacidos de la operación misma de campo

/*
1. Primero se busca verificar la existencia de la geometría de esos predios nuevos y/o segregados.
Para ello, lo primero que se hace es relacionar la capa de predios de campo con la capa geométrica de terrenos de campo
*/
create temp table temp_relacion_predios_geometrias as
(
	select dblinkrelueprediocampo.id_terreno
		,dblinkrelueprediocampo.numero_predial
		,dblinkrelueprediocampo.numero_predial_anterior
		,dblinkueterrenocampo.geom
	from dblink_rel_uepredio_campo dblinkrelueprediocampo
	inner join colombiaseg_lote6.dblink_ue_terreno_campo dblinkueterrenocampo on dblinkrelueprediocampo.id_terreno = dblinkueterrenocampo.t_id
);

-- (a.2.1) 
-- Formularios (Exitosos e Incompletos) sin unidades de intervención más sí con geometría desde campo
-- Como tienen una geometría se les calcula su respectivo centroide
create temp table temp_centroide_formularios_con_geometria_sin_ui as
(
	select distinct tempformulariossincronizadosincompletosexitosos.predio_t_id_precarga
		,tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
		,temprelacionprediosgeometrias.numero_predial_anterior
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_predio
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_terreno
	    ,tempformulariossincronizadosincompletosexitosos.hay_precarga_construccion
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_interesado
		,tempformulariossincronizadosincompletosexitosos.se_pudo_realizar_formulario
		,tempformulariossincronizadosincompletosexitosos.tableta_sincronizacion
		,tempformulariossincronizadosincompletosexitosos.resultado_visita
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,ST_Centroid(temprelacionprediosgeometrias.geom) geom
	from temp_formularios_sincronizados_incompletos_exitosos tempformulariossincronizadosincompletosexitosos
	left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on terrenosdigitalizadoslote6.predio_numero_predial = tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
	left join temp_relacion_predios_geometrias temprelacionprediosgeometrias on tempformulariossincronizadosincompletosexitosos.numero_predial_formulario = temprelacionprediosgeometrias.numero_predial
	where terrenosdigitalizadoslote6.codigo_unidad_intervencion is null and temprelacionprediosgeometrias.geom is not null
);

-- Formularios (Exitosos e Incompletos) sin unidades de intervención y con geometría
-- Relizado el cálculo del centrooide se procede a realizar Intersección con UI
create temp table temp_interseccion_formularios_ui as
(
	select tempcentroideformularioscongeometriasinui.predio_t_id_precarga
			,tempcentroideformularioscongeometriasinui.numero_predial_formulario
			,tempcentroideformularioscongeometriasinui.numero_predial_anterior
		    ,tempcentroideformularioscongeometriasinui.hay_precarga_predio
		    ,tempcentroideformularioscongeometriasinui.hay_precarga_terreno
	        ,tempcentroideformularioscongeometriasinui.hay_precarga_construccion
		    ,tempcentroideformularioscongeometriasinui.hay_precarga_interesado
			,tempcentroideformularioscongeometriasinui.se_pudo_realizar_formulario
			,tempcentroideformularioscongeometriasinui.tableta_sincronizacion
			,tempcentroideformularioscongeometriasinui.resultado_visita
			,dblinkmhunidadintervencion.codigo codigo_unidad_intervencion
			,ST_Intersects(tempcentroideformularioscongeometriasinui.geom, dblinkmhunidadintervencion.geometria)
	from temp_centroide_formularios_con_geometria_sin_ui tempcentroideformularioscongeometriasinui, dblink_mh_unidadintervencion dblinkmhunidadintervencion
	where ST_Intersects(tempcentroideformularioscongeometriasinui.geom, dblinkmhunidadintervencion.geometria) is true
);

-- (a.2.2) Formularios (Exitosos e Incompletos) sin geometrías desde campo
create temp table temp_centroide_formularios_sin_geometria_sin_ui as
(
	select distinct tempformulariossincronizadosincompletosexitosos.predio_t_id_precarga
		,tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
		,temprelacionprediosgeometrias.numero_predial_anterior
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_predio
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_terreno
	    ,tempformulariossincronizadosincompletosexitosos.hay_precarga_construccion
		,tempformulariossincronizadosincompletosexitosos.hay_precarga_interesado
		,tempformulariossincronizadosincompletosexitosos.se_pudo_realizar_formulario
		,tempformulariossincronizadosincompletosexitosos.tableta_sincronizacion
		,tempformulariossincronizadosincompletosexitosos.resultado_visita
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,ST_Centroid(temprelacionprediosgeometrias.geom) geom
	from temp_formularios_sincronizados_incompletos_exitosos tempformulariossincronizadosincompletosexitosos
	left join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 on terrenosdigitalizadoslote6.predio_numero_predial = tempformulariossincronizadosincompletosexitosos.numero_predial_formulario
	left join temp_relacion_predios_geometrias temprelacionprediosgeometrias on tempformulariossincronizadosincompletosexitosos.numero_predial_formulario = temprelacionprediosgeometrias.numero_predial
	where terrenosdigitalizadoslote6.codigo_unidad_intervencion is null and temprelacionprediosgeometrias.geom is null
);

-- (a) Tabla resultante del proceso a, que inclute la referenciación de la UI por cada formulario sincronizado
-- Podría evolucionar incorporando datos de fecha de sincronización
-- Se debe verificar por calidad los formularios exitosos que NO tiene geometría
create table seguimiento_sincronizacion_formularios_lote_6 as
(
select distinct *
from
(
	(
		select predio_t_id_precarga
			,numero_predial_formulario
			,numero_predial_anterior			
			,hay_precarga_predio
		    ,hay_precarga_terreno
	        ,hay_precarga_construccion
		    ,hay_precarga_interesado
			,se_pudo_realizar_formulario
			,tableta_sincronizacion
			,resultado_visita
			,codigo_unidad_intervencion 
		from temp_formularios_con_ui
	)
	union
	(
		select predio_t_id_precarga
			,numero_predial_formulario
			,numero_predial_anterior
			,hay_precarga_predio
		    ,hay_precarga_terreno
	        ,hay_precarga_construccion
		    ,hay_precarga_interesado
			,se_pudo_realizar_formulario
			,tableta_sincronizacion
			,resultado_visita
			,codigo_unidad_intervencion
		from temp_interseccion_formularios_ui
	)
	union
	(
		select predio_t_id_precarga
			,numero_predial_formulario
			,numero_predial_anterior
			,hay_precarga_predio
		    ,hay_precarga_terreno
	        ,hay_precarga_construccion
		    ,hay_precarga_interesado
			,se_pudo_realizar_formulario
			,tableta_sincronizacion
			,resultado_visita
			,'Verificación Geográfica' codigo_unidad_intervencion
		from temp_centroide_formularios_sin_geometria_sin_ui
	)
) t1
);

-- (b) Comparación con información entregada por componente jurídico
create temp table temp_numeros_prediales_precarga as
(
	select numero_predial
		,numero_predial_anterior
		,fmi
		,count(*) conteo
	from colombiaseg_lote6.informacion_juridica_el_paso	
	group by 1, 2, 3
);

-- Se relaciona la información entregada (que tiene relación con precarga) con la ua predio centralizada por base de datos
create temp table temp_asignacion_predio_t_id as
(
	select uapredio.t_id predio_t_id
		   ,tempnumerospredialesprecarga.numero_predial
		   ,tempnumerospredialesprecarga.numero_predial_anterior
		   ,tempnumerospredialesprecarga.fmi fmi_analisis_juridico
	from temp_numeros_prediales_precarga tempnumerospredialesprecarga
	left join dblink_ua_predio uapredio on (uapredio.numero_predial = tempnumerospredialesprecarga.numero_predial)
);

-- (c) Verificación de información de campo frente a la precarga realizada por España

-- (d) Cruce entre procesos (a) y (b)
create table relacion_sincronizacion_analisis_juridico_gabinete_lote_6 as
(
	-- Se crea un incremental toda vez que se asignará a este campo la condición de primary key,
	-- dado que de no existir, dentro de QGIS los registros no se podrán editar.
	select row_number () over (order by seguimientosincronizacionformualarioslote6.predio_t_id_precarga) id 
		,(case when numero_predial_formulario like '%N%' then 'Nuevo\Segregado'
		else 'Precarga'
		end) tipo_predio
		,(case when substring(seguimientosincronizacionformualarioslote6.numero_predial_formulario, 1, 5) = '20250' then 'El Paso'
			when substring(seguimientosincronizacionformualarioslote6.numero_predial_formulario, 1, 5) = '70713' then 'San Onofre'
			when substring(seguimientosincronizacionformualarioslote6.numero_predial_formulario, 1, 5) = '70823' then 'Toluviejo'
			else 'Sin información'
			end) nombre_municipio
		,seguimientosincronizacionformualarioslote6.predio_t_id_precarga
		,seguimientosincronizacionformualarioslote6.numero_predial_formulario
		,seguimientosincronizacionformualarioslote6.numero_predial_anterior
		,seguimientosincronizacionformualarioslote6.hay_precarga_predio
		,seguimientosincronizacionformualarioslote6.hay_precarga_terreno
	    ,seguimientosincronizacionformualarioslote6.hay_precarga_construccion
		,seguimientosincronizacionformualarioslote6.hay_precarga_interesado
		,seguimientosincronizacionformualarioslote6.se_pudo_realizar_formulario
		,seguimientosincronizacionformualarioslote6.tableta_sincronizacion
		,seguimientosincronizacionformualarioslote6.resultado_visita
		,seguimientosincronizacionformualarioslote6.codigo_unidad_intervencion
		,(case when tempasignacionprediotid.fmi_analisis_juridico is not null then 'Cuenta Análisis Jurídico Gabinete'
		else 'Sin Análisis Jurídico Gabinete'
		end) estado_analisis_juridico_gabinete
		,cast(null as int2) asignacion
		,cast(null as int2) estado_revision
		,cast(null as date) fecha_verificacion
	from seguimiento_sincronizacion_formularios_lote_6 seguimientosincronizacionformualarioslote6
	left join temp_asignacion_predio_t_id tempasignacionprediotid on seguimientosincronizacionformualarioslote6.numero_predial_formulario = tempasignacionprediotid.numero_predial
);

-- Se altera la tabla anterior con el objetivo de asignar a una columna la cualidad de ser primary key
-- Para ello se altera la tabla, se le adiciona una constante y se le informa por código, cual columna será

alter table relacion_sincronizacion_analisis_juridico_gabinete_lote_6 add constraint pk_id primary key (id);

-- (e) QA
-- Verificación de la existencia de predios levantados al menos dos veces por diferente prediador
select numero_predial_formulario
 ,count(*)
from relacion_sincronizacion_analisis_juridico_gabinete_lote_6
group by 1
having count(*) > 1

/*
select *
from relacion_sincronizacion_analisis_juridico_gabinete_lote_6
where numero_predial_formulario = '2025020250005017N001N001'
*/



