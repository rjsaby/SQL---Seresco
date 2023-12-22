--select dblink_disconnect('conn1');
select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Espacolo35');
call colombiaseg_lote6.dblink_bd_maphurricane();

drop table if exists temp_solicitud_original;
create temp table temp_solicitud_original as
(
	select fmi
		,nombre_municipio
		,null numero_predial
		,null numero_predial_anterior
		,null folio_matricula_inmobiliaria_derivado
		,null folio_matricula_inmobiliaria_matriz
		,null area_total_registral_m2
		,null snr_nombre_completo
		,null snr_tipo_documento
		,null snr_numero_documento
		,null tipo_documento_soporte
		--,null numero_documento_compra_solicitud
		,null estado_folio
		,null numero_documento
		,null fecha_documento
		,null ente_emisor
	-- **************** AUTOMATIZAR **************** 	
	from colombiaseg_lote6.fmi_sanonofre_sin_interrelacion
);

drop table if exists temp_anotaciones_parametrizadas;
create temp table temp_anotaciones_parametrizadas as
(
	select distinct folio_matricula
		,tipo_predio
		,tipo_documento tipo_documento_derecho
		,cast(concat_ws('-',substring(fecha_documento,7,4),substring(fecha_documento,4,2),substring(fecha_documento,1,2)) as date) fecha_documento  
		,ltrim(oficina_origen) oficina_origen
		,'Compraventa' naturaleza_juridica
		,(case when nro_identificacion like '%CC%' then 'Cedula_Ciudadania'
			when nro_identificacion like '%SE%' then 'Secuencial'
			when nro_identificacion like '%NIT%' then 'NIT'
			when nro_identificacion like '%PS%' then 'Pasaporte'
			when nro_identificacion like '%NT%' then 'NT'
			when nro_identificacion like '%NU%' then 'NU'
			when nro_identificacion like '%CE%' then 'Cedula_Extrangeria'
			when nro_identificacion like '%RC%' then 'Registro_Civil'
			when nro_identificacion like '%TI%' then 'Tarjeta_Identidad'
			else nro_identificacion
			end) tipo_documento
		,ltrim(replace(substring(nro_identificacion,6,10),'.','')) numero_documento
		,interviniente nombre_interesado
		,regexp_replace(numero_catrastral,'\s+', '') numero_catastral
		,numero_catastral_antiguo
		,((hectarea * 10000) +  metros) area_metros2
		,estado_folio
		,folio_derivado
	from colombiaseg_lote6.anotaciones_lote_6
	where municipio = 'EL PASO' and naturaleza_juridica like '%COMPRAVENTA%' and propietario = 'SI'
	order by 2
);

drop table if exists temp_anotaciones_normalizadas;
create temp table temp_anotaciones_normalizadas as
(
	select *
	from (
		select folio_matricula
				,tipo_predio
				,tipo_documento_derecho
				,fecha_documento  
				,oficina_origen
				,naturaleza_juridica
				,tipo_documento
				,numero_documento
				,nombre_interesado
				,numero_catastral
				,numero_catastral_antiguo
				,area_metros2
				,estado_folio
				,folio_derivado
				,row_number() over (partition by folio_matricula order by fecha_documento desc) as rango
		from temp_anotaciones_parametrizadas
		order by 1
	) t1
	where rango = 1
);

-- Cruce [Anotaciones]
drop table if exists tbl_analisis_solicitud_anotaciones_lote_6;
create table tbl_analisis_solicitud_anotaciones_lote_6 as
(
	select t1.fmi
		,t1.nombre_municipio	
		,t2.numero_catastral numero_predial
		,t2.numero_catastral_antiguo numero_predial_anterior
		--,t2.folio_matricula
		,t2.folio_derivado folio_matricula_inmobiliaria_derivado
		,t1.folio_matricula_inmobiliaria_matriz
		,t2.area_metros2 area_total_registral_m2
		,t2.nombre_interesado snr_nombre_completo
		,t2.tipo_documento snr_tipo_documento
		,t2.numero_documento snr_numero_documento
		,t2.tipo_documento_derecho tipo_documento_soporte
		--,t1.numero_documento_compra_solicitud
		,t2.estado_folio
		,t1.numero_documento
		,t2.fecha_documento
		,t2.oficina_origen ente_emisor
		,t2.naturaleza_juridica	
		,t2.tipo_predio			
	from temp_solicitud_original t1
	inner join temp_anotaciones_normalizadas t2
	on t1.fmi = t2.folio_matricula
);

-- NO Cruce [Anotaciones]
drop table if exists temp_folios_sin_cruce_con_anotaciones;
create temp table temp_folios_sin_cruce_con_anotaciones as
(
	select t1.fmi
		,t1.nombre_municipio	
		,t2.numero_catastral numero_predial
		,t2.numero_catastral_antiguo numero_predial_anterior
		--,t2.folio_matricula
		,t2.folio_derivado folio_matricula_inmobiliaria_derivado
		,t1.folio_matricula_inmobiliaria_matriz
		,t2.area_metros2 area_total_registral_m2
		,t2.nombre_interesado snr_nombre_completo
		,t2.tipo_documento snr_tipo_documento
		,t2.numero_documento snr_numero_documento
		,t2.tipo_documento_derecho tipo_documento_soporte
		--,t1.numero_documento_compra_solicitud
		,t2.estado_folio
		,t1.numero_documento
		,t2.fecha_documento
		,t2.oficina_origen ente_emisor
		,t2.naturaleza_juridica	
		,t2.tipo_predio			
	from temp_solicitud_original t1
	left join temp_anotaciones_normalizadas t2 on t1.fmi = t2.folio_matricula
	where t2.folio_matricula is null
);

drop table if exists temp_snr_derecho;
create temp table temp_snr_derecho as
(
	select derecho.t_id derecho_t_id
	,calidad.dispname calidad_derecho_registro
	,fuentet.dispname tipo_documento
	,fuented.numero_documento
	,fuented.fecha_documento
	,fuented.ente_emisor
	,derecho.snr_predio_registro
	from dblink_snr_derecho derecho
	inner join dblink_snr_fuentederecho fuented on derecho.snr_fuente_derecho = fuented.t_id
	inner join dblink_snr_calidadderechotipo calidad on derecho.calidad_derecho_registro = calidad.t_id
	inner join dblink_snr_fuentetipo fuentet on fuentet.t_id = fuented.tipo_documento
);

drop table if exists temp_snr_titular;
create temp table temp_snr_titular as
(
select t1.titular_t_id
,t1.tipo_persona
,t1.tipo_documento
,t1.numero_documento
,(case when t1.nombre_completo = '' then t1.razon_social
		else t1.nombre_completo
		end) nombre_completo
from (
		select titular.t_id titular_t_id
		,pertitu.dispname tipo_persona
		,docutitu.dispname tipo_documento
		,numero_documento
		,concat_ws(' ', nombres, primer_apellido, segundo_apellido) nombre_completo
		,razon_social
		from dblink_snr_titular titular
		inner join dblink_snr_personatitulartipo pertitu on titular.tipo_persona = pertitu.t_id
		inner join dblink_snr_documentotitulartipo docutitu on titular.tipo_documento = docutitu.t_id
		) t1
);

drop table if exists temp_derecho_titular;
create temp table temp_derecho_titular as
(
	select titular.titular_t_id
	,derecho.derecho_t_id
	,titular.tipo_persona
	,titular.tipo_documento tipo_documento_derecho
	,titular.numero_documento numero_documento_derecho
	,titular.nombre_completo
	,derecho.calidad_derecho_registro
	,derecho.tipo_documento tipo_documento_interesado
	,derecho.numero_documento numero_documento_interesado
	,derecho.fecha_documento
	,derecho.ente_emisor
	,derecho.snr_predio_registro	
	from dblink_snr_titular_derecho rel 
	inner join temp_snr_derecho derecho on derecho.derecho_t_id = rel.snr_derecho
	inner join temp_snr_titular titular on titular_t_id = rel.snr_titular
);


drop table if exists temp_snr_predio_registro;
create temp table temp_snr_predio_registro as
(
	select concat_ws('-',predio.codigo_orip, predio.matricula_inmobiliaria) folio_matricula_inmobiliaria
		,predio.numero_predial_nuevo_en_fmi
		,predio.numero_predial_anterior_en_fmi
		,clase.dispname clase_suelo
		,derecho.titular_t_id
		,derecho.derecho_t_id
		,derecho.tipo_persona
		,derecho.tipo_documento_derecho
		,derecho.numero_documento_derecho
		,derecho.nombre_completo
		,derecho.calidad_derecho_registro
		,derecho.tipo_documento_interesado
		,derecho.numero_documento_interesado
		,derecho.fecha_documento
		,derecho.ente_emisor
		,derecho.snr_predio_registro
		from dblink_snr_predioregistro predio
	inner join dblink_snr_clasepredioregistrotipo clase on predio.clase_suelo_registro = clase.t_id
	inner join temp_derecho_titular derecho on predio.t_id = derecho.snr_predio_registro
);

drop table if exists tbl_analisis_solicitud_snr_lote_6;
create temp table tbl_analisis_solicitud_snr_lote_6 as
(
	select distinct t1.folio_matricula_inmobiliaria fmi
			-- **************** AUTOMATIZAR ****************
			,'San Onofre' nombre_municipio
			,t1.numero_predial_nuevo_en_fmi numero_predial
			,t1.numero_predial_anterior_en_fmi numero_predial_anterior
			,null folio_matricula_inmobiliaria_derivado
			,null folio_matricula_inmobiliaria_matriz
			,null area_total_registral_m2
			,t1.nombre_completo snr_nombre_completo
			,t1.tipo_documento_interesado tipo_documento_soporte  
			,t1.numero_documento_interesado snr_numero_documento
			,t1.tipo_documento_derecho snr_tipo_documento
			,null estado_folio
			,t1.numero_documento_derecho numero_documento		
			,t1.fecha_documento fecha_documento
			,t1.ente_emisor
			,null naturaleza_juridica
			,t1.clase_suelo tipo_predio
	from temp_snr_predio_registro t1
	inner join temp_folios_sin_cruce_con_anotaciones t2 on t1.folio_matricula_inmobiliaria = t2.fmi
);

drop table if exists tbl_solicitud_vs_fuentes_lote_6;
create table tbl_solicitud_vs_fuentes_lote_6 as
(
	select distinct *
	from
	(
		select fmi
			,nombre_municipio	
			,numero_predial
			,numero_predial_anterior
			,folio_matricula_inmobiliaria_derivado
			,folio_matricula_inmobiliaria_matriz
			,area_total_registral_m2
			,snr_nombre_completo
			,snr_tipo_documento
			,snr_numero_documento
			,tipo_documento_soporte
			,estado_folio
			,numero_documento
			,fecha_documento
			,ente_emisor
			,naturaleza_juridica	
			,tipo_predio	
		from tbl_analisis_solicitud_anotaciones_lote_6
	union
		select fmi
			,nombre_municipio
			,numero_predial
			,numero_predial_anterior
			,folio_matricula_inmobiliaria_derivado
			,folio_matricula_inmobiliaria_matriz
			,cast(area_total_registral_m2 as float) area_total_registral_m2
			,snr_nombre_completo
			,snr_tipo_documento
			,numero_documento snr_numero_documento			
			,tipo_documento_soporte
			,estado_folio		
			,snr_numero_documento numero_documento
			,fecha_documento
			,ente_emisor
			,naturaleza_juridica
			,tipo_predio
		from tbl_analisis_solicitud_snr_lote_6
	) t1
);

select dblink_disconnect('conn1');

