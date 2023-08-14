drop table if exists tmp_fuente_derecho;
drop table if exists tmp_fuente_derecho_derecho;
drop table if exists tmp_derecho_calidad;
drop table if exists tmp_snr_titular;
drop table if exists tmp_snr_derecho_titular;
drop table if exists tmp_predio_registro_w_derecho;

-- Fuente Derecho Vs Tipo Documento
create temp table tmp_fuente_derecho as
(
	select fuentederecho.t_id fuentederecho_t_id
			,fuentederecho.t_ili_tid fuentederecho_t_ili_tid
			,fuentetipo.dispname tipo_documento
			,fuentederecho.numero_documento
			,fuentederecho.fecha_documento
			,fuentederecho.ente_emisor
			,fuentederecho.ciudad_emisora
	from ladmtoluviejo.snr_fuentederecho fuentederecho
	left join ladmtoluviejo.snr_fuentetipo fuentetipo
	on fuentederecho.tipo_documento = fuentetipo.t_id
);

-- Fuente Derecho Vs SNR Derecho
create temp table tmp_fuente_derecho_derecho as (
	select tmp_fuente_derecho.*
		,snrderecho.t_id snrderecho_t_id
		,snrderecho.t_ili_tid snrderecho_t_ili_tid
		,snrderecho.calidad_derecho_registro
		,snrderecho.codigo_naturaleza_juridica
		,snrderecho.snr_fuente_derecho
		,snrderecho.snr_predio_registro
	from ladmtoluviejo.snr_derecho snrderecho
	left join tmp_fuente_derecho tmp_fuente_derecho
	on snrderecho.snr_fuente_derecho = fuentederecho_t_id
);

-- SNR Derecho Vs Calidad Derecho
create temp table tmp_derecho_calidad as
(
	select tmp_fuente_derecho_derecho.*	
		   ,calidadderechotipo.dispname tipo_calidad_derecho
	from tmp_fuente_derecho_derecho tmp_fuente_derecho_derecho
	left join ladmtoluviejo.snr_calidadderechotipo calidadderechotipo
	on tmp_fuente_derecho_derecho.calidad_derecho_registro = calidadderechotipo.t_id 
);

-- SNR Titular Derecho
create temp table tmp_snr_titular as
(
	select snrtitular.t_id snrtitular_t_id
		,snrtitular.t_ili_tid
		,snrpersonatitulartipo.dispname tipo_persona
		,snrdocumentotitulartipo.dispname snrdocumentotitulartipo_tipo_documento
		,snrtitular.numero_documento snrtitular_numero_documento
		,snrtitular.nombres
		,snrtitular.primer_apellido
		,snrtitular.segundo_apellido
		,snrtitular.razon_social
	from ladmtoluviejo.snr_titular snrtitular
	inner join ladmtoluviejo.snr_personatitulartipo snrpersonatitulartipo on snrtitular.tipo_persona = snrpersonatitulartipo.t_id
	inner join ladmtoluviejo.snr_documentotitulartipo snrdocumentotitulartipo on snrtitular.tipo_documento = snrdocumentotitulartipo.t_id
);

-- SNR Derecho Vs SNR Titular
create temp table tmp_snr_derecho_titular as
(
	select snrtitularderecho.snr_titular
	,snrtitularderecho.snr_derecho
	,tmpderechocalidad.*
	,tmpsnrtitular.*
	from ladmtoluviejo.snr_titular_derecho snrtitularderecho
	-- Cruce con SNR Derecho (que trae ya la relación con Fuente Derecho)
	left join tmp_derecho_calidad tmpderechocalidad on snrtitularderecho.snr_derecho = tmpderechocalidad.snrderecho_t_id
	-- Cruce con Titular 
	left join tmp_snr_titular tmpsnrtitular on snrtitularderecho.snr_titular = tmpsnrtitular.snrtitular_t_id
);

-- SNR Derecho\Titular Vs SNR Predio Registro
create temp table tmp_predio_registro_w_derecho as
(
	select snrpredioregistro.t_id snrpredioregistro_t_id
		  ,snrpredioregistro.codigo_orip
		  ,snrpredioregistro.matricula_inmobiliaria
		  ,snrpredioregistro.numero_predial_nuevo_en_fmi
		  ,snrpredioregistro.numero_predial_anterior_en_fmi
		  ,snrpredioregistro.nomenclatura_registro
		  ,snrpredioregistro.cabida_linderos
		  ,snrpredioregistro.clase_suelo_registro
		  ,tmpsnrderechotitular.snr_titular
		  ,tmpsnrderechotitular.snr_derecho
		  ,tmpsnrderechotitular.fuentederecho_t_id
		  ,tmpsnrderechotitular.tipo_documento
		  ,tmpsnrderechotitular.numero_documento
		  ,tmpsnrderechotitular.fecha_documento
		  ,tmpsnrderechotitular.ente_emisor
		  ,tmpsnrderechotitular.ciudad_emisora
		  ,tmpsnrderechotitular.tipo_calidad_derecho
		  ,tmpsnrderechotitular.codigo_naturaleza_juridica
		  ,tmpsnrderechotitular.snr_predio_registro
		  ,tmpsnrderechotitular.tipo_persona
		  ,tmpsnrderechotitular.snrdocumentotitulartipo_tipo_documento titular_tipo_documento
		  ,tmpsnrderechotitular.snrtitular_numero_documento titular_numero_documento
		  ,tmpsnrderechotitular.nombres
		  ,tmpsnrderechotitular.primer_apellido
		  ,tmpsnrderechotitular.segundo_apellido
		  ,tmpsnrderechotitular.razon_social		  
	from tmp_snr_derecho_titular tmpsnrderechotitular 
	left join ladmtoluviejo.snr_predioregistro snrpredioregistro on tmpsnrderechotitular.snr_predio_registro = snrpredioregistro.t_id
);

drop table if exists snr_predio_unificado_ladmtoluviejo;

-- Resultado Final
create table snr_predio_unificado_ladmtoluviejo as
(
	select tmppredioregistrowderecho.snrpredioregistro_t_id snrpredioregistro_t_id
		  ,tmppredioregistrowderecho.codigo_orip
		  ,tmppredioregistrowderecho.matricula_inmobiliaria
		  ,tmppredioregistrowderecho.numero_predial_nuevo_en_fmi
		  ,tmppredioregistrowderecho.numero_predial_anterior_en_fmi
		  ,tmppredioregistrowderecho.nomenclatura_registro
		  ,tmppredioregistrowderecho.cabida_linderos
		  ,tmppredioregistrowderecho.clase_suelo_registro
		  ,clasepredioregistrotipo.dispname tipo_suelo_registro
		  ,tmppredioregistrowderecho.snr_titular
		  ,tmppredioregistrowderecho.snr_derecho
		  ,tmppredioregistrowderecho.fuentederecho_t_id
		  ,tmppredioregistrowderecho.tipo_documento
		  ,tmppredioregistrowderecho.numero_documento
		  ,tmppredioregistrowderecho.fecha_documento
		  ,tmppredioregistrowderecho.ente_emisor
		  ,tmppredioregistrowderecho.ciudad_emisora
		  ,tmppredioregistrowderecho.tipo_calidad_derecho
		  ,tmppredioregistrowderecho.codigo_naturaleza_juridica
		  ,tmppredioregistrowderecho.snr_predio_registro
		  ,tmppredioregistrowderecho.tipo_persona
		  ,tmppredioregistrowderecho.titular_tipo_documento
		  ,tmppredioregistrowderecho.titular_numero_documento
		  ,tmppredioregistrowderecho.nombres
		  ,tmppredioregistrowderecho.primer_apellido
		  ,tmppredioregistrowderecho.segundo_apellido
		  ,tmppredioregistrowderecho.razon_social		
	from tmp_predio_registro_w_derecho tmppredioregistrowderecho
	left join ladmtoluviejo.snr_clasepredioregistrotipo clasepredioregistrotipo
	on tmppredioregistrowderecho.clase_suelo_registro = clasepredioregistrotipo.t_id
);
