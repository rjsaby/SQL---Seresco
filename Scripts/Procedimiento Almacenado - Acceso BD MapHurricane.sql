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

create or replace procedure dblink_bd_maphurricane()
language plpgsql
as $$
begin
	
	-- Conexi�n DBLINK
	-- perform dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');
	-- select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');
	
	--dblink
	drop table if exists dblink_rel_uepredio;
	drop table if exists dblink_ua_predio;
	drop table if exists dblink_ue_terreno;
	drop table if exists dblink_ue_construccion;
	drop table if exists dblink_ue_terreno;
	drop table if exists dblink_rel_unidadintervencion_predio;
	drop table if exists dblink_mh_unidadintervencion;
	drop table if exists dblink_mh_unidadintervencionestado;
	drop table if exists dblink_mh_limite_administrativo;
	drop table if exists dblink_tc_clasesuelotipo;
	drop table if exists dblink_rrr_derecho;
	drop table if exists dblink_tc_derechotipo;
	drop table if exists dblink_tc_destinacioneconomicatipo;
	drop table if exists dblink_rel_ordenpredio;
	drop table if exists dblink_mh_orden_trabajo;
	drop table if exists dblink_mh_usuario;
	drop table if exists dblink_mh_formulario;
	drop table if exists dblink_mh_usuario_rol;
	drop table if exists dblink_tc_ordentipo;
	drop table if exists dblink_tc_condicionprediotipo;
	drop table if exists dblink_tc_estadoorden;
	drop table if exists dblink_maphurricane;
	drop table if exists dblink_rel_prediointeresado;
	drop table if exists dblink_int_interesado;
	drop table if exists dblink_int_interesado_campo;
	drop table if exists dblink_tc_interesadotipo;
	drop table if exists dblink_tc_interesadodocumentotipo;
	drop table if exists dblink_tc_sexotipo;
	drop table if exists dblink_tc_grupoetnicotipo;
	drop table if exists dblink_tc_estadociviltipo;
	drop table if exists dblink_mh_subidas_campo;
	drop table if exists dblink_tc_estadopredio;
	drop table if exists dblink_gc_prediocatastro;
	drop table if exists dblink_gc_terreno;
	drop table if exists dblink_gc_construccion;
	drop table if exists dblink_ue_construccion_campo;
	drop table if exists dblink_lc_resultadovisitatipo;
	drop table if exists dblink_rel_uepredio_campo;
	drop table if exists dblink_ue_terreno_campo;
	drop table if exists dblink_ue_unidad_construccion;
    drop table if exists dblink_ue_unidad_construccion_campo;
	drop table if exists dblink_tc_unidadconstrucciontipo;
	drop table if exists dblink_tc_usouconstipo;
	drop table if exists dblink_tc_construcciontipo;
	drop table if exists dblink_tc_dominioconstrucciontipo;
	drop table if exists dblink_tc_calificartipo;
	drop table if exists dblink_lc_relacionprediotipo;
	drop table if exists dblink_int_agrupacioninteresados;
	drop table if exists dblink_rrr_derecho_campo;
	drop table if exists dblink_uc_estructura_campo;
	drop table if exists dblink_objeto_construccion_tipo;
	drop table if exists dblink_estado_conservacion_tipo;


	-- ******************************** CONEXI�N REMOTA ********************************

	create temp table dblink_rel_uepredio as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_terreno
			,id_servidumbretransito
			,id_unidadconstruccion
			,id_construccion
			,id_predio
			,a_ultmod
			,f_ultmod 
		FROM serladm.rel_uepredio reluepredio') 
		AS 
		t(
		t_id bigint
		,id_terreno int8
		,id_servidumbretransito int8
		,id_unidadconstruccion int8
		,id_construccion int8
		,id_predio int8
		,a_ultmod varchar(15)
		,f_ultmod timestamp
		)
	);

	create temp table dblink_ua_predio as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,departamento
			,municipio
			,id_operacion
			,numero_predial
			,id_prediotipo
			,id_condicionpredio
			,fecha_captura
			,id_clasesuelotipo
			,id_direccion
			,id_estadopredio
			,a_ultmod
			,f_ultmod
			,id_categoriasuelotipo
			,id_destinacioneconomicatipo
			,avaluo_catastral
			,matricula_inmobiliaria
			,nombre
			,numero_predial_anterior
			,nupre
			,avaluo_comercial
		FROM serladm.ua_predio') 
		AS 
		t(
			t_id bigint
			,departamento varchar(2)
			,municipio varchar(3)
			,id_operacion varchar(30)
			,numero_predial varchar(30)
			,id_prediotipo int8
			,id_condicionpredio int8
			,fecha_captura timestamp
			,id_clasesuelotipo int8
			,id_direccion int8
			,id_estadopredio int8
			,a_ultmod varchar(50)
			,f_ultmod timestamp
			,id_categoriasuelotipo int8
			,id_destinacioneconomicatipo int8
			,avaluo_catastral float8
			,matricula_inmobiliaria varchar(80)
			,nombre varchar(255)
			,numero_predial_anterior varchar(20)
			,nupre varchar(11)
			,avaluo_comercial float8
		)
	);

	create temp table dblink_ue_terreno as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,area_terreno
			,geometria
			,id_dimensiontipo
			,id_relacionsuperficietipo
			,fecha_carga
			,local_id
			,id_direccion
			,a_ultmod
			,f_ultmod
			,avaluo_terreno
			,manzana_vereda_codigo
			,avaluo_comercial_terreno
		FROM serladm.ue_terreno') 
		AS 
		t(
			t_id bigint
			,area_terreno float8
			,geometria geometry(multipolygonz, 3857)
			,id_dimensiontipo int8
			,id_relacionsuperficietipo int8
			,fecha_carga timestamp
			,local_id varchar(255)
			,id_direccion int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
			,avaluo_terreno float8
			,manzana_vereda_codigo varchar(21)
			,avaluo_comercial_terreno float8
		)
	);	

	create temp table dblink_ue_construccion as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,identificador
			,id_tipoconstruccion
			,id_dominioconstrucciontipo
			,numero_pisos
			,numero_sotanos
			,anio_construccion
			,area_construccion
			,observaciones
			,dimension
			,id_relacionsuperficie
			,geometria
			,fecha_carga
			,id_direccion
			,a_ultmod
			,f_ultmod
			,avaluo_construccion
			,numero_mezanines
			,numero_semisotanos
			,altura
			,avaluo_comercial_construccion
			,valor_referencia_construccion
		FROM serladm.ue_construccion') 
		AS 
		t(
			t_id bigint
			,identificador varchar(2)
			,id_tipoconstruccion int8
			,id_dominioconstrucciontipo int8
			,numero_pisos int4
			,numero_sotanos int4
			,anio_construccion int4
			,area_construccion float8
			,observaciones text
			,dimension int8
			,id_relacionsuperficie int8
			,geometria geometry(multipolygonz, 3857)
			,fecha_carga timestamp
			,id_direccion int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
			,avaluo_construccion float8
			,numero_mezanines int4
			,numero_semisotanos int4
			,altura numeric(6,2)
			,avaluo_comercial_construccion float8
			,valor_referencia_construccion numeric(16,1)
		)
	);

	create temp table dblink_ue_unidad_construccion as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,identificador
			,tipo_dominio
			,tipo_construccion
			,tipo_unidad_construccion
			,tipo_planta
			,planta_ubicacion
			,uso
			,area_construida
			,area_privada_construida
			,altura
			,observaciones
			,id_construccion
			,dimension
			,relacion_superficie
			,geometria
			,fecha_carga
			,a_ultmod
			,f_ultmod
			,avaluo_unidad_construccion
			,anio_construccion
			,total_banios
			,total_habitaciones
			,total_locales
			,total_pisos
			,avaluo_comercial_unidad_construccion
		FROM serladm.ue_unidadconstruccion') 
		AS 
		t(
			t_id bigint
			,identificador varchar(3)
			,tipo_dominio int8
			,tipo_construccion int8
			,tipo_unidad_construccion int8
			,tipo_planta int8
			,planta_ubicacion int4
			,uso int8
			,area_construida float8
			,area_privada_construida float8
			,altura int4
			,observaciones text
			,id_construccion int8
			,dimension int8
			,relacion_superficie int8
			,geometria geometry(multipolygonz, 3857)
			,fecha_carga timestamp
			,a_ultmod varchar(15)
			,f_ultmod timestamp
			,avaluo_unidad_construccion float8
			,anio_construccion int4
			,total_banios int4
			,total_habitaciones int4
			,total_locales int4
			,total_pisos int4
			,avaluo_comercial_unidad_construccion float8
		)
	);

	create temp table dblink_rel_unidadintervencion_predio as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_unidadintervencion
			,id_predio
			,a_ultmod
			,f_ultmod
		FROM serladm.rel_unidadintervencion_predio') 
		AS 
		t(
			t_id bigint
			,id_unidadintervencion int8
			,id_predio int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_mh_unidadintervencion as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,geometria
			,id_limite_administrativo
			,codigo
			,descripcion
			,id_estado
			,f_ultestado
			,a_ultmod
			,f_ultmod
		FROM serladm.mh_unidadintervencion') 
		AS 
		t(
			t_id bigint
			,geometria geometry(multipolygonz, 3857)
			,id_limite_administrativo int8
			,codigo varchar(10)
			,descripcion varchar(255)
			,id_estado int8
			,f_ultestado timestamp
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_mh_unidadintervencionestado as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.mh_unidadintervencionestado') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_mh_limite_administrativo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_tipolimiteadministrativo
			,codigo
			,nombre
			,a_ultmod
			,f_ultmod
			,geom
		FROM serladm.mh_limite_administrativo') 
		AS 
		t(
			t_id int8
			,id_tipolimiteadministrativo int8
			,codigo varchar(5)
			,nombre varchar(50)
			,a_ultmod varchar(15)
			,f_ultmod timestamp
			,geom geometry(multipolygon, 3857)
		)
	);

	create temp table dblink_tc_clasesuelotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_clasesuelotipo') 
		AS 
		t(
			t_id int8
			,codigo int8 
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_rel_prediointeresado as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_interesado
			,id_agrupacioninteresado
			,id_predio
			,a_ultmod
			,f_ultmod
		FROM serladm.rel_prediointeresado') 
		AS 
		t(
			t_id bigint
			,id_interesado int8
			,id_agrupacioninteresado int8
			,id_predio int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_int_interesado as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_interesadotipo
			,id_interesadodocumentotipo
			,documento_identidad
			,primer_nombre
			,segundo_nombre
			,primer_apellido
			,segundo_apellido
			,id_sexotipo
			,id_grupoetnico
			,razon_social
			,nombre
			,autoriza_notificacion_correo
			,id_direccion
			,a_ultmod
			,f_ultmod
			,grupo_etnico2
			,id_estadociviltipo
		FROM serladm.int_interesado') 
		AS 
		t(
			t_id bigint
			,id_interesadotipo int8
			,id_interesadodocumentotipo int8
			,documento_identidad varchar(50)
			,primer_nombre varchar(100)
			,segundo_nombre varchar(100)
			,primer_apellido varchar(100)
			,segundo_apellido varchar(100)
			,id_sexotipo int8
			,id_grupoetnico int8
			,razon_social varchar(255)
			,nombre varchar(255)
			,autoriza_notificacion_correo bool
			,id_direccion int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
			,grupo_etnico2 int8
			,id_estadociviltipo int8
		)
	);

	create temp table dblink_tc_interesadotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_interesadotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_interesadodocumentotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_interesadodocumentotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_sexotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_sexotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_grupoetnicotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_grupoetnicotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_estadociviltipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_estadociviltipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_rrr_derecho as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,tipo
			,fraccion_derecho
			,fecha_inicio_tenencia
			,descripcion
			,id_interesado
			,id_agrupacioninteresados
			,id_predio
			,a_ultmod
			,f_ultmod
		FROM serladm.rrr_derecho') 
		AS 
		t(
			t_id bigint
			,tipo int8
			,fraccion_derecho numeric(11,10)
			,fecha_inicio_tenencia timestamp(0)
			,descripcion varchar(255)
			,id_interesado int8
			,id_agrupacioninteresados int8
			,id_predio int8
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_derechotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_derechotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_destinacioneconomicatipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_destinacioneconomicatipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_rel_ordenpredio as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_ordentrabajo
			,id_predio
			,a_ultmod
			,f_ultmod
		FROM serladm.rel_ordenpredio') 
		AS 
		t(
			t_id bigint
			,id_ordentrabajo int8
			,id_predio int8
			,a_ultmod varchar(100)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_mh_orden_trabajo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,orden_tipo
			,estado_orden
			,codigo_orden
			,a_ultmod
			,f_ultmod
			,fecha_envio
			,id_zona
			,id_formulario
			,descripcion
			,usuario_asignado
			,usuario_supervisor
		FROM serladm.mh_orden_trabajo') 
		AS 
		t(
			t_id int8
			,orden_tipo int8
			,estado_orden int8
			,codigo_orden varchar(255)
			,a_ultmod varchar(100)
			,f_ultmod timestamp
			,fecha_envio timestamp
			,id_zona int8
			,id_formulario int8
			,descripcion text
			,usuario_asignado int8
			,usuario_supervisor int8
		)
	);

	create temp table dblink_mh_usuario as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,email
			,"password"
			,id_rol
			,primer_nombre
			,segundo_nombre
			,primer_apellido
			,segundo_apellido
			,tipo_documento
			,numero_documento
			,a_ultmod
			,f_ultmod
			,inactivo
			,token_odk
			,id_odk
		FROM serladm.mh_usuario') 
		AS 
		t(
			t_id int8
			,email varchar(100)
			,"password" varchar(100)
			,id_rol int8
			,primer_nombre varchar(100)
			,segundo_nombre varchar(100)
			,primer_apellido varchar(100)
			,segundo_apellido varchar(100)
			,tipo_documento int8
			,numero_documento varchar(50)
			,a_ultmod varchar(100)
			,f_ultmod timestamp
			,inactivo bool
			,token_odk varchar(500)
			,id_odk int8
		)
	);

	create temp table dblink_mh_formulario as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,fecha_actualizacion
			,fecha_creacion
			,nombre
		FROM serladm.mh_formulario') 
		AS 
		t(
			t_id int8
			,fecha_actualizacion timestamp
			,fecha_creacion timestamp
			,nombre varchar(255)
		)
	);

	create temp table dblink_mh_usuario_rol as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactivo
			,a_ultmod
			,f_ultmod
		FROM serladm.mh_usuario_rol') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(100)
			,inactivo bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_ordentipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,a_ultmod
			,f_ultmod
			,inactive
		FROM serladm.tc_ordentipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,a_ultmod varchar(16)
			,f_ultmod timestamp
			,inactive bool
		)
	);

	create temp table dblink_tc_condicionprediotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_condicionprediotipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_tc_estadoorden as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_estadoorden') 
		AS 
		t(
			t_id bigint
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_mh_subidas_campo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,nombre
			,fecha
			,autor
			,id_orden
			,cargado
		FROM serladm.mh_subidas_campo') 
		AS 
		t(
			t_id int8
			,nombre varchar(100)
			,fecha timestamp
			,autor varchar(50)
			,id_orden int8
			,cargado bool
		)
	);

	create temp table dblink_maphurricane as
	(
	select * from dblink('conn1', 
		'SELECT identificacion_predio_numero_predial
			,identificacion_predio_numero_predial_precarga
			,identificacion_predio_hay_precarga_predio
			,identificacion_predio_hay_precarga_terreno
			,identificacion_predio_hay_precarga_construccion
			,identificacion_predio_hay_precarga_interesados
			,se_puede_realizar_encuesta
			,meta_instanceid
			,meta_instancename
			,"key"
			,submitterid
			,submittername
			,status
			,lc_resultadovisitatipo
			,identificacion_predio_usando
			,identificacion_predio_pre_t_id
			,submissiondate
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta13
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta17
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta21
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta25
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta29
			,grupo_encuesta_grupo_sec_iii_interesados_total_interesados
			,grupo_encuesta_grupo_sec_iii_interesados_int_agrupacionintere00
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_preguntas_
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta30
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta31
			,grupo_encuesta_grupo_sec_iii_interesados_int_agrupacioninteresa
		FROM serladmcampo."MAPHURRICANE"') 
		AS 
		t(
			identificacion_predio_numero_predial varchar(200)
			,identificacion_predio_numero_predial_precarga varchar(200)
			,identificacion_predio_hay_precarga_predio varchar(200)
			,identificacion_predio_hay_precarga_terreno varchar(200)
			,identificacion_predio_hay_precarga_construccion varchar(200)
			,identificacion_predio_hay_precarga_interesados varchar(200)
			,se_puede_realizar_encuesta varchar(200)
			,meta_instanceid varchar(200)
			,meta_instancename varchar(200)
			,"key" varchar(200)
			,submitterid varchar(200)
			,submittername varchar(200)
			,status varchar(200)
			,lc_resultadovisitatipo varchar(250)
			,identificacion_predio_usando varchar(200)
			,identificacion_predio_pre_t_id varchar(200)
			,submissiondate varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta13 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta17 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta21 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta25 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta29 varchar(200)
			,grupo_encuesta_grupo_sec_iii_interesados_total_interesados varchar(200)
			,grupo_encuesta_grupo_sec_iii_interesados_int_agrupacionintere00 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_preguntas_ varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta30 varchar(200)
			,grupo_encuesta_grupo_sec_ii_contacto_visita_contacto_pregunta31 varchar(200)
			,grupo_encuesta_grupo_sec_iii_interesados_int_agrupacioninteresa varchar(200)
		)
	);

	create temp table dblink_tc_estadopredio as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactivo
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_estadopredio') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactivo bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_gc_prediocatastro as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,t_ili_tid
			,tipo_catastro
			,numero_predial
			,numero_predial_anterior
			,nupre
			,circulo_registral
			,matricula_inmobiliaria_catastro
			,tipo_predio
			,condicion_predio
			,destinacion_economica
			,sistema_procedencia_datos
			,fecha_datos
		FROM serladm.gc_prediocatastro') 
		AS 
		t(
			t_id bigint
			,t_ili_tid varchar(200)
			,tipo_catastro varchar(255)
			,numero_predial varchar(30)
			,numero_predial_anterior varchar(20)
			,nupre varchar(11)
			,circulo_registral varchar(4)
			,matricula_inmobiliaria_catastro varchar(80)
			,tipo_predio varchar(100)
			,condicion_predio int8
			,destinacion_economica varchar(150)
			,sistema_procedencia_datos int8
			,fecha_datos date
		)
	);

	create temp table dblink_gc_terreno as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,t_ili_tid
			,area_terreno_alfanumerica
			,area_terreno_digital
			,manzana_vereda_codigo
			,numero_subterraneos
			,geometria
			,gc_predio
		FROM serladm.gc_terreno') 
		AS 
		t(
			t_id bigint
			,t_ili_tid varchar(200)
			,area_terreno_alfanumerica numeric(16,2)
			,area_terreno_digital numeric(16,2)
			,manzana_vereda_codigo varchar(17)
			,numero_subterraneos int4
			,geometria geometry(multipolygonz, 3857)
			,gc_predio int8
		)
	);

	create temp table dblink_gc_construccion as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,t_ili_tid
			,identificador
			,etiqueta
			,tipo_construccion
			,tipo_dominio
			,numero_pisos
			,numero_sotanos
			,numero_mezanines
			,numero_semisotanos
			,codigo_edificacion
			,codigo_terreno
			,area_construida
			,geometria
			,gc_predio
		FROM serladm.gc_construccion') 
		AS 
		t(
			t_id bigint
			,t_ili_tid varchar(200)
			,identificador varchar(30)
			,etiqueta varchar(50)
			,tipo_construccion int8
			,tipo_dominio varchar(20)
			,numero_pisos int4
			,numero_sotanos int4  
			,numero_mezanines int4
			,numero_semisotanos int4
			,codigo_edificacion int4
			,codigo_terreno varchar(30)
			,area_construida numeric(16,2)
			,geometria geometry(multipolygonz, 3857)
			,gc_predio int8
		)
	);

	create temp table dblink_lc_resultadovisitatipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,thisclass
			,baseclass
			,itfcode
			,ilicode
			,seq
			,inactive
			,dispname
			,description
		FROM serladm.lc_resultadovisitatipo') 
		AS 
		t(
			t_id int8
			,thisclass varchar(1024)
			,baseclass varchar(1024) 
			,itfcode int4
			,ilicode varchar(1024) 
			,seq int4
			,inactive bool
			,dispname varchar(250)
			,description varchar(1024)
		)
	);

	create temp table dblink_rel_uepredio_campo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_construccion
			,id_unidadconstruccion
			,id_terreno
			,numero_predial
			,numero_predial_anterior
			,accion_novedad
			,instance_id
			,enviado
			,fecha_mod
			,id
			,t_id_vuelta
		FROM serladmcampo.rel_uepredio') 
		AS 
		t(
			t_id bigint
			,id_construccion varchar(200)
			,id_unidadconstruccion varchar(200)
			,id_terreno varchar(200)
			,numero_predial varchar(200)
			,numero_predial_anterior varchar(200)
			,accion_novedad varchar(200)
			,instance_id varchar(200)
			,enviado bool
			,fecha_mod date
			,id int8
			,t_id_vuelta varchar
		)
	);

	create table dblink_ue_terreno_campo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,local_id
			,etiqueta
			,fecha_edicion
			,fecha_alta
			,fid
			,geom
		FROM serladmcampo.ue_terreno
		WHERE substring(local_id,1,5) in (''20250'',''70713'',''70823'')') 
		AS 
		t(
			t_id varchar(200)
			,local_id varchar(200)
			,etiqueta varchar(200)
			,fecha_edicion varchar(200)
			,fecha_alta varchar(200)
			,fid varchar(200)
			,geom geometry(multipolygon, 3857)
		)
	);

	create table dblink_tc_unidadconstrucciontipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_unidadconstrucciontipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);
	
	create table dblink_tc_usouconstipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_usouconstipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create table dblink_tc_construcciontipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_construcciontipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);


	create temp table dblink_ue_construccion_campo as
	(
	select * from dblink('conn1', 
		'SELECT numero_predial_18
			,construccion_general_man_ue_construccion_id_tipoconstruccion
			,construccion_general_ue_construccion_id_dominioconstrucciontipo
			,construccion_general_pre_ue_construccion_numero_pisos
			,construccion_general_corr_pre_ue_construccion_numero_pisos
			,construccion_general_ue_construccion_anio_construccion
			,parent_key
			,"key"
		FROM serladmcampo."MAPHURRICANE-ue_construccion"') 
		AS 
		t(
			numero_predial_18 varchar(200)
			,construccion_general_man_ue_construccion_id_tipoconstruccion varchar(200)
			,construccion_general_ue_construccion_id_dominioconstrucciontipo varchar(200)
			,construccion_general_pre_ue_construccion_numero_pisos varchar(200)
			,construccion_general_corr_pre_ue_construccion_numero_pisos varchar(200)
			,construccion_general_ue_construccion_anio_construccion varchar(200)
			,parent_key varchar(200)
			,"key" varchar(200)
		)
	);

	create temp table dblink_tc_dominioconstrucciontipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_dominioconstrucciontipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_ue_unidad_construccion_campo as
	(
	select * from dblink('conn1', 
		'SELECT numero_predial_19 
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_uso
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_habitac
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_banios
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_locales
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_altura
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_anio_construc
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_area_privada_
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_observaciones
			,ue_calificacionconvencional_id_calificartipo
			,grupo_calificacion_convencional_ue_grupocalificacion_estructura
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados_u
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados00
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados01
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados02
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados03
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados04
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados05
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados06
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados07
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_o
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue00
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue01
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue02
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue03
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue04
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_g
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue05
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue06
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_ue_
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u00
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u01
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u02
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u03
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u04
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u05
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u06
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u07
			,grupo_calificacion_convencional_ue_grupocalificacion_complement
			,grupo_calificacion_convencional_ue_grupocalificacion_compleme00
			,grupo_calificacion_convencional_ue_grupocalificacion_compleme01
			,grupo_calificacion_convencional_total_calificacion_industrial
			,grupo_calificacion_convencional_total_calificacion_no_industria
			,grupo_calificacion_convencional_lc_calconve_total_calificacion
			,grupo_calificacion_no_convencional_lc_anexotipo_tipo_anexo
			,parent_key
			,"key" 
		FROM serladmcampo."MAPHURRICANE-ue_unidadconstruccion"') 
		AS 
		t(	numero_predial_19 varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_uso varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_habitac varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_banios varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_total_locales varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_altura varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_anio_construc varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_area_privada_ varchar(200)
			,ue_unidadconstruccion_datos_ue_unidadconstruccion_observaciones varchar(250)
			,ue_calificacionconvencional_id_calificartipo varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_estructura varchar(250)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados_u varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados00 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados01 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados02 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados03 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados04 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados05 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados06 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_acabados07 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_o varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue00 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue01 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue02 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue03 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue04 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue_g varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue05 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_banio_ue06 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_ue_ varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u00 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u01 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u02 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u03 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u04 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u05 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u06 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_cocina_u07 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_complement varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_compleme00 varchar(200)
			,grupo_calificacion_convencional_ue_grupocalificacion_compleme01 varchar(200)
			,grupo_calificacion_convencional_total_calificacion_industrial varchar(200)
			,grupo_calificacion_convencional_total_calificacion_no_industria varchar(200)
			,grupo_calificacion_convencional_lc_calconve_total_calificacion varchar(200)
			,grupo_calificacion_no_convencional_lc_anexotipo_tipo_anexo varchar(200)
			,parent_key varchar(200)
			,"key" varchar(200)
		)
	);

	create temp table dblink_tc_calificartipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_calificartipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		)
	);

	create temp table dblink_lc_relacionprediotipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,thisclass
			,baseclass
			,itfcode
			,ilicode
			,seq
			,inactive
			,dispname
			,description
		FROM serladm.lc_relacionprediotipo') 
		AS 
		t(
			t_id int8
			,thisclass varchar(1024)
			,baseclass varchar(1024) 
			,itfcode int4
			,ilicode varchar(1024) 
			,seq int4
			,inactive bool
			,dispname varchar(250)
			,description varchar(1024)
		)
	);

	create temp table dblink_int_agrupacioninteresados as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,id_grupointeresadotipo
			,nombre
			,comienzo_vida_util_version
			,a_ultmod
			,f_ultmod
		FROM serladm.int_agrupacioninteresados') 
		AS 
		t(
			t_id int8
			,id_grupointeresadotipo int8
			,nombre varchar(255)
			,comienzo_vida_util_version timestamp
			,a_ultmod varchar(15)
			,f_ultmod timestamp

		)
	);

	create temp table dblink_int_interesado_campo as
	(
	select * from dblink('conn1', 
		'SELECT int_interesado_datos_contacto_numero_predial_14
		,int_interesado_datos_contacto_int_interesado_documento_identida
		,int_interesado_datos_contacto_int_interesado_razon_social
		,int_interesado_datos_contacto_nombre
		,int_interesado_datos_contacto_int_interesado_id_grupoetnico
		,int_interesado_datos_contacto_int_interesado_id_sexotipo
		,int_interesado_datos_contacto_contactovisita_id_estadociviltipo
		,int_interesado_datos_contacto_int_interesado_id_interesadotipo
		,int_interesado_datos_contacto_int_interesado_id_interesadodocum
		,parent_key
		,"key"
		FROM serladmcampo."MAPHURRICANE-int_interesado"') 
		AS 
		t(
		int_interesado_datos_contacto_numero_predial_14 varchar(200)
		,int_interesado_datos_contacto_int_interesado_documento_identida varchar(200)
		,int_interesado_datos_contacto_int_interesado_razon_social varchar(200)
		,int_interesado_datos_contacto_nombre varchar(200)
		,int_interesado_datos_contacto_int_interesado_id_grupoetnico varchar(200)
		,int_interesado_datos_contacto_int_interesado_id_sexotipo varchar(200)
		,int_interesado_datos_contacto_contactovisita_id_estadociviltipo varchar(200)
		,int_interesado_datos_contacto_int_interesado_id_interesadotipo varchar(200)
		,int_interesado_datos_contacto_int_interesado_id_interesadodocum varchar(200)
		,parent_key varchar(200)
		,"key" varchar(200)
		)
	);

	create temp table dblink_rrr_derecho_campo as
	(
	select * from dblink('conn1', 
		'SELECT rrr_derecho_grupo_rrr_derecho_fraccion_derecho
			,rrr_derecho_grupo_rrr_derecho_fecha_inicio_tenencia
			,rrr_derecho_grupo_rrr_derecho_descripcion
			,rrr_derecho_grupo_rrr_derecho_tipo_valor_defecto
			,parent_key
			,"key"
		FROM serladmcampo."MAPHURRICANE-rrr_derecho"') 
		AS 
		t(
			rrr_derecho_grupo_rrr_derecho_fraccion_derecho varchar(200)
			,rrr_derecho_grupo_rrr_derecho_fecha_inicio_tenencia varchar(200)
			,rrr_derecho_grupo_rrr_derecho_descripcion varchar(200)
			,rrr_derecho_grupo_rrr_derecho_tipo_valor_defecto varchar(200)
			,parent_key varchar(200)
			,"key" varchar(200)
		) 
	);

	create temp table dblink_uc_estructura_campo as
	(
	select * from dblink('conn1', 
		'SELECT ue_objetoconstruccion_armazon_tipo
			,ue_objetoconstruccion_armazon_puntos
			,ue_objetoconstruccion_muros_tipo
			,ue_objetoconstruccion_muros_puntos
			,ue_objetoconstruccion_cubierta_tipo
			,ue_objetoconstruccion_cubierta_puntos
			,ue_grupocalificacion_estructura_conservacion
			,ue_grupocalificacion_estructura_conservacion_puntos
			,ue_grupocalificacion_estructura_subtotal
			,parent_key
			,"key"
		FROM serladmcampo."MAPHURRICANE-ue_grupocalificacion_estructura"') 
		AS 
		t(
		ue_objetoconstruccion_armazon_tipo varchar(200)
		,ue_objetoconstruccion_armazon_puntos varchar(200)
		,ue_objetoconstruccion_muros_tipo varchar(200)
		,ue_objetoconstruccion_muros_puntos varchar(200)
		,ue_objetoconstruccion_cubierta_tipo varchar(200)
		,ue_objetoconstruccion_cubierta_puntos varchar(200)
		,ue_grupocalificacion_estructura_conservacion varchar(200)
		,ue_grupocalificacion_estructura_conservacion_puntos varchar(200)
		,ue_grupocalificacion_estructura_subtotal varchar(200)
		,parent_key varchar(200)
		,"key" varchar(200)	
		) 
	);

	create temp table dblink_objeto_construccion_tipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_objetoconstrucciontipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		) 
	);

	create temp table dblink_estado_conservacion_tipo as
	(
	select * from dblink('conn1', 
		'SELECT t_id
			,codigo
			,descripcion
			,inactive
			,a_ultmod
			,f_ultmod
		FROM serladm.tc_estadoconservaciontipo') 
		AS 
		t(
			t_id int8
			,codigo int8
			,descripcion varchar(1024)
			,inactive bool
			,a_ultmod varchar(15)
			,f_ultmod timestamp
		  ) 
	);

end$$