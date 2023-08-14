drop table if exists tipo_construccion;
drop table if exists dblink_ua_predio;
drop table if exists dblink_ue_terreno;
drop table if exists dblink_ue_construccion;
drop table if exists dblink_ue_unidad_construccion;
drop table if exists dblink_tc_usouconstipo;
drop table if exists dblink_tc_construcciontipo;
drop table if exists temp_unidades_construccion;
drop table if exists dblink_tc_unidadconstrucciontipo;
drop table if exists temp_construcciones_convencionales_no_visitadas_bpm;


select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');


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

	create temp table temp_unidades_construccion as
	(
		select reluepredio.id_predio t_id_predio
			,uapredio.numero_predial predio_numero_predial
			,reluepredio.id_terreno t_id_terreno
			,reluepredio.id_construccion t_id_construccion
			,reluepredio.id_unidadconstruccion t_id_unidadconstruccion
			,ueunidadconstruccion.identificador
			--,ueunidadconstruccion.tipo_construccion
			,tcconstrucciontipo.descripcion tipo_construccion
			--,ueunidadconstruccion.tipo_unidad_construccion
			,tcunidadconstrucciontipo.descripcion tipo_unidadconstruccion
			--,ueunidadconstruccion.uso
			,tcusouconstipo.descripcion tipo_uso
		from dblink_rel_uepredio reluepredio
		inner join dblink_ua_predio uapredio on uapredio.t_id = reluepredio.id_predio
		inner join dblink_ue_unidad_construccion ueunidadconstruccion on reluepredio.id_unidadconstruccion = ueunidadconstruccion.t_id
		inner join dblink_tc_unidadconstrucciontipo tcunidadconstrucciontipo on ueunidadconstruccion.tipo_unidad_construccion = tcunidadconstrucciontipo.t_id
		inner join dblink_tc_usouconstipo tcusouconstipo on ueunidadconstruccion.uso = tcusouconstipo.t_id
		inner join dblink_tc_construcciontipo tcconstrucciontipo on ueunidadconstruccion.tipo_construccion = tcconstrucciontipo.t_id
		where substring(uapredio.numero_predial, 1, 5) in ('20250','70713','70823')
	); 

	create table temp_construcciones_convencionales_no_visitadas_bpm as
	(
		select tempunidadesconstruccion.*
		from temp_unidades_construccion tempunidadesconstruccion
		left join colombiaseg_lote6.estado_enlace_formularios_predios_preoperativo_lote_6 estadoenlaceformulariosprediospreoperativolote6
		on (tempunidadesconstruccion.t_id_predio = estadoenlaceformulariosprediospreoperativolote6.precarga_predio_t_id) 
			or (tempunidadesconstruccion.predio_numero_predial = estadoenlaceformulariosprediospreoperativolote6.identificacion_predio_numero_predial)
		where estadoenlaceformulariosprediospreoperativolote6.resultado_visita is null and tempunidadesconstruccion.tipo_construccion = 'Convencional'
	);





