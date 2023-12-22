/*
 * Cruce de los listados de los censos con las tablas de interesados y agrupaciones de interesados de Maphurricane/serladm, 
 * para ver los coincidentes; supongo lo mejor sería cruzar por número de cedula, después por nombres y apellidos.
 * */

select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Espacolo35');

call colombiaseg_lote6.dblink_bd_maphurricane();

select *
from colombiaseg_lote6.censos_resguardos_indigenas_lote_6;

drop table if exists interesados_filtrados;
create temp table interesados_filtrados as
(
	select distinct t_id_interesado
		,tipo_persona
		,tipo_documento
		,documento_identidad
		,concat(lower(primer_nombre),' ', lower(segundo_nombre)) nombres
		,concat(lower(primer_apellido), ' ', lower(segundo_apellido)) apellidos
		,sexo
		,grupo_etnico
		,lower(nombre) nombre_completo
		,estado_civil
	from colombiaseg_lote6.interesados_lote_6
	where tipo_persona <> 'Persona_Juridica'
);

drop table if exists interesado_vs_censo;
create temp table interesado_vs_censo as
(
	select t1.t_id_interesado
			,t1.tipo_persona
			,t1.tipo_documento
			,t1.documento_identidad
			,t1.nombres
			,t1.apellidos
			,t1.sexo
			,t1.grupo_etnico
			,t1.nombre_completo
			,t1.estado_civil
			--,t2."TIPO IDENTIFICACION" censos_tipo_identificacion
			--,t2."NUMERO DOCUMENTO" censo_numero_documento
			--,t2."NOMBRES" censo_nombres
			--,t2."APELLIDOS" censo_apellidos
			,t2."SEXO" censo_sexo
			,t2."ESTADO CIVIL" censo_estado_civil
			,t2."COMUNIDAD INDIGENA" censo_comunidad_indigena
			,lower(t2."DIRECCION") censo_direccion
			,t2."TELEFONO" censo_telefono
	from (
			select distinct t_id_interesado
			,tipo_persona
			,tipo_documento
			,documento_identidad
			,concat(lower(primer_nombre),' ', lower(segundo_nombre)) nombres
			,concat(lower(primer_apellido), ' ', lower(segundo_apellido)) apellidos
			,sexo
			,grupo_etnico
			,lower(nombre) nombre_completo
			,estado_civil
		from colombiaseg_lote6.interesados_lote_6
		where tipo_persona <> 'Persona_Juridica'
		) t1
	inner join colombiaseg_lote6.censos_resguardos_indigenas_lote_6 t2 
	on t1.documento_identidad = t2."NUMERO DOCUMENTO"
);

select dblink_disconnect('conn1');