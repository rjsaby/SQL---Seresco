drop table if exists terrenos_predio_w_id_interesado;
drop table if exists terrenos_predio_w_interesado_sin_estandarizar;
drop table if exists terrenos_predio_w_interesado_estandarizado;
drop table if exists preparacion_r1_r2;

-- Terrenos\Predios Vs Relación Interesados Predio
create temp table terrenos_predio_w_id_interesado as
(
	select terrenosdigitalizadoslote6.*
		,relprediointeresado.id_interesado 
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	inner join serladm.rel_prediointeresado  relprediointeresado
	on terrenosdigitalizadoslote6.predio_t_id = relprediointeresado.id_predio
);

-- Terrenos\Predios Vs Interesados
create temp table terrenos_predio_w_interesado_sin_estandarizar as
(
	select terrenosprediowidinteresado.predio_t_id
		,terrenosprediowidinteresado.terreno_t_id
		,intinteresado.t_id t_id_interesado
		,intinteresado.id_interesadotipo
		,intinteresado.id_interesadodocumentotipo
		,intinteresado.documento_identidad
		,intinteresado.primer_nombre
		,intinteresado.segundo_nombre
		,intinteresado.primer_apellido
		,intinteresado.segundo_apellido
		,intinteresado.id_sexotipo
		,intinteresado.id_grupoetnico
		,intinteresado.razon_social
		,intinteresado.nombre
		,intinteresado.grupo_etnico2
		,intinteresado.id_estadociviltipo
	from terrenos_predio_w_id_interesado terrenosprediowidinteresado
	inner join serladm.int_interesado intinteresado
	on terrenosprediowidinteresado.id_interesado = intinteresado.t_id
);

-- Parametrizacion de dominios
create temp table terrenos_predio_w_interesado_estandarizado as
(
select terrenosprediowinteresadosinestandarizar.predio_t_id
		,terrenosprediowinteresadosinestandarizar.terreno_t_id
		,terrenosprediowinteresadosinestandarizar.t_id_interesado
		,tcinteresadotipo.descripcion tipo_persona
		,tcinteresadodocumentotipo.descripcion tipo_documento
		,terrenosprediowinteresadosinestandarizar.documento_identidad
		,terrenosprediowinteresadosinestandarizar.primer_nombre
		,terrenosprediowinteresadosinestandarizar.segundo_nombre
		,terrenosprediowinteresadosinestandarizar.primer_apellido
		,terrenosprediowinteresadosinestandarizar.segundo_apellido
		,tcsexotipo.descripcion sexo
		,tcgrupoetnicotipo.descripcion grupo_etnico
		,terrenosprediowinteresadosinestandarizar.razon_social
		,terrenosprediowinteresadosinestandarizar.nombre
		,terrenosprediowinteresadosinestandarizar.grupo_etnico2
		,tcestadociviltipo.descripcion estado_civil
from terrenos_predio_w_interesado_sin_estandarizar terrenosprediowinteresadosinestandarizar
left join serladm.tc_interesadotipo tcinteresadotipo on terrenosprediowinteresadosinestandarizar.id_interesadotipo = tcinteresadotipo.t_id
left join serladm.tc_interesadodocumentotipo tcinteresadodocumentotipo on terrenosprediowinteresadosinestandarizar.id_interesadodocumentotipo = tcinteresadodocumentotipo.t_id
left join serladm.tc_sexotipo tcsexotipo on terrenosprediowinteresadosinestandarizar.id_sexotipo = tcsexotipo.t_id
left join serladm.tc_grupoetnicotipo tcgrupoetnicotipo on terrenosprediowinteresadosinestandarizar.id_grupoetnico = tcgrupoetnicotipo.t_id
left join serladm.tc_estadociviltipo tcestadociviltipo on terrenosprediowinteresadosinestandarizar.id_estadociviltipo = tcestadociviltipo.t_id
);


-- Consulta para exportar tabla de interesados, relacionados con terrenos y predios para todo Lote 6
drop table if exists interesados_lote_6;

create table interesados_lote_6 as
(
	select predio_t_id
			,terreno_t_id
			,t_id_interesado
			,tipo_persona
			,tipo_documento
			,documento_identidad
			,primer_nombre
			,segundo_nombre
			,primer_apellido
			,segundo_apellido
			,sexo
			,grupo_etnico
			,razon_social
			,nombre
			,grupo_etnico2
			,estado_civil
	from terrenos_predio_w_interesado_estandarizado
);

-- Creación de Tabla Registros 1 y 2 con preparación de NPN (El Paso)
create temp table preparacion_r1_r2 as
(
select concat('20250',r1elpaso.nopredial) numero_predial
,r2elpaso.matriculainmobiliaria 
,r1elpaso.estadocivil
,r1elpaso.tipodocumento
,r1elpaso.nodocumento
,r1elpaso.nombre
,r1elpaso.direccion
from public.r1_elpaso r1elpaso
inner join public.r2_elpaso r2elpaso on r1elpaso.nopredial = r2elpaso.nopredial 
);

-- Creación Tabla Física

drop table if exists r1_r2_elpaso;

create table r1_r2_elpaso as
(
select numero_predial
,matriculainmobiliaria 
,estadocivil
,tipodocumento
,nodocumento
,nombre
,direccion
from preparacion_r1_r2
);


