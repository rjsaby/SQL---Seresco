drop table if exists preparacion_r1_r2_elpaso;
drop table if exists preparacion_r1_r2_sanonofre;
drop table if exists preparacion_r1_r2_toluviejo;

-- Creación de Tabla Registros 1 y 2 con preparación de NPN (El Paso)
create temp table preparacion_r1_r2_elpaso as
(
	select distinct concat('20250',r1elpaso.nopredial) numero_predial
	,r2elpaso.matriculainmobiliaria 
	,r1elpaso.estadocivil
	,r1elpaso.tipodocumento
	,r1elpaso.nodocumento
	,r1elpaso.nombre
	,r1elpaso.direccion
	from public.r1_elpaso r1elpaso
	inner join public.r2_elpaso r2elpaso on r1elpaso.nopredial = r2elpaso.nopredial 
);

create temp table preparacion_r1_r2_sanonofre as
(
	select distinct concat('70713',r1sanonofre.nopredial) numero_predial
	,r2sanonofre.matriculainmobiliaria 
	,r1sanonofre.estadocivil
	,r1sanonofre.tipodocumento
	,r1sanonofre.nodocumento
	,r1sanonofre.nombre
	,r1sanonofre.direccion
	from public.r1_sanonofre r1sanonofre
	inner join public.r2_sanonofre r2sanonofre on r1sanonofre.nopredial = r2sanonofre.nopredial 
);

create temp table preparacion_r1_r2_toluviejo as
(
	select distinct concat('70823',r1toluviejo.nopredial) numero_predial
	,r2toluviejo.matriculainmobiliaria 
	,r1toluviejo.estadocivil
	,r1toluviejo.tipodocumento
	,r1toluviejo.nodocumento
	,r1toluviejo.nombre
	,r1toluviejo.direccion
	from public.r1_toluviejo r1toluviejo
	inner join public.r2_toluviejo r2toluviejo on r1toluviejo.nopredial = r2toluviejo.nopredial 
);

create table r1_r2_lote_6 as
(
	select *
	from preparacion_r1_r2_elpaso
	union
	select *
	from preparacion_r1_r2_sanonofre
	union
	select *
	from preparacion_r1_r2_toluviejo
);


