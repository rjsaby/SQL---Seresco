drop table if exists tmp_matricula_snr_sin_e;
drop table if exists tmp_matricula_snr;
drop table if exists tmp_matricula_gc_sin_e;
drop table if exists tmp_matricula_gc;
drop table if exists interrelacion_propietario;

-- 
create temp table tmp_matricula_snr_sin_e as
(
	select distinct concat(codigo_orip,matricula_inmobiliaria) matricula_inmobiliaria
		,numero_predial_nuevo_en_fmi
		,numero_predial_anterior_en_fmi
		,titular_numero_documento
		,concat_ws(' ',nombres,primer_apellido,segundo_apellido) nombre_completo_snr
		,razon_social razon_social_snr
	from public.snr_predio_unificado_ladmelpaso
);

--
update tmp_matricula_snr_sin_e
set nombre_completo_snr = null
where nombre_completo_snr = '';

--
create temp table tmp_matricula_snr as
(
select matricula_inmobiliaria
	,numero_predial_nuevo_en_fmi
	,numero_predial_anterior_en_fmi
	,titular_numero_documento
	,coalesce(nombre_completo_snr,razon_social_snr) nombre_completo_snr
from tmp_matricula_snr_sin_e
);

create temp table tmp_matricula_gc_sin_e as
(
	select distinct numero_predial
		,numero_predial_anterior
		,concat(circulo_registral, matricula_inmobiliaria_catastro) matricula_inmobiliaria_catastro
		,numero_documento
		,concat_ws(' ',primer_nombre,segundo_nombre,primer_apellido,segundo_apellido) nombre_completo_igac
		,razon_social razon_social_igac
	from public.gc_predio_unificado_ladmelpaso
	where circulo_registral <> '*' or matricula_inmobiliaria_catastro <> '*'
);

update tmp_matricula_gc_sin_e
set nombre_completo_igac = null
where nombre_completo_igac = '';

update tmp_matricula_gc_sin_e
set nombre_completo_igac = null
where nombre_completo_igac = '* * * *';

create temp table tmp_matricula_gc as
(
select numero_predial
	,numero_predial_anterior
	,matricula_inmobiliaria_catastro
	,numero_documento
	,coalesce(nombre_completo_igac,razon_social_igac) nombre_completo_igac
from tmp_matricula_gc_sin_e
);

-- Estadístico Sin Cambio
select distinct tmpmatriculasnr.matricula_inmobiliaria
	--,tmpmatriculasnr.numero_predial_nuevo_en_fmi
	--,tmpmatriculasnr.numero_predial_anterior_en_fmi
	,tmpmatriculasnr.titular_numero_documento
	,tmpmatriculasnr.nombre_completo_snr
	--,tmpmatriculagc.numero_predial
	--,tmpmatriculagc.numero_predial_anterior
	,tmpmatriculagc.matricula_inmobiliaria_catastro
	,tmpmatriculagc.numero_documento
	,tmpmatriculagc.nombre_completo_igac
from tmp_matricula_snr tmpmatriculasnr
inner join tmp_matricula_gc tmpmatriculagc 
on (tmpmatriculasnr.matricula_inmobiliaria = tmpmatriculagc.matricula_inmobiliaria_catastro)
	and (tmpmatriculasnr.titular_numero_documento) = (tmpmatriculagc.numero_documento)
	and (tmpmatriculasnr.nombre_completo_snr = tmpmatriculagc.nombre_completo_igac)

-- Estadístico Complementar o Rectificar
select distinct tmpmatriculasnr.matricula_inmobiliaria
	--,tmpmatriculasnr.numero_predial_nuevo_en_fmi
	--,tmpmatriculasnr.numero_predial_anterior_en_fmi
	,tmpmatriculasnr.titular_numero_documento
	,tmpmatriculasnr.nombre_completo_snr
	--,tmpmatriculagc.numero_predial
	--,tmpmatriculagc.numero_predial_anterior
	,tmpmatriculagc.matricula_inmobiliaria_catastro
	,tmpmatriculagc.numero_documento
	,tmpmatriculagc.nombre_completo_igac
from tmp_matricula_snr tmpmatriculasnr
inner join tmp_matricula_gc tmpmatriculagc 
on (tmpmatriculasnr.matricula_inmobiliaria = tmpmatriculagc.matricula_inmobiliaria_catastro)
	and (((tmpmatriculasnr.titular_numero_documento) = (tmpmatriculagc.numero_documento) and (tmpmatriculasnr.nombre_completo_snr <> tmpmatriculagc.nombre_completo_igac))
	or ((tmpmatriculasnr.titular_numero_documento) <> (tmpmatriculagc.numero_documento) and (tmpmatriculasnr.nombre_completo_snr = tmpmatriculagc.nombre_completo_igac)))
	
-- Estadístico: Posible Cambio de Propietario
select distinct t1.matricula_inmobiliaria_catastro
	,t1.numero_documento
	,t1.nombre_completo_igac
from (
	select distinct tmpmatriculasnr.matricula_inmobiliaria
		--,tmpmatriculasnr.numero_predial_nuevo_en_fmi
		--,tmpmatriculasnr.numero_predial_anterior_en_fmi
		,tmpmatriculasnr.titular_numero_documento
		,tmpmatriculasnr.nombre_completo_snr
		--,tmpmatriculagc.numero_predial
		--,tmpmatriculagc.numero_predial_anterior
		,tmpmatriculagc.matricula_inmobiliaria_catastro
		,tmpmatriculagc.numero_documento
		,tmpmatriculagc.nombre_completo_igac
	from tmp_matricula_snr tmpmatriculasnr
	inner join tmp_matricula_gc tmpmatriculagc 
	on (tmpmatriculasnr.matricula_inmobiliaria = tmpmatriculagc.matricula_inmobiliaria_catastro)
		and concat(tmpmatriculasnr.titular_numero_documento,tmpmatriculasnr.nombre_completo_snr) 
			<> concat(tmpmatriculagc.numero_documento,tmpmatriculagc.nombre_completo_igac)
	) t1
where t1.matricula_inmobiliaria_catastro = '19231344';



	
