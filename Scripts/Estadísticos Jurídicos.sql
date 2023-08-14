-- Predios Vs Tipo Suelo
select tipo_suelo_registro
	,count(*)
from public.snr_predio_unificado_ladmtoluviejo
group by tipo_suelo_registro

-- Omisión\Comisión
drop table if exists snr_matricula;
drop table if exists gc_matricula;

create temp table snr_matricula as
(
	select distinct concat(codigo_orip,matricula_inmobiliaria) snr_matricula
	from public.snr_predio_unificado_ladmtoluviejo
);

create temp table gc_matricula as
(
	select distinct concat(circulo_registral,matricula_inmobiliaria_catastro) gc_matricula
	from public.gc_predio_unificado_ladmtoluviejo
);

-- Comisión
select *
from gc_matricula gcmatricula
left join snr_matricula snrmatricula on  gcmatricula.gc_matricula = snrmatricula.snr_matricula
where snr_matricula is null

-- Omisión
select *
from snr_matricula snrmatricula
left join gc_matricula gcmatricula on snrmatricula.snr_matricula = gcmatricula.gc_matricula
where gc_matricula is null

