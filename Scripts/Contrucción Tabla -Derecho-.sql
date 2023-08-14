
drop table if exists derecho_lote_6;

create table derecho_lote_6 as
(
select rrrderecho.t_id derecho_t_id
	,rrrderecho.id_predio predio_t_id
	,rrrderecho.id_interesado interesado_t_id
	,uapredio.numero_predial
	,tcderechotipo.descripcion tipo_derecho
	,rrrderecho.fraccion_derecho fraccion_derecho 
	,rrrderecho.fecha_inicio_tenencia
from serladm.rrr_derecho rrrderecho
inner join serladm.tc_derechotipo tcderechotipo on rrrderecho.tipo = tcderechotipo.t_id
left join serladm.ua_predio uapredio on rrrderecho.id_predio = uapredio.t_id
where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

drop table if exists destinacion_economica_lote_6;

create table destinacion_economica_lote_6 as
(
	select distinct uapredio.t_id predio_t_id
		,tcdestinacioneconomicatipo.descripcion destinacion_economica
	from serladm.ua_predio uapredio
	inner join serladm.tc_destinacioneconomicatipo tcdestinacioneconomicatipo on uapredio.id_destinacioneconomicatipo = tcdestinacioneconomicatipo.t_id
	where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
);

