drop table if exists temp_interesados_lote_6;
drop table if exists preparacion;

create temp table temp_interesados_lote_6 as
(
	select predio_t_id
		,tipo_persona
		,tipo_documento 
		,documento_identidad 
		,concat_ws(' ', primer_nombre, segundo_nombre, primer_apellido, segundo_apellido) nombres
	from public.interesados_lote_6
);


select *
from public.interesados_lote_6

-- Temporal y vista
create temp table preparacion as
(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion
		,estadofoliosmatriculalote6.matricula_inmobiliaria
		,estadofoliosmatriculalote6.estado_folio
		,derecholote6.tipo_derecho
		,derecholote6.fraccion_derecho
		,derecholote6.fecha_inicio_tenencia
		,condicionpredioporladmlote6.descripcion condicion_predio
		,destinacioneconomicalote6.destinacion_economica
		,concat_ws(' ', interesadoslote6.primer_nombre, interesadoslote6.segundo_nombre, interesadoslote6.primer_apellido, interesadoslote6.segundo_apellido) nombres
		,interesadoslote6.tipo_persona
		,interesadoslote6.tipo_documento
		,interesadoslote6.documento_identidad
		,terrenosdigitalizadoslote6.geometria 
	from public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	left join public.estado_folios_matricula_lote_6 estadofoliosmatriculalote6 on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
	left join public.condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6 on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
	left join public.derecho_lote_6 derecholote6 on terrenosdigitalizadoslote6.predio_t_id = derecholote6.predio_t_id 
	left join public.interesados_lote_6 interesadoslote6 on interesadoslote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
	left join public.destinacion_economica_lote_6 destinacioneconomicalote6 on destinacioneconomicalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id 
);

drop table if exists predio_socializacion_nivel_3;

create table predio_socializacion_nivel_3 as
(
	select terrenosdigitalizadoslote6.predio_t_id predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion codigo_unidad_intervencion
		,estadofoliosmatriculalote6.matricula_inmobiliaria matricula_inmobiliaria
		,estadofoliosmatriculalote6.estado_folio
		,derecholote6.tipo_derecho tipo_derecho
		,derecholote6.fraccion_derecho fraccion_derecho
		,derecholote6.fecha_inicio_tenencia fecha_inicio_tenencia
		,condicionpredioporladmlote6.descripcion condicion_predio
		,destinacioneconomicalote6.destinacion_economica destinacion_economica
		,interesadoslote6.primer_nombre primer_nombre
		,interesadoslote6.segundo_nombre segundo_nombre
		,interesadoslote6.primer_apellido primer_apellido
		,interesadoslote6.segundo_apellido segundo_apellido
		,interesadoslote6.razon_social 
		,interesadoslote6.tipo_persona tipo_persona
		,interesadoslote6.tipo_documento tipo_documento
		,interesadoslote6.documento_identidad documento_identidad
	from terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	left join estado_folios_matricula_lote_6 estadofoliosmatriculalote6 on terrenosdigitalizadoslote6.predio_t_id = estadofoliosmatriculalote6.predio_t_id
	left join condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6 on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
	left join derecho_lote_6 derecholote6 on terrenosdigitalizadoslote6.predio_t_id = derecholote6.predio_t_id 
	left join interesados_lote_6 interesadoslote6 on interesadoslote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
	left join destinacion_economica_lote_6 destinacioneconomicalote6 on destinacioneconomicalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
)



