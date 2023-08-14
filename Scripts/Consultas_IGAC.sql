drop table if exists tmp_predio_ladmtoluviejo;

-- GC Predio Vs Condicion Predio y Sistema Procedencia Registros
create temp table tmp_predio_ladmtoluviejo as
(
	select prediocatastro.t_id prediocatastro_t_id
		,prediocatastro.tipo_catastro
		,prediocatastro.numero_predial
		,prediocatastro.numero_predial_anterior
		,prediocatastro.nupre
		,prediocatastro.circulo_registral
		,prediocatastro.matricula_inmobiliaria_catastro
		,prediocatastro.tipo_predio
		,condicionprediotipo.dispname condicion_predio
		,prediocatastro.destinacion_economica
		,sistemaprocedenciadatostipo.dispname sistema_procedencia_datos
	from ladmtoluviejo.gc_prediocatastro prediocatastro
	left join ladmtoluviejo.gc_condicionprediotipo condicionprediotipo on prediocatastro.condicion_predio = condicionprediotipo.t_id 
	left join ladmtoluviejo.gc_sistemaprocedenciadatostipo sistemaprocedenciadatostipo on prediocatastro.sistema_procedencia_datos = sistemaprocedenciadatostipo.t_id
);

drop table if exists gc_predio_unificado_ladmtoluviejo;

-- GC Predio Catastro VS GC Propietario
create table gc_predio_unificado_ladmtoluviejo as
(
	select tmppredioladmtoluviejo.*
		,gcpropietario.t_id gcpropietario_t_id
		,gcpropietario.tipo_documento 
		,gcpropietario.numero_documento
		,gcpropietario.digito_verificacion
		,gcpropietario.primer_nombre
		,gcpropietario.segundo_nombre
		,gcpropietario.primer_apellido
		,gcpropietario.segundo_apellido
		,gcpropietario.razon_social
	from tmp_predio_ladmtoluviejo tmppredioladmtoluviejo
	left join ladmtoluviejo.gc_propietario gcpropietario on tmppredioladmtoluviejo.prediocatastro_t_id = gcpropietario.gc_predio_catastro
);