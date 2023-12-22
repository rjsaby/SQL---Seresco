select (case when t1.municipio = '823' then 'San José de Toluviejo'
	when t1.municipio = '250' then 'El Paso'
	when t1.municipio = '787' then 'Tamalameque'
	when t1.municipio = '032' then 'Astrea'
	when t1.municipio = '268' then 'El Peñon'
	when t1.municipio = '458' then 'Montecristo'
	when t1.municipio = '713' then 'San Onofre'
	else 'error'
	end) muncipio	
	,t1.numero_predial
	--,t1.id_condicionpredio
	,t2.descripcion condicion_predio_tipo
	,t1.matricula_inmobiliaria 
	,t1.nombre
	,t1.numero_predial_anterior
	,t1.id_operacion	
from serladm.ua_predio t1
inner join serladm.tc_condicionprediotipo t2
on t1.id_condicionpredio = t2.t_id 


