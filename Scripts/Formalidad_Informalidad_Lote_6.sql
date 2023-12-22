select * from ladmcolr123_20231206_astreamunicipal_urbanah7.col_uebaunit;
select * from ladmcolr123_20231206_astreamunicipal_urbanah7.lc_predio;
select * from ladmcolr123_20231206_astreamunicipal_urbanah7.lc_terreno;
--select * from ladmcolr123_20231206_astreamunicipal_urbanah7.lc_predio_informalidad;

-- Relacion terreno\predio
drop table if exists temp_relacion_predio_terreno;
create temp table temp_relacion_predio_terreno as
(
	select relacion.t_id
		,relacion.ue_lc_terreno
		,relacion.baunit
	-- *** Cambio BD ***
	from ladmcolr123_20231206_astreamunicipal_urbanah7.col_uebaunit relacion
	where relacion.ue_lc_terreno is not null
);

-- Se crean las dos versiones de terrenos

drop table if exists temp_terrenos_1;
create temp table temp_terrenos_1 as
(
	select t_id t1_t_id
		,geometria t1_geometria
		,local_id t1_local_id
	-- *** Cambio BD ***
	from ladmcolr123_20231206_astreamunicipal_urbanah7.lc_terreno t1
	where local_id not like '%A%'
);

drop table if exists temp_terrenos_2;
create temp table temp_terrenos_2 as
(
	select t_id t2_t_id
		,geometria t2_geometria
		,local_id t2_local_id
	-- *** Cambio BD ***
	from ladmcolr123_20231206_astreamunicipal_urbanah7.lc_terreno t2
	--where local_id like '%A%'
);

-- Interseccion
drop table if exists interseccion_preliminar;
create temp table interseccion_preliminar as
(
	select distinct t1_t_id t_id_terreno_posible_formal
		,t1_local_id local_id_posible_formal
		,ST_Area(t1_geometria) area_posible_formal
		,t2_t_id t_id_terreno_posible_informal
		,t2_local_id local_id_posible_informal
		,ST_Area(t2_geometria) area_posible_informal
		,ST_Intersects(t1_geometria, t2_geometria) existe_interseccion
		,ST_Area(ST_Intersection(t1_geometria, t2_geometria)) area_cruce
	from temp_terrenos_1, temp_terrenos_2 
	where t1_t_id <> t2_t_id and ST_Intersects(t1_geometria, t2_geometria) is true and ST_Area(ST_Intersection(t1_geometria, t2_geometria)) <> 0
	order by 1, 3
);

-- Relación Predio\Terreno w Interesección
drop table if exists temp_relacion_predioterreno_interseccion_parte_1;
create temp table temp_relacion_predioterreno_interseccion_parte_1 as
(
select t2.baunit baunit_posible_formal 
	,t1.t_id_terreno_posible_formal
	,t1.local_id_posible_formal
	,t1.area_posible_formal
	,t1.t_id_terreno_posible_informal
	,t1.local_id_posible_informal
	,t1.area_posible_informal
	,t1.existe_interseccion
	,t1.area_cruce		
from interseccion_preliminar t1
inner join temp_relacion_predio_terreno t2
on t1.t_id_terreno_posible_formal = t2.ue_lc_terreno
);

drop table if exists temp_relacion_predioterreno_interseccion_parte_2;
create temp table temp_relacion_predioterreno_interseccion_parte_2 as
(
select t1.baunit_posible_formal 
	,t1.t_id_terreno_posible_formal
	,t1.local_id_posible_formal
	,t1.area_posible_formal
	,t2.baunit baunit_posible_informal
	,t1.t_id_terreno_posible_informal	
	,t1.local_id_posible_informal
	,t1.area_posible_informal
	,t1.existe_interseccion
	,t1.area_cruce
from temp_relacion_predioterreno_interseccion_parte_1 t1
inner join temp_relacion_predio_terreno t2
on t1.t_id_terreno_posible_informal = t2.ue_lc_terreno
);

-- Información Predio
select t1.baunit_posible_formal 
	,t1.t_id_terreno_posible_formal
	,t1.local_id_posible_formal
	,concat_ws('-',t2.codigo_orip, t2.matricula_inmobiliaria) fmi_posible_formal
	,t2.numero_predial npn_posible_formal
	,t2.tipo codigo_tipopredio_posible_formal
	,condicion.dispname condicion_predio_posible_formal
	,t1.area_posible_formal
	,t1.baunit_posible_informal
	,t1.t_id_terreno_posible_informal
	,concat_ws('-',t3.codigo_orip, t3.matricula_inmobiliaria) fmi_posible_informal
	,t3.numero_predial npn_posible_informal
	,t3.tipo codigo_tipopredio_posible_informal
	,condicion_1.dispname condicion_predio_posible_informal
	,t1.local_id_posible_informal
	,t1.area_posible_informal
	--,t1.existe_interseccion
	,t1.area_cruce
from temp_relacion_predioterreno_interseccion_parte_2 t1
-- *** Cambio BD ***
inner join ladmcolr123_20231206_astreamunicipal_urbanah7.lc_predio t2 on t2.t_id = t1.baunit_posible_formal
inner join ladmcolr123_20231206_astreamunicipal_urbanah7.lc_predio t3 on t3.t_id = t1.baunit_posible_informal
inner join ladmcolr123_20231206_astreamunicipal_urbanah7.lc_condicionprediotipo condicion on condicion.t_id = t2.condicion_predio
inner join ladmcolr123_20231206_astreamunicipal_urbanah7.lc_condicionprediotipo condicion_1 on condicion_1.t_id = t3.condicion_predio;