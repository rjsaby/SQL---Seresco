drop table if exists av_zonahomogeneafisicarural_astrea;


select *
from public."ZHT_ElPaso";


create table av_zonahomogeneafisicarural_elpaso as
(
select objectid t_id
	,(objectid * 2) t_ili_id
	,concat('Z',objectid) codigo
	,cod_zht codigo_zona_homogenea 
	,cast(cod_ua as numeric) area_homogenea_tierra
	,(case when agua = 'ESCASAS' then 356
		   when agua = 'SUFICIENTES' then 355
		   else 0
		   end) disponibilidad_agua
	,(case when vias = 'Malas' then 455
	       when vias = 'Buenas' then 451
	       when vias = 'Regulares' then 453
	       else 0
	       end)	influencia_vial
	,(case when usoactuals = 'Mixto' then 834
	       when usoactuals = 'Pastos naturales' then 827
	       when usoactuals = 'Cultivos permanentes' then 822  
	       when usoactuals = 'Bosques' then 830
	       when usoactuals = 'Tierras con maleza' then 825
	       when usoactuals = 'Tierras de labor irrigadas' then 823
	       when usoactuals = 'Cuerpos de agua' then 832
	       when usoactuals = 'Pastos artificiales' then 828
	       when usoactuals = 'Condiciones especiales' then 833
	       when usoactuals = 'Tierras de labor no irrigadas' then 824	       
	       when usoactuals is null then null
	       else null
	       end) uso_actual_suelo
	,null norma_uso_suelo
	,cast('2024-01-01' as date) vigencia
	,geom
from public."ZHT_ElPaso"
-- where usoactuals is not null
order by 1
);


/*
select agua
	,count(*)
from public."ZHT_ElPaso"
group by 1;

select * from serladm.av_disponibilidadaguatipo;

select vias
	,count(*)
from public."ZHT_ElPaso"
group by 1;

select * from serladm.av_influenciavialruraltipo;

select usoactuals
	,count(*)
from public."ZHT_ElPaso"
group by 1; 

select * from serladm.av_usosueloruraltipo
*/

-- ZHF Rurales

create table av_zonahomogeneafisicarural_astrea as
(
	select objectid t_id
		,(objectid * 2) t_ili_id
		,concat('Z',objectid) codigo
		,cod_zhf_2 codigo_zona_homogenea 
		,cast(cod_aht as numeric) area_homogenea_tierra
		,(case when aguas = 'Escasas' then 356
			   when aguas = 'Suficientes' then 355
			   else null
			   end) disponibilidad_agua
		,(case when vías = 'Malas' then 455
		       when vías = 'Buenas' then 451
		       when vías = 'Regulares' then 453
		       else null
		       end)	influencia_vial
		,(case when uso_actual = 'Mixto' then 834
		       when uso_actual = 'Pastos naturales' then 827
		       when uso_actual = 'Cultivos permanentes' then 822
		       when uso_actual = 'Bosques' then 830
		       when uso_actual is null then null
		       else null
		       end) uso_actual_suelo
		,norma_de_u norma_uso_suelo
		,cast('2024-01-01' as date) vigencia
		,geom
	from public.shape_tabla_astrea
	where uso_actual is not null
	order by 1
);

select aguas
	,count(*)
from public.shape_tabla_astrea
group by 1;

select * from serladm.av_disponibilidadaguatipo;

select vías
	,count(*)
from public.shape_tabla_astrea
group by 1;

select * from serladm.av_influenciavialruraltipo;

select uso_actual
	,count(*)
from public.shape_tabla_astrea
group by 1; 

select * from serladm.av_usosueloruraltipo;

-- -- ZHF Urbanas
drop table if exists av_zonahomogeneafisicaurbana_astrea_laye;

create table av_zonahomogeneafisicaurbana_elpaso_elcarmen as
(
	select id_0  t_id
	,id_0*2 t_ili_id
	,concat('Z',codigo) codigo
	,codzonafis codigo_zona_fisica
	,(case when topografia = 'Rango de pendiente 0-7%' then 55
		else null
		end) topografia
	,(case when servpub = 'Servicios básicos incompletos' then 495
	       when servpub = 'Servicios básicos completos' then 496
		   else null
		   end) servicio_publico
	,(case when infvias = 'Pavimentadas' then 760
		when infvias = 'Sin Pavimentar' then 761
		else null
		end) influencia_vial
	,(case when usoactsuel = 'Lote' then 5
		when usoactsuel = 'Residencial' then 1
		when usoactsuel = 'Institucional' then 4
		else null
		end) uso_actual_suelo
	,normusosue norma_uso_suelo
	,(case when tipconstru = 'Residencial 2 - (Bajo)' then 477
	when tipconstru is null then null
	else null
	end) tipificacion_construccion
	,cast('2024-01-01' as date) vigencia
	,geom
	from public."ZHFU_El_Paso_ElCarmen"
);

-- Dominios
-- * Topografía
select topografia
	,count(*)
from public."ZHFU_El_Paso_ElCarmen"
group by 1; 

select * from serladm.av_topografiazonatipo;

-- * Infraestructura Vial
select infvias
	,count(*)
from public."ZHFU_El_Paso_ElCarmen"
group by 1; 

select * from serladm.av_influenciavialurbanatipo;

-- * Servicio público
select servpub
	,count(*)
from public."ZHFU_El_Paso_ElCarmen"
group by 1; 

select * from serladm.av_serviciospublicostipo;

-- * Uso del Suelo
select usoactsuel
	,count(*)
from public."ZHFU_El_Paso_ElCarmen"
group by 1; 

select * from serladm.av_usosuelourbanotipo;

-- * Tipificación construcción
select tipconstru
	,count(*)
from public."ZHFU_El_Paso_ElCarmen"
group by 1; 

select * from serladm.av_tipificacionconstrucciontipo;