drop table if exists relacion_predio_orden_trabajo_subida_campo;

-- Relación predio\orden de trabajo: Dependiendo de lo cargado e la subida campo (sincronización) se identifica su id predio
create temp table relacion_predio_orden_trabajo_subida_campo as
(
	select distinct relordenpredio.id_ordentrabajo
		,relordenpredio.id_predio
		,mhsubidascampo.nombre
		,mhsubidascampo.fecha
		,mhsubidascampo.cargado
	from serladm.rel_ordenpredio relordenpredio
	inner join serladm.mh_subidas_campo mhsubidascampo
	on relordenpredio.id_ordentrabajo = mhsubidascampo.id_orden
);

-- Total de Predios por ID Orden
select id_ordentrabajo 
	,count(*)
from serladm.rel_ordenpredio
where id_ordentrabajo = 58379
group by 1

-- Predios asociados a una orden
select id_ordentrabajo
	,id_predio
from serladm.rel_ordenpredio
where id_ordentrabajo = 58379

-- Terrenos asociados a una orden de trabajo
select distinct *
from relacion_predio_orden_trabajo_subida_campo relacionpredioordentrabajosubidacampo
inner join terreno_orden_trabajo_lote_6 terrenoordentrabajolote6
on relacionpredioordentrabajosubidacampo.id_predio = terrenoordentrabajolote6.predio_t_id
where relacionpredioordentrabajosubidacampo.id_ordentrabajo = 58379

-- ********************** AVANCE CAMPO **********************
select *
from serladmcampo."MAPHURRICANE"

-- Modificación Proyecto
select * from serladmcampo.qgis_projects;

-- Relación UE y Novedades
select * from serladmcampo.rel_uepredio where substring(numero_predial,1,5) in ('20250', '70713', '70823');

-- División Líneas
select * from serladmcampo.ue_divide_lines where substring(predio,1,5) in ('20250', '70713', '70823');

-- Polígonos
select * from serladmcampo.ue_polygons where substring(predio,1,5) in ('20250', '70713', '70823');


