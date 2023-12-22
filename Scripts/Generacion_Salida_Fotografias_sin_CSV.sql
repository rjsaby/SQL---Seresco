select distinct T1.predio_num numero_predial
    ,T2.nombre_folder
    ,T2.nombre_punto
    --,T2.latitud
    --,T2.longitud
    ,T2.nombre_fotografia
    ,ST_Intersects(T1.geom, T2.geom) cruce_espacial
    ,T1.geometria
from colombiaseg_lote6.subdivision_ueterreno_lote_6_el_paso T1, (
    select nombre_folder 
        ,nombre_punto
        ,latitud
        ,longitud
        ,nombre_fotografia
        ,st_transform(st_point(longitud, latitud, 4686), 9377) geom 
    from colombiaseg_lote6.kmz_estandarizados_lote_6) T2
where ST_Intersects(T1.geometria, T2.geom) is true;


select nombre_fotografia
,count(*) conteo
from public.terrenos_fotografias_por_kmz_lote_6
where nombre_folder = 'SUBD_ELPASO'
group by nombre_fotografia
having count(*) > 1;

drop table if exists parametrizacion_fotografias;

create table parametrizacion_fotografias as
(
select numero_predial
	,nombre_folder
	,nombre_punto
	,nombre_fotografia
	,concat(numero_predial,'_',nombre_fotografia) nombre_foto_npn
from terrenos_fotografias_por_kmz_lote_6
where nombre_folder = 'SUBD_ELPASO' and nombre_fotografia = 'Línea 20_09.jpg'
);

create table creacion_folios_fotografias as
(
	select distinct numero_predial
	from terrenos_fotografias_por_kmz_lote_6
	where nombre_folder = 'SUBD_ELPASO'
);