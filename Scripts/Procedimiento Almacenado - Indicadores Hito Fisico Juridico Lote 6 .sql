-- Procedimiento Almacenado
/*
 * Los procesos que se ejecutan en este PA son:
 * Cálculo y exportación de estadísticos asociados a los
 * indicadores físico jurídicos para el lote 6
 */

/*
 * Requiere:
 * Terrenos Digitalizados Lote 6 Actualizado
 * */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-11-30
 * */

create or replace procedure indicadores_hito_fisicojuridico_lote_6(municipio_codigo text)
language plpgsql
as $proc_body$
	declare
		consulta_1 text;
		ruta_archivo_1 text;
		consulta_2 text;
		ruta_archivo_2 text;
		consulta_3 text;
		ruta_archivo_3 text;
		consulta_4 text;
		ruta_archivo_4 text;
		consulta_5 text;
		ruta_archivo_5 text;
		consulta_6 text;
		ruta_archivo_6 text;
		consulta_7 text;
		ruta_archivo_7 text;
		consulta_8 text;
		ruta_archivo_8 text;
		consulta_9 text;
		ruta_archivo_9 text;

begin
	
	ruta_archivo_1 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_1_' || municipio_codigo ||'.csv';
	ruta_archivo_2 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_2_' || municipio_codigo ||'.csv';
	ruta_archivo_3 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_3_' || municipio_codigo ||'.csv';
	ruta_archivo_4 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_4_' || municipio_codigo ||'.csv';
	ruta_archivo_5 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_5_' || municipio_codigo ||'.csv';
	ruta_archivo_6 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_6_' || municipio_codigo ||'.csv';
	ruta_archivo_7 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_7_' || municipio_codigo ||'.csv';
	ruta_archivo_8 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_8_' || municipio_codigo ||'.csv';
	ruta_archivo_9 := 'C:\docsProyectos\4.SERESCO\Resultados\_7_Gestion_Proyecto\_7_3_Componente_Juridico\_7_3_5_Resultados\estadistico_9_' || municipio_codigo ||'.csv';

	-- Llamada a BDLINK
	call colombiaseg_lote6.dblink_bd_maphurricane();

	-- Borrado tablas
	-- Temporales
	drop table if exists temp_condicion_predio_por_ui_lote_6;
	drop table if exists temp_clase_suelo_predio_por_ui;
	drop table if exists temp_clase_suelo_predio_por_ui_lote_6;
	drop table if exists temp_folios_cerrado_abierto_lote_6;
	drop table if exists temp_terrenos_propietarios;
	drop table if exists temp_centroides_terrenos_lote_6;
	drop table if exists temp_vereda_municipio_lote_6;
	drop table if exists temp_terrenos_ui_vereda;
	drop table if exists temp_gc_centroides_construcciones_lote_6;
	drop table if exists temp_gc_construcciones_ui_lote_6;

	-- Tabla Física
	drop table if exists tbl_terrenos_toluviejo_veredas;
	drop table if exists tbl_gc_construccion_ui;

	-- Condición del Predio
	create temp table temp_condicion_predio_por_ui_lote_6 as
	(
		select terrenosdigitalizadoslote6.predio_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
			,condicionpredioporladmlote6.descripcion condicion_predio
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		inner join colombiaseg_lote6.condicion_predio_por_ladm_lote_6 condicionpredioporladmlote6
		on terrenosdigitalizadoslote6.predio_t_id = condicionpredioporladmlote6.predio_t_id
		where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in (municipio_codigo)
	);

	-- Clase Suelo
	create temp table temp_clase_suelo_predio_por_ui as
	(
		select uapredio.t_id predio_t_id
			,uapredio.numero_predial predio_numero_predial
			,tcclasesuelotipo.descripcion 
		from dblink_ua_predio uapredio
		inner join dblink_tc_clasesuelotipo tcclasesuelotipo on uapredio.id_clasesuelotipo = tcclasesuelotipo.t_id
		where substring(uapredio.numero_predial,1,5) in (municipio_codigo)
	);

	-- Predio \ Vereda \ UI:
	-- (a) Generación de Centroides a Terrenos Digitalizados
	create temp table temp_centroides_terrenos_lote_6 as
	(
	select predio_t_id
		,predio_id_operacion
		,predio_numero_predial
		,terreno_t_id
		,terreno_area_terreno
		,codigo_unidad_intervencion
		,ST_Centroid(st_transform(geometria,9377)) geometria
	from colombiaseg_lote6.terrenos_digitalizados_lote_6
	where predio_numero_predial like (municipio_codigo||'%')
	);

	-- (b) Selección de Vereda dependiendo del Municipio Lote 6
	create temp table temp_vereda_municipio_lote_6 as
	(
		select codigo
			,nombre
			,st_transform(geometria, 9377) geometria
		from dblink_gc_vereda
		-- ****
		where codigo like (municipio_codigo||'%')
	);

	-- (c) Construcción de Join Espacial Entre Centroides\Veredas
	create temp table temp_terrenos_ui_vereda as
	(
	select tempcentroidesterrenoslote6.predio_t_id
		,tempcentroidesterrenoslote6.predio_id_operacion
		,tempcentroidesterrenoslote6.predio_numero_predial
		,tempcentroidesterrenoslote6.terreno_t_id
		,tempcentroidesterrenoslote6.terreno_area_terreno
		,tempcentroidesterrenoslote6.codigo_unidad_intervencion
		,tempveredamunicipiolote6.codigo
		,tempveredamunicipiolote6.nombre
	from temp_centroides_terrenos_lote_6 tempcentroidesterrenoslote6
	left join temp_vereda_municipio_lote_6 tempveredamunicipiolote6 on ST_Intersects(tempcentroidesterrenoslote6.geometria, tempveredamunicipiolote6.geometria)
	);

	-- (d) Eliminación de nulos
	update temp_terrenos_ui_vereda 
	set codigo = 'Urbano\Expansi�n'
	where codigo is null;

	-- (e) Creacion Tabla 
	-- ****
	create table tbl_terrenos_toluviejo_veredas as
	(
		select predio_t_id
			,predio_id_operacion
			,predio_numero_predial
			,terreno_t_id
			,terreno_area_terreno
			,codigo_unidad_intervencion
			,codigo codigo_vereda
			,nombre nombre_vereda
		from temp_terrenos_ui_vereda
	);

	-- (f) Relación Clase de suelo, Predio y UI, asociado informaci�n Veredal
	create temp table temp_clase_suelo_predio_por_ui_lote_6 as
	(
		select terrenosdigitalizadoslote6.predio_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion
			,tblterrenosveredas.codigo_vereda
			,terrenosdigitalizadoslote6.geometria
			,clasesuelopredioporui.descripcion clase_suelo
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		inner join temp_clase_suelo_predio_por_ui clasesuelopredioporui on terrenosdigitalizadoslote6.predio_t_id = clasesuelopredioporui.predio_t_id
		inner join colombiaseg_lote6.tbl_terrenos_toluviejo_veredas tblterrenosveredas on terrenosdigitalizadoslote6.predio_t_id = tblterrenosveredas.predio_t_id 
		where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in (municipio_codigo)
	);

	--Estado de Folios
	-- Folios Activos\Cerrados
	create temp table temp_folios_cerrado_abierto_lote_6 as
	(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.codigo_unidad_intervencion 
		,estadofoliosmatriculalote6.estado_folio
	from colombiaseg_lote6.estado_folios_matricula_lote_6 estadofoliosmatriculalote6
	inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6 
		on estadofoliosmatriculalote6.predio_t_id = terrenosdigitalizadoslote6.predio_t_id
	where substring(terrenosdigitalizadoslote6.predio_numero_predial,1,5) in (municipio_codigo)
	);

	-- Interesados:
	-- (1) Interesados Municipio
	create temp table temp_terrenos_propietarios as
	(
		select clasesuelopredioporui.clase_suelo 
			,clasesuelopredioporui.predio_t_id
			,clasesuelopredioporui.predio_numero_predial
			,clasesuelopredioporui.codigo_unidad_intervencion
			,interesadoslote6.tipo_persona
			,interesadoslote6.documento_identidad
		from temp_clase_suelo_predio_por_ui_lote_6 clasesuelopredioporui inner join colombiaseg_lote6.interesados_lote_6 interesadoslote6
		on clasesuelopredioporui.predio_t_id = interesadoslote6.predio_t_id
	);

	-- Construcciones Gestor Catastral
	-- (1) Preparación de construcciones gestor, generación de centroides y cálculo de área real en [ha]
	create temp table temp_gc_centroides_construcciones_lote_6 as
	(
		select gcconstruccion.t_id gcconstruccion_t_id
			,gcconstruccion.identificador
			,gcconstruccion.tipo_construccion
			,gcconstruccion.tipo_dominio
			,gcconstruccion.numero_pisos
			,gcconstruccion.numero_sotanos
			,gcconstruccion.numero_mezanines
			,gcconstruccion.numero_semisotanos
			,gcconstruccion.codigo_edificacion
			,gcconstruccion.codigo_terreno
			,gcconstruccion.area_construida
			,gcconstruccion.gc_predio
			,gcprediocatastro.t_id gcprediocatastro_t_id
			,gcprediocatastro.numero_predial
			,gcprediocatastro.numero_predial_anterior
			,(ST_Area(st_transform(geometria,9377))/10000) area_calculada_construccion_gestor
			,ST_Centroid(st_transform(gcconstruccion.geometria, 9377)) geometry
		from dblink_gc_construccion gcconstruccion inner join dblink_gc_prediocatastro gcprediocatastro
		on gcconstruccion.gc_predio = gcprediocatastro.t_id
		where substring(gcprediocatastro.numero_predial,1,5) in (municipio_codigo)
	);

	-- (2) Asignación de UI
	create temp table temp_gc_construcciones_ui_lote_6 as
	(
		select tempgccentroidesconstruccioneslote6.gcconstruccion_t_id
				,tempgccentroidesconstruccioneslote6.identificador
				,tempgccentroidesconstruccioneslote6.tipo_construccion
				,tempgccentroidesconstruccioneslote6.tipo_dominio
				,tempgccentroidesconstruccioneslote6.numero_pisos
				,tempgccentroidesconstruccioneslote6.numero_sotanos
				,tempgccentroidesconstruccioneslote6.numero_mezanines
				,tempgccentroidesconstruccioneslote6.numero_semisotanos
				,tempgccentroidesconstruccioneslote6.codigo_edificacion
				,tempgccentroidesconstruccioneslote6.codigo_terreno
				,tempgccentroidesconstruccioneslote6.area_construida
				,tempgccentroidesconstruccioneslote6.gc_predio
				,tempgccentroidesconstruccioneslote6.gcprediocatastro_t_id
				,tempgccentroidesconstruccioneslote6.numero_predial
				,tempgccentroidesconstruccioneslote6.numero_predial_anterior
				,tempgccentroidesconstruccioneslote6.area_calculada_construccion_gestor
				,unidadesintervencionmhlote6.unidadintervencion_t_id
				,unidadesintervencionmhlote6.codigo_municipio
				,unidadesintervencionmhlote6.nombre_municipio
				,unidadesintervencionmhlote6.codigo_unidad_intervencion
				,unidadesintervencionmhlote6.descripcion
				,unidadesintervencionmhlote6.estado
				,tempgccentroidesconstruccioneslote6.geometry
		from temp_gc_centroides_construcciones_lote_6 tempgccentroidesconstruccioneslote6
		inner join colombiaseg_lote6.unidades_intervencion_mh_lote_6 unidadesintervencionmhlote6
		on ST_Intersects(tempgccentroidesconstruccioneslote6.geometry, unidadesintervencionmhlote6.geometria)
	);

	-- (3) Construcción de Tabla -tbl_gc_construccion_ui-
	create table tbl_gc_construccion_ui as
	(
		select gcprediocatastro_t_id
			,numero_predial
			,numero_predial_anterior
			,gcconstruccion_t_id
			,tipo_construccion
			,area_construida
			,area_calculada_construccion_gestor
			,unidadintervencion_t_id
			,codigo_municipio
			,nombre_municipio
			,codigo_unidad_intervencion
			,descripcion
			,estado
		from temp_gc_construcciones_ui_lote_6
	);

-- ************************************************* Estad�sticos *************************************************

	-- ESTAD�STICO (2)
	-- Estadístico: Total General Predios por su Condici�n
	consulta_1 := format('copy (select condicion_predio
									,count(*) total_predios
								from temp_condicion_predio_por_ui_lote_6
								group by 1
								order by 1) to %L CSV HEADER', ruta_archivo_1);							
						
	-- Estadístico: Condici�n Predio Zona UI
	consulta_2 := format('copy (select condicionpredioporuilote6.codigo_unidad_intervencion
									,clasesuelopredioporui.descripcion ui_zona
									,condicionpredioporuilote6.condicion_predio
									,count(*) total_predios
								from temp_condicion_predio_por_ui_lote_6 condicionpredioporuilote6 inner join temp_clase_suelo_predio_por_ui clasesuelopredioporui on condicionpredioporuilote6.predio_t_id = clasesuelopredioporui.predio_t_id
								group by 1,2,3
								order by 1) to %L CSV HEADER', ruta_archivo_2);							
							
	-- ESTADÍSTICO (3)
	-- Estadístico: Zona UI Vereda Predio
	consulta_3 := format('copy (select codigo_unidad_intervencion
									,clase_suelo
									,codigo_vereda
									,count(*) total_predios
									,round(sum(ST_Area(geometria)/10000)) area_total_ha
								from temp_clase_suelo_predio_por_ui_lote_6
								group by 1, 2, 3
								order by 1) to %L CSV HEADER', ruta_archivo_3);					
					
	-- ESTADISTICO (4)
	consulta_4 := format('copy (select codigo_unidad_intervencion
									,estado_folio
									,count(*) total
								from temp_folios_cerrado_abierto_lote_6
								where estado_folio <> ''Sin informaci�n por BD''
								group by 1, 2
								order by 1) to %L CSV HEADER', ruta_archivo_4);								
							
	-- ESTADISTICO (5)
	-- (5.1) Total Predios por UI y Clase de Suelo
	consulta_5 := format('copy (select codigo_unidad_intervencion
									,clase_suelo
									,count(*) total
								from temp_terrenos_propietarios
								group by 1, 2
								order by 1) to %L CSV HEADER', ruta_archivo_5);								
							
	-- (5.2) Total Interesados por UI y Clase de Suelo
	consulta_6 := format('copy (select t1.clase_suelo 
									,t1.codigo_unidad_intervencion
									,count(*) total_interesados
								from (
									select clase_suelo
										,codigo_unidad_intervencion
										,predio_t_id
										,documento_identidad
									from temp_terrenos_propietarios
									where documento_identidad <> '' '' and documento_identidad is not null and documento_identidad <> ''0''
								) t1
								group by 1, 2
								order by 2) to %L CSV HEADER', ruta_archivo_6);							
							
	-- ESTADISTICO (7)
	-- (7.1) Construcciones Gestor
	
	consulta_7 := format('copy (select codigo_unidad_intervencion
									,count(*) total
									from colombiaseg_lote6.tbl_gc_construccion_ui
									where substring(numero_predial,1,5) in ('''|| municipio_codigo ||''') 
									group by 1) to %L CSV HEADER', ruta_archivo_7);	
						
	-- (7.2) Construcciones Digitalizadas
	-- Cantidad de construcciones actualizadas
	consulta_8 := format('copy (select codigo_unidad_intervencion
									,count(*) total
									from colombiaseg_lote6.tbl_gc_construccion_ui
									where substring(numero_predial,1,5) in ('''|| municipio_codigo ||''')
								group by 1) to %L CSV HEADER', ruta_archivo_8);							
							
	-- Sumatoria de áreas de construcciones nuevas por unidades de intervención
	consulta_9 := format('copy (select t1.codigo_unidad_intervencion
									,sum(area_calculada)
								from (
									select codigo_unidad_intervencion
										,(ST_Area(st_transform(geometria,9377))/10000) area_calculada
									from colombiaseg_lote6.construcciones_digitalizadas_lote_6
									where substring(predio_numero_predial,1,5) in ('''|| municipio_codigo ||''')
								) t1
								group by 1
								order by 1) to %L CSV HEADER', ruta_archivo_9);
						
	-- Ejecucion
	execute consulta_1;
	execute consulta_2;
	execute consulta_3;
	execute consulta_4;
	execute consulta_5;
	execute consulta_6;
	execute consulta_7;
	execute consulta_8;
	execute consulta_9;
	
end;
$proc_body$;
