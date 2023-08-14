-- Procedimiento Almacenado
--create procedure priorizacion_metodos_lote_6()
create or replace procedure priorizacion_metodos_lote_6()
language plpgsql
as $$
	declare 
		nombre_archivo text;
		consulta text;
		ruta_archivo text;
			
begin
	
	nombre_archivo := 'Estadisticos_';
	ruta_archivo := 'D:/PUBLIC/SERESCO/' || nombre_archivo || to_char(current_date, 'YYYYMMDD') || '.csv';

	-- Temporales Priorización
	drop table if exists terreno_predio_gc;
	drop table if exists terreno_digitalizado_area_calculada;
	drop table if exists resultados_diferencias_areas_gc_dig_terrenos;
	drop table if exists base_analisis_area;
	drop table if exists vericacion_area_juridica;
	drop table if exists predios_con_derivados;
	drop table if exists areas_digitalizadas;
	drop table if exists reporte_sin_estandarizar;
	drop table if exists posibles_informalidades_lote_6;
	drop table if exists construccion_validadores_reporte;
	drop table if exists reporte_estandarizado cascade;
	drop table if exists temp_saldos_conservacion_lote_6;
	drop table if exists temp_preparacion_construcciones_gestor;
	drop table if exists temp_conteo_construcciones_gestor_por_predio;
    drop table if exists temp_conteo_construcciones_digitalizada_por_predio;
	drop table if exists temp_construccion_digitalizada_con_id_predio;
	drop table if exists temp_construcciones_digitalizadas_filtro_lote_6;
	drop table if exists temp_paramatrizacion_nulos_cruce_construcciones;
	drop table if exists temp_parametrizacion_construcciones;
	drop table if exists temp_terrenos_diferencia_construcciones;
	
	-- Fisicas
	drop table if exists condicion_predio_por_npn_lote_6;
	drop table if exists priorizacion_predio_metodo_lote_6 cascade;

	--Vistas
	drop table if exists vw_terrenos_priorizacion_predio_metodo_lote_6;

	--****************************** CONEXIÓN ******************************

	call colombiaseg_lote6.dblink_bd_maphurricane();

	-- ************************************************* PRIORIZACION MÉTODOS *************************************************

	-- 1er INDICADOR: Diferencias de Área entre lo suministrado por GC y lo digitalizado 
	-- (1) Análisis Áreas Digitalizadas
	create temp table terreno_predio_gc as
	(
		select gcprediocatastro.numero_predial
			,gcterreno.area_terreno_alfanumerica
			,gcterreno.area_terreno_digital
			,ST_Area(geometria) area_terreno_calculada_gc
			,st_transform(gcterreno.geometria, 9377) geometria
		from dblink_gc_prediocatastro gcprediocatastro
		left join dblink_gc_terreno gcterreno on gcprediocatastro.t_id = gcterreno.gc_predio
		where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- (2)
	create temp table terreno_digitalizado_area_calculada as
	(
		select predio_t_id
			,predio_numero_predial
			,codigo_unidad_intervencion
			,ST_Area(st_transform(geometria, 9377)) area_terreno_calculada_dig
			--,geometria 
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	);
	
	-- (3)
	create temp table resultados_diferencias_areas_gc_dig_terrenos as
	(
	select terrenosdigitalizadoslote6.predio_t_id
		,terrenosdigitalizadoslote6.predio_numero_predial
		,terrenosdigitalizadoslote6.area_terreno_calculada_dig
		,terrenoprediogc.area_terreno_calculada_gc
		,round(abs(area_terreno_calculada_gc-area_terreno_calculada_dig)) diferencia
		,(round(abs(area_terreno_calculada_gc-area_terreno_calculada_dig))*100)/terrenoprediogc.area_terreno_digital porcentaje_diferencia
		--,terrenosdigitalizadoslote6.geometria
	from terreno_digitalizado_area_calculada terrenosdigitalizadoslote6
	inner join terreno_predio_gc terrenoprediogc 
	on terrenosdigitalizadoslote6.predio_numero_predial = terrenoprediogc.numero_predial
	);	

	-- (4) Análisis Terrenos Superiores en Área o Nacidos en la digitalizacion
	create temp table base_analisis_area as
	(
		select distinct predio_t_id
			,predio_numero_predial
			,area_terreno_calculada_dig
			,area_terreno_calculada_gc
			,diferencia
			,porcentaje_diferencia
			,(case when porcentaje_diferencia > 9 then 'Si'
				   when porcentaje_diferencia <= 9 then 'No'
				   else 'No Aplica'
				   end) superior_en_area_estandar
			-- 5to INDICADOR: Terrenos Digitalizados nuevos frente a la base de datos suministrada por el GC.
			,(case when area_terreno_calculada_gc is null then 'Si'
				   when area_terreno_calculada_gc is not null then 'No'
				   else 'No Aplica'
				   end) terrenos_nuevos_digitalizacion
		from resultados_diferencias_areas_gc_dig_terrenos
	);
	
 	-- 2do INDICADOR: Posibles Informalidades
 	-- Se analiza a partir del caracter 21 del NPN 
	-- (1) Posibles Informalidades
	create table condicion_predio_por_npn_lote_6 as
	(
		select predio_t_id
			,predio_numero_predial
			,(case when substring(predio_numero_predial,21,1) = '0' then 'NPN'
				   when substring(predio_numero_predial,21,1) = '3' then 'Bienes de Uso Público'
				   when substring(predio_numero_predial,21,1) = '4' then 'Vías'
				   when substring(predio_numero_predial,21,1) = '5' then 'Mejoras'
				   when substring(predio_numero_predial,21,1) = '7' then 'Parque Cementerio'
				   when substring(predio_numero_predial,21,1) = '8' then 'Condominio'
				   when substring(predio_numero_predial,21,1) = '9' then 'PH'
				   -- Por argumento de España
				   when substring(predio_numero_predial,21,1) = '2' then 'Informalidad'
				   else 'Sin información'
				   end
			) condicion_predio
		from colombiaseg_lote6.terrenos_digitalizados_lote_6
	);

	-- (2)
	create temp table posibles_informalidades_lote_6 as
	(
		select predio_t_id
			,predio_numero_predial
			,'Si' posible_informalidad
		from condicion_predio_por_npn_lote_6
		where condicion_predio = 'Mejoras' or condicion_predio = 'Informalidad'
	);

 	-- 3er INDICADOR: Comparación Con Área Registral
	-- (1) Verificaciones frente a áreas registrales
	create temp table vericacion_area_juridica as
	(
	select distinct predio_t_id
		,predio_numero_predial
		,'Si' verificacion_area_registral
	from colombiaseg_lote6.verificacion_fisico_juridica_areas_lote_6
	where verificacion_linderos_o_info_juridica = 'Requiere'
	);
	
	-- 4to INDICADOR: Comparación frente a Derivados
	-- (4) Verificaciones frente a derivados
	create temp table predios_con_derivados as
	(
		select distinct uapredio.t_id predio_t_id
			,uapredio.numero_predial predio_numero_predial
			,'Si' predios_con_derivados
		from colombiaseg_lote6.folio_matriz_folio_derivado_lote_6 foliomatrizfolioderivadolote6
		inner join dblink_ua_predio uapredio
		on foliomatrizfolioderivadolote6.folio_matricula = uapredio.matricula_inmobiliaria
		where uapredio.matricula_inmobiliaria is not null
	);

	-- 6to INDICADOR: Presencia de Saldos de Convervación
	create temp table temp_saldos_conservacion_lote_6 as
	(
		select predio_t_id
			,numero_predial
			,'Si' predios_con_saldos
		from colombiaseg_lote6.saldos_conservacion_lote_6
	);	
	
	-- 7mo INDICADOR: Construcciones
	-- (1) Se prepara la información básica y se interrelaciona cada construcción con su respectivo predio
	-- Todo esto solo para Lote 6
	create temp table temp_preparacion_construcciones_gestor as
		(
		select distinct gcconstruccion.identificador
			,gcconstruccion.tipo_construccion
			,gcconstruccion.codigo_terreno numero_predial 
			--,gcprediocatastro.numero_predial
			,gcprediocatastro.t_id predio_t_id
		from dblink_gc_construccion gcconstruccion
		inner join dblink_gc_prediocatastro gcprediocatastro on gcconstruccion.codigo_terreno = gcprediocatastro.numero_predial
		where substring(gcprediocatastro.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- (2) Cálculo de total de construcciones gestor por predio
	create temp table temp_conteo_construcciones_gestor_por_predio as
	(
	select numero_predial
		,count(*) total_construcciones_gestor
	from temp_preparacion_construcciones_gestor
	group by 1
	);
	
	-- (3) Preparación Construcciones Digitalización, asignación de id_predio a cada construcción
	create temp table temp_construccion_digitalizada_con_id_predio as
	(
		select distinct ueconstruccion.identificador
			,ueconstruccion.id_tipoconstruccion
			,reluepredio.id_predio predio_t_id
		from dblink_ue_construccion ueconstruccion
		inner join dblink_rel_uepredio reluepredio on ueconstruccion.t_id = reluepredio.id_construccion
	);
	
	-- (4) Filtro de construcciones por Lote + asignación de NPN
	create temp table temp_construcciones_digitalizadas_filtro_lote_6 as
	(
		select tempconstrucciondigitalizadaconidpredio.*
			,uapredio.numero_predial
		from temp_construccion_digitalizada_con_id_predio tempconstrucciondigitalizadaconidpredio
		inner join dblink_ua_predio uapredio on tempconstrucciondigitalizadaconidpredio.predio_t_id = uapredio.t_id
		where substring(uapredio.numero_predial,1,5) in ('20250', '70713', '70823')
	);
	
	-- (5) Conteo de construcciones digitalizadas por predio
	create temp table temp_conteo_construcciones_digitalizada_por_predio as
	(
		select numero_predial
			,count(*) total_construcciones_digitalizadas
		from temp_construcciones_digitalizadas_filtro_lote_6
		group by 1
	);
	
	-- (6) Parametrización de nulos (no cruce) entre la relación de construcciones gestor\digitalización
	create temp table temp_paramatrizacion_nulos_cruce_construcciones as
	(
		select tempconteoconstruccionesdigitalizadaporpredio.numero_predial numero_predial_digitalizacion
			,tempconteoconstruccionesdigitalizadaporpredio.total_construcciones_digitalizadas
			,tempconteoconstruccionesgestorporpredio.numero_predial numero_predial_gestor
			,tempconteoconstruccionesgestorporpredio.total_construcciones_gestor
			,(case when tempconteoconstruccionesgestorporpredio.total_construcciones_gestor is null then 0
				   else tempconteoconstruccionesgestorporpredio.total_construcciones_gestor
				   end) parametrizacion_conteo_gestor
		from temp_conteo_construcciones_digitalizada_por_predio tempconteoconstruccionesdigitalizadaporpredio
		left join temp_conteo_construcciones_gestor_por_predio tempconteoconstruccionesgestorporpredio
		on tempconteoconstruccionesdigitalizadaporpredio.numero_predial = 
			tempconteoconstruccionesgestorporpredio.numero_predial
	);
	
	-- (7) Parametrización de indicador dependiendo la comparación en número de construcciones
	create temp table temp_parametrizacion_construcciones as
	(
		select numero_predial_digitalizacion
			,total_construcciones_digitalizadas
			,numero_predial_gestor
			,total_construcciones_gestor
			,parametrizacion_conteo_gestor
			,(case when total_construcciones_digitalizadas = parametrizacion_conteo_gestor then 'No'
			       when total_construcciones_digitalizadas > parametrizacion_conteo_gestor then 'Si'
			       when total_construcciones_digitalizadas < parametrizacion_conteo_gestor then 'No'
			       else 'Sin parametrización'
			       end) terrenos_diferencia_construcciones
			,null clasificacion_terrenos_diferencia_construcciones
		from temp_paramatrizacion_nulos_cruce_construcciones
	);
	
	update temp_parametrizacion_construcciones
	set clasificacion_terrenos_diferencia_construcciones = 1
	where terrenos_diferencia_construcciones = 'Si';
	
	update temp_parametrizacion_construcciones
	set clasificacion_terrenos_diferencia_construcciones = 0
	where terrenos_diferencia_construcciones = 'No';

	-- (8) Tabla final
	create temp table temp_terrenos_diferencia_construcciones as
	(
		select uapredio.t_id predio_t_id
			,numero_predial_digitalizacion numero_predial
			,terrenos_diferencia_construcciones
			,clasificacion_terrenos_diferencia_construcciones
		from temp_parametrizacion_construcciones tempparametrizacionconstrucciones
		inner join dblink_ua_predio uapredio
		on tempparametrizacionconstrucciones.numero_predial_gestor = uapredio.numero_predial
	);

	-- ************************************************* CONSTRUCCIÓN MATRIZ INDICATIVA *************************************************
	-- Construcción de Matriz Indicativa Método Directo\Indirecto

	-- (1) Áreas digitalizadas
	create temp table areas_digitalizadas as
	(
		select predio_t_id
			,predio_numero_predial
			,superior_en_area_estandar
			,terrenos_nuevos_digitalizacion
		from base_analisis_area
	);
	
	-- (2) Temporal sin estandar
	create temp table reporte_sin_estandarizar as 
	(
		select terrenosdigitalizadoslote6.predio_t_id
			,terrenosdigitalizadoslote6.predio_numero_predial
			,terrenosdigitalizadoslote6.codigo_unidad_intervencion
			,areasdigitalizadas.superior_en_area_estandar
			,areasdigitalizadas.terrenos_nuevos_digitalizacion
			,posiblesinformalidadeslote6.posible_informalidad
			,vericacionareajuridica.verificacion_area_registral
			,prediosconderivados.predios_con_derivados
			,tempsaldosconservacionlote6.predios_con_saldos
			,tempterrenosdiferenciaconstrucciones.terrenos_diferencia_construcciones
		from colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		left join areas_digitalizadas areasdigitalizadas on terrenosdigitalizadoslote6.predio_t_id = areasdigitalizadas.predio_t_id
		left join posibles_informalidades_lote_6 posiblesinformalidadeslote6 on terrenosdigitalizadoslote6.predio_t_id = posiblesinformalidadeslote6.predio_t_id
		left join vericacion_area_juridica vericacionareajuridica on terrenosdigitalizadoslote6.predio_t_id = vericacionareajuridica.predio_t_id
		left join predios_con_derivados prediosconderivados on terrenosdigitalizadoslote6.predio_t_id = prediosconderivados.predio_t_id
		left join temp_saldos_conservacion_lote_6 tempsaldosconservacionlote6 on terrenosdigitalizadoslote6.predio_t_id = tempsaldosconservacionlote6.predio_t_id
		left join temp_terrenos_diferencia_construcciones tempterrenosdiferenciaconstrucciones on terrenosdigitalizadoslote6.predio_t_id = tempterrenosdiferenciaconstrucciones.predio_t_id
	);
	
	-- (3) Estandarización
	update reporte_sin_estandarizar
	set posible_informalidad = 'No'
	where posible_informalidad is null;
	
	update reporte_sin_estandarizar
	set verificacion_area_registral = 'No'
	where verificacion_area_registral is null;
	
	update reporte_sin_estandarizar
	set predios_con_derivados = 'No'
	where predios_con_derivados is null;
	
	update reporte_sin_estandarizar
	set superior_en_area_estandar = 'No'
	where superior_en_area_estandar is null;	

	update reporte_sin_estandarizar
	set terrenos_nuevos_digitalizacion = 'No'
	where terrenos_nuevos_digitalizacion is null;

	update reporte_sin_estandarizar
	set predios_con_saldos = 'No'
	where predios_con_saldos is null;

	update reporte_sin_estandarizar
	set terrenos_diferencia_construcciones = 'No Aplica' -- Sin construcción
	where predios_con_saldos is null;
	
	-- (4) Construcción de validadores
	create temp table construccion_validadores_reporte as
	(
		select predio_t_id
			,predio_numero_predial
			,codigo_unidad_intervencion
			,superior_en_area_estandar
			,(case when superior_en_area_estandar = 'Si' then 1
				   when superior_en_area_estandar = 'No Aplica' then 1
				   else 0
				   end) categorizacion_superior_en_area_estandar
			,terrenos_nuevos_digitalizacion
			,(case when terrenos_nuevos_digitalizacion = 'Si' then 1
			   else 0
			   end) categorizacion_terrenos_nuevos_digitalizacion	
			,posible_informalidad
			,(case when posible_informalidad = 'Si' then 1
			   else 0
			   end) categorizacion_posible_informalidad	
			,verificacion_area_registral
			,(case when verificacion_area_registral = 'Si' then 1
			   else 0
			   end) categorizacion_verificacion_area_registral
			,predios_con_derivados
			,(case when predios_con_derivados = 'Si' then 1
			   else 0
			   end) categorizacion_predios_con_derivados
			,predios_con_saldos
			,(case when predios_con_saldos = 'Si' then 1
			   else 0
			   end) categorizacion_predios_con_saldos
			,terrenos_diferencia_construcciones
			,(case when terrenos_diferencia_construcciones = 'Si' then 1
				   when terrenos_diferencia_construcciones = 'No' then 0
				   when terrenos_diferencia_construcciones = 'No Aplica' then 0
			   else 0
			   end) categorizacion_terrenos_diferencia_construcciones
		from reporte_sin_estandarizar
	);
	
	create temp table reporte_estandarizado as
	(
		select predio_t_id
			,predio_numero_predial
			,codigo_unidad_intervencion
			,superior_en_area_estandar
			,categorizacion_superior_en_area_estandar
			,terrenos_nuevos_digitalizacion
			,categorizacion_terrenos_nuevos_digitalizacion
			,posible_informalidad
			,categorizacion_posible_informalidad
			,verificacion_area_registral
			,categorizacion_verificacion_area_registral
			,predios_con_derivados
			,categorizacion_predios_con_derivados
			,predios_con_saldos
			,categorizacion_predios_con_saldos
			,terrenos_diferencia_construcciones
			,categorizacion_terrenos_diferencia_construcciones
			,(categorizacion_superior_en_area_estandar + categorizacion_terrenos_nuevos_digitalizacion 
				+ categorizacion_posible_informalidad + categorizacion_verificacion_area_registral 
				+ categorizacion_predios_con_derivados + categorizacion_predios_con_saldos
				+ categorizacion_terrenos_diferencia_construcciones) totalizacion_categorias
		from construccion_validadores_reporte
	);

	-- Creación de Tabla Física
	create table priorizacion_predio_metodo_lote_6 as
	(
		select predio_t_id
				,predio_numero_predial
				,codigo_unidad_intervencion
				,superior_en_area_estandar
				,categorizacion_superior_en_area_estandar
				,terrenos_nuevos_digitalizacion
				,categorizacion_terrenos_nuevos_digitalizacion
				,posible_informalidad
				,categorizacion_posible_informalidad
				,verificacion_area_registral
				,categorizacion_verificacion_area_registral
				,predios_con_derivados
				,categorizacion_predios_con_derivados
				,predios_con_saldos
				,categorizacion_predios_con_saldos
				,terrenos_diferencia_construcciones
				,categorizacion_terrenos_diferencia_construcciones
				,totalizacion_categorias
				,(case when totalizacion_categorias <> 0 then 'Método Directo'
					   else 'Método Indirecto'
					   end) seleccion_metodo
				,(case when totalizacion_categorias > 3 then 'Muy Alto'
					   when totalizacion_categorias = 3 then 'Alto'
				       when totalizacion_categorias = 2 then 'Medio'
				       when totalizacion_categorias = 1 then 'Bajo'
				       else 'No Aplica'
				       end) priorizacion_metodo_indirecto
		from reporte_estandarizado
	);

	-- Creación Vista Priorización de Métodos
	create view vw_terrenos_priorizacion_predio_metodo_lote_6 as
	(
		select distinct row_number () over (order by priorizacionprediometodolote6.predio_t_id) id
				,priorizacionprediometodolote6.predio_t_id
				,priorizacionprediometodolote6.predio_numero_predial
				,priorizacionprediometodolote6.codigo_unidad_intervencion
				,priorizacionprediometodolote6.superior_en_area_estandar
				,priorizacionprediometodolote6.categorizacion_superior_en_area_estandar
				,priorizacionprediometodolote6.terrenos_nuevos_digitalizacion
				,priorizacionprediometodolote6.categorizacion_terrenos_nuevos_digitalizacion
				,priorizacionprediometodolote6.posible_informalidad
				,priorizacionprediometodolote6.categorizacion_posible_informalidad
				,priorizacionprediometodolote6.verificacion_area_registral
				,priorizacionprediometodolote6.categorizacion_verificacion_area_registral
				,priorizacionprediometodolote6.predios_con_derivados
				,priorizacionprediometodolote6.categorizacion_predios_con_derivados
				,priorizacionprediometodolote6.predios_con_saldos
				,priorizacionprediometodolote6.categorizacion_predios_con_saldos
				,priorizacionprediometodolote6.terrenos_diferencia_construcciones
				,priorizacionprediometodolote6.categorizacion_terrenos_diferencia_construcciones
				,priorizacionprediometodolote6.totalizacion_categorias
				,priorizacionprediometodolote6.seleccion_metodo
				,priorizacionprediometodolote6.priorizacion_metodo_indirecto
				,geometria
		from priorizacion_predio_metodo_lote_6 priorizacionprediometodolote6
		inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
		on terrenosdigitalizadoslote6.predio_t_id = priorizacionprediometodolote6.predio_t_id
	);

end;$$
