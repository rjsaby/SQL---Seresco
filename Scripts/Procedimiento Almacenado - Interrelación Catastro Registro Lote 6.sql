-- Procedimiento Almacenado
/*
 * Los procesos que se ejecutan en este PA son:
 * Preparación de información tabular para generación de estadísticos interrelación C\R (Gestor)
 * Preparación de información tabular para generación de estadísticos interrelación C\R (Levantamiento Catastral)
 */

/*
 * Desarrollado por: Rodian Saby
 * Última Actualización: 2023-11-14
 * */

create or replace procedure estadisticos_interrelacion_catastro_registro_lote_6()
language plpgsql
as $$
begin
	
	-- *** Conexión BDLINK ***
	call colombiaseg_lote6.dblink_bd_maphurricane();

	-- *** Borrado Tablas ***
	drop table if exists tbl_interrelacion_gestor_snr_lote_6;
	drop table if exists tbl_interrelacion_levantamientocatastral_snr_lote_6;

	-- *** Borrado Vista ***
	drop view if exists vw_terrenos_interrelacion_gccatastro_registro_lote_6;
	drop view if exists vw_terrenos_interrelacion_lccatastro_registro_lote_6;

	-- Interrelación SNR\GC
	create table tbl_interrelacion_gestor_snr_lote_6 as
	(
		select (case when substring(prediocata.numero_predial,1,5) = '20250' then 'El Paso'
				when substring(prediocata.numero_predial,1,5) = '70713' then 'San Onofre'
				when substring(prediocata.numero_predial,1,5) = '70823' then 'Toluviejo'
				else 'Sin información'
				end) nombre_municipio
			,prediocata.t_id predio_t_id
			,inipredio.t_id t_id_inipredioinsumo
			,empa.dispname tipo_emparejamiento
			,empa.description descripcion_emparejamiento
			--,inipredio.observaciones
			,inipredio.gc_predio_catastro
			,inipredio.snr_predio_juridico
			,prediocata.numero_predial
			,(case when prediocata.circulo_registral = '*' then null
				when prediocata.circulo_registral <> '*' then concat_ws('-', prediocata.circulo_registral, prediocata.matricula_inmobiliaria_catastro)
				else 'Sin información'
				end) folio_matricula_inmobiliaria
			,gcondipredio.dispname condicion_predio
			,prediocata.destinacion_economica
		from dblink_ini_predioinsumos inipredio
		inner join dblink_ini_emparejamientotipo empa on inipredio.tipo_emparejamiento = empa.t_id
		inner join dblink_gc_prediocatastro prediocata on inipredio.gc_predio_catastro = prediocata.t_id
		inner join dblink_gc_condicionprediotipo gcondipredio on prediocata.condicion_predio = gcondipredio.t_id
		where substring(prediocata.numero_predial,1,5) in ('20250', '70713', '70823')
	);	
	
	-- Interrelación SNR\LC
	create table tbl_interrelacion_levantamientocatastral_snr_lote_6 as
	(
		select (case when substring(uapre.numero_predial,1,5) = '20250' then 'El Paso'
				when substring(uapre.numero_predial,1,5) = '70713' then 'San Onofre'
				when substring(uapre.numero_predial,1,5) = '70823' then 'Toluviejo'
				else 'Sin información'
				end) nombre_municipio 
			,uapre.t_id predio_t_id
			,uaini.ini_predio_insumos
			,uaini.ua_predio
			,empa.dispname tipo_emparejamiento
			--,inipredio.observaciones
			,uapre.numero_predial
			--,uapre.id_prediotipo
			,tccondipre.descripcion condicion_predio
			,tcestapred.descripcion estado_predio
			,tcdestieco.descripcion destinacion_economica_tipo
		from dblink_ua_predio_ini_predioinsumos uaini
		inner join dblink_ini_predioinsumos inipredio on inipredio.t_id = uaini.ini_predio_insumos
		inner join dblink_ini_emparejamientotipo empa on inipredio.tipo_emparejamiento = empa.t_id
		inner join dblink_ua_predio uapre on uapre.t_id = uaini.ua_predio
		inner join dblink_tc_condicionprediotipo tccondipre on tccondipre.t_id = uapre.id_condicionpredio
		inner join dblink_tc_estadopredio tcestapred on tcestapred.t_id = uapre.id_estadopredio
		inner join dblink_tc_destinacioneconomicatipo tcdestieco on tcdestieco.t_id = uapre.id_destinacioneconomicatipo
		where substring(uapre.numero_predial,1,5) in ('20250', '70713', '70823')
	); 
	
	-- *** Vistas ***
	-- Vista Terrenos \ Interrelación GC-SNR
	create view vw_terrenos_interrelacion_gccatastro_registro_lote_6 as
	(
		select row_number () over (order by inter.predio_t_id) id
			,inter.nombre_municipio
			,inter.predio_t_id
			,inter.t_id_inipredioinsumo
			,inter.tipo_emparejamiento
			,inter.descripcion_emparejamiento
			,inter.gc_predio_catastro
			,inter.snr_predio_juridico
			,inter.numero_predial
			,inter.folio_matricula_inmobiliaria
			,inter.condicion_predio
			,inter.destinacion_economica		
			,geome.geometria
		from colombiaseg_lote6.tbl_interrelacion_gestor_snr_lote_6 inter
		inner join colombiaseg_lote6.terrenos_gestor_lote_6 geome on inter.numero_predial = geome.numero_predial
	);

	-- alter table vw_terrenos_interrelacion_gccatastro_registro_lote_6 add constraint pk_id primary key (id); 

	-- Vista Terrenos \ Interrelación LC-SNR
	create view vw_terrenos_interrelacion_lccatastro_registro_lote_6 as
	(
		select row_number () over (order by inter.predio_t_id) id
			,inter.nombre_municipio 
			,inter.predio_t_id
			,inter.ini_predio_insumos
			,inter.ua_predio
			,inter.tipo_emparejamiento
			,inter.numero_predial
			,inter.condicion_predio
			,inter.estado_predio
			,inter.destinacion_economica_tipo
			,geome.geometria
		from colombiaseg_lote6.tbl_interrelacion_levantamientocatastral_snr_lote_6 inter
		inner join colombiaseg_lote6.terrenos_digitalizados_lote_6 geome on inter.predio_t_id = geome.predio_t_id
	);

	-- alter table vw_terrenos_interrelacion_lccatastro_registro_lote_6 add constraint pk_id primary key (id); 

end;
$$;
	

 