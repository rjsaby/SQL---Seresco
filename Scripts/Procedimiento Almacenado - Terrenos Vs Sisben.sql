-- Procedimiento Almacenado
/*
 * Los procesos que se ejecutan en este PA son:
 * Interrelación de la información SISBEN con la capa de Terrenos\Predios digitalizados del lote 6
 */

/*
 * Requiere:
 * Terrenos Digitalizados Lote 6 Actualizado
 * */

/*
 * Desarrollado por: Rodian Saby
 * Última actualización: 2023-11-30
 * */

create or replace procedure sisben_terrenos_lote_6()
language plpgsql
as $cuerpo_procedimiento$

begin

	drop table if exists temp_sisben;
	drop table if exists sisben_georreferenciado_lote_6;
	
	drop table if exists terrenos_sisben_lote_6;
	
	create temp table temp_sisben as
	(
		select --cod_dpto
			cod_mpio
			,(case when cod_clase = '1' then 'cabecera'
				when cod_clase = '2' then 'centro poblado'
				when cod_clase = '3' then 'rural disperso'
				else cod_clase
				end) cod_clase
			,cast(tot_viviendas as numeric) tot_viviendas
			--,num_ficha
			--,fec_digitalizacion
			,pri_apellido
			,seg_apellido
			,pri_nombre
			,sec_nombre
			,(case when tip_documento = '1' then 'Registro civil'
				when tip_documento = '2' then 'Tarjeta de identidad'
				when tip_documento = '3' then 'C�dula de ciudadan�a'
				when tip_documento = '4' then 'C�dula de extranjer�a'
				when tip_documento = '5' then 'DNI (pa�s de origen)'
				when tip_documento = '6' then 'Pasaporte'
				when tip_documento = '7' then 'Salvoconducto para refugiado'
				when tip_documento = '8' then 'Permiso Especial de Permanencia (PEP)'
				when tip_documento = '9' then 'Permiso de Protecci�n Temporal (PPT)'
				else tip_documento
				end)
			,num_documento
			--,grupo
			--,nivel
			--,clasificacion
			--,coord_x_manual_rec
			--,coord_y_manual_rec
			,cast(coord_x_auto_rec as numeric) coordenada_x
			,cast(coord_y_auto_rec as numeric) coordenada_y
			,cod_centro_poblado
			--,cod_comuna
			--,cod_corregimiento
			,nom_corregimiento
			--,cod_vereda
			,nom_vereda
			--,cod_barrio
			--,nom_barrio
			,dir_vivienda
			,(case when tip_vivienda = '1' then 'casa'
				when tip_vivienda = '2' then 'apartamento'
				when tip_vivienda = '3' then 'cuarto'
				when tip_vivienda = '4' then 'otro tipo de vivienda'
				when tip_vivienda = '5' then 'vivienda indigena'
				else tip_vivienda
				end) tip_vivienda
			,(case when tip_mat_paredes = '1' then 'Bloque, ladrillo, piedra, madera pulida'
				when tip_mat_paredes = '2' then 'Tapia pisada, adobe'
				when tip_mat_paredes = '3' then 'Bahareque'
				when tip_mat_paredes = '4' then 'Material prefabricado'
				when tip_mat_paredes = '5' then 'Madera burda, tabla, tabl�n'
				when tip_mat_paredes = '6' then 'Guadua, casa, esterilla, otro vegetal'
				when tip_mat_paredes = '7' then 'Zinc, tela, lona, cart�n, latas, desechos, pl�stico'
				when tip_mat_paredes = '8' then 'Sin paredes'
				else tip_mat_paredes
				end) tip_mat_paredes
			,(case when tip_mat_pisos = '1' then 'Alfombra o tapete, m�rmol, parque, madera pulida y lacada'
				when tip_mat_pisos = '2' then 'Baldosa, vinilo, tableta, ladrillo'
				when tip_mat_pisos = '3' then 'Cemento, gravilla'
				when tip_mat_pisos = '4' then 'Madera burda, madera en mal estado, tabla, tabl�n'
				when tip_mat_pisos = '5' then 'Tierra o arena'
				when tip_mat_pisos = '6' then 'otro'
				else tip_mat_pisos
				end) tip_mat_pisos
			--,ind_tiene_energia
			--,tip_estrato_energia
			--,ind_tiene_alcantarillado
			--,ind_tiene_gas
			--,ind_tiene_recoleccion
			--,ind_tiene_acueducto
			--,tip_estrato_acueducto
			,num_cuartos_vivienda
			,num_hogares_vivienda
			,(case when tip_ocupa_vivienda = '1' then 'En arriendo o subarriendo'
				when tip_ocupa_vivienda = '2' then 'Propia, la est�n pagando'
				when tip_ocupa_vivienda = '3' then 'Propia, totalmente pagada'
				when tip_ocupa_vivienda = '4' then 'Con permiso del propietario'
				when tip_ocupa_vivienda = '5' then 'Posesi�n sin t�tulo, ocupante de hecho'
				else tip_ocupa_vivienda
				end) tip_ocupa_vivienda
			,cast(num_cuartos_exclusivos as numeric) num_cuartos_exclusivos
			,cast(num_cuartos_dormir as numeric) num_cuartos_dormir
			,cast(num_cuartos_unicos as numeric) num_cuartos_unicos
			,(case when tip_santario = '1' then 'Con conexi�n a alcantarillado'
				when tip_santario = '2' then 'Con conexi�n a pozo s�ptico'
				when tip_santario = '3' then 'Sin conexi�n a alcantarillado ni a pozo s�ptico'
				when tip_santario = '4' then 'Letrina, bajamar'
				when tip_santario = '5' then 'No tiene'
				else tip_santario
				end)
			,(case when tip_ubi_sanitario = '1' then 'Dentro de la vivienda'
				when tip_ubi_sanitario = '2' then 'Fuera de la vivienda'
				when tip_ubi_sanitario = '3' then 'No aplica por flujo'
				else tip_ubi_sanitario
				end) tip_ubi_sanitario
			--,tip_uso_sanitario
			--,tip_origen_agua
			--,ind_agua_llega_7dias
			--,num_dias_llega
			--,ind_agua_llega_24horas
			--,num_horas_llega
			--,tip_uso_agua_beber
			--,tip_elimina_basura
			,(case when ind_tiene_cocina = '1' then 'si'
				when ind_tiene_cocina = '2' then 'no'
				else ind_tiene_cocina
				end) ind_tiene_cocina
			--,tip_prepara_alimentos
			--,tip_uso_cocina
			--,tip_energia_cocina
			--,ind_evento_inundacion
			--,num_evento_inundacion
			--,ind_evento_avalancha
			--,num_evento_avalancha
			--,ind_evento_terremoto
			--,num_evento_terremoto
			--,ind_evento_incendio
			--,num_evento_incendio
			--,ind_evento_vendaval
			--,num_evento_vendaval
			--,ind_evento_humdimiento
			--,num_evento_hundimiento
			,(case when tip_parentesco = '1' then 'Jefe del hogar'
				when tip_parentesco = '2' then 'C�nyuge o compa�ero(a)'
				when tip_parentesco = '3' then 'Hijo(a), hijastro(a), hijo(a) adoptivo(a)'
				when tip_parentesco = '4' then 'Nieto(a)'
				when tip_parentesco = '5' then 'Padre, madre, padrastro, madrastra'
				when tip_parentesco = '6' then 'Hermano(a)'
				when tip_parentesco = '7' then 'Yerno / Nuera'
				when tip_parentesco = '8' then 'Abuelo(a)'
				when tip_parentesco = '9' then 'Suegro(a)'
				when tip_parentesco = '10' then 'T�o(a)'
				when tip_parentesco = '11' then 'Sobrino(a)'
				when tip_parentesco = '12' then 'Primo(a)'
				when tip_parentesco = '13' then 'Cu�ado(a)'
				when tip_parentesco = '14' then 'Otro pariente'
				when tip_parentesco = '15' then 'Empleado(a) de servicio dom�stico'
				when tip_parentesco = '16' then 'Pariente del servicio dom�stico'
				when tip_parentesco = '17' then 'Pensionista'
				when tip_parentesco = '18' then 'Pariente de pensionista'
				when tip_parentesco = '19' then 'No pariente'
				else tip_parentesco
				end) tip_parentesco
			,(case when tip_estado_civil = '1' then 'Uni�n libre'
				when tip_estado_civil = '2' then 'Casado(a)'
				when tip_estado_civil = '3' then 'Viudo(a)'
				when tip_estado_civil = '4' then 'Separado(a) o divorciado(a)'
				when tip_estado_civil = '5' then 'Soltero(a)'
				else tip_estado_civil
				end) tip_estado_civil
			,ide_hogar
			,ide_informante
			,pri_nom_informante
			,seg_nom_informante
			,pri_ape_informante
			,seg_ape_informante
			--,ide_firma_informante
			,email_contacto
			,num_tel_contacto
			,(case when marca = '1' then 'Registro Valido'
				when marca = '2' then 'Excluido de publicaci�n'
				when marca = '3' then 'En verificaci�n - requiere iniciar tr�mite por parte del ciudadano'
				when marca = '4' then 'En verificaci�n - requiere actualizaci�n de encuesta para mejorar procesos de recolecci�n de informaci�n'
				else marca
				end) marca
			,estado
			,fec_actualizacion
		from colombiaseg_lote6.sisben_unificado
	);
	
	-- Creación de columna geográfica a tabla temporal
	alter table temp_sisben add column geom GEOMETRY(Point, 4686);
	
	/*
	Se actualiza el campo creado anteriormente (geom) y a partir de las columnas
	con los registros de posición, latitud y longitud de la tabla origen
	georreferenció.
	Para ello, primero se usa la función, postgis ST_MakePoint y a ello se le asigna el sistema
	de referencia MAGNA con ST_SetSRID.
	*/
	update temp_sisben set geom = ST_SetSRID(ST_MakePoint(coordenada_y, coordenada_x), 4686);
	
	-- Se transforman las coordendas al sistema de proyecci�n Origen-Nacional
	create table sisben_georreferenciado_lote_6 as
	(
		select cod_mpio codigo_municipio
			,cod_clase codigo_clase
			,tot_viviendas total_viviendas
			,pri_apellido primer_apellido
			,seg_apellido segundo_apellido
			,pri_nombre primer_nombre
			,sec_nombre segundo_nombre
			,tip_documento tipo_documento
			,num_documento numero_documento
			--,coordenada_x
			--,coordenada_y
			,cod_centro_poblado codigo_centro_poblado
			,nom_corregimiento nombre_corregimiento
			,nom_vereda nombre_vereda
			,dir_vivienda direccion_vivienda
			,tip_vivienda tipo_vivienda
			,tip_mat_paredes tipo_material_paredes
			,tip_mat_pisos tipo_material_pisos
			,num_cuartos_vivienda numero_cuartos_vivienda
			,num_hogares_vivienda numero_hogares_vivienda
			,tip_ocupa_vivienda tipo_ocupacion_vivienda
			,num_cuartos_exclusivos numero_cuartos_exclusivos
			,num_cuartos_dormir numero_cuartos_dormir
			,num_cuartos_unicos numero_cuartos_unicos
			,tip_santario tipo_sanitario
			,tip_ubi_sanitario tipo_ubicacion_sanitario
			,ind_tiene_cocina indice_tiene_cocina
			,tip_parentesco tipo_parentesco
			,tip_estado_civil tipo_estado_civil
			,ide_hogar identificador_hogar
			,ide_informante identificador_informante
			,pri_nom_informante primer_nombre_informante
			,seg_nom_informante segundo_nombre_informante
			,pri_ape_informante primer_apellido_informante
			,seg_ape_informante segundo_apellido_informante
			,email_contacto
			,num_tel_contacto numero_telefono_contacto
			,marca
			,estado
			,fec_actualizacion fecha_actualizacion
			,ST_Transform(geom, 9377) geom
		from temp_sisben
	);
	
	create table terrenos_sisben_lote_6 as
	(
		select distinct T1.codigo_municipio
			,T1.codigo_clase
			,T1.total_viviendas
			,T1.primer_apellido
			,T1.segundo_apellido
			,T1.primer_nombre
			,T1.segundo_nombre
			,T1.tipo_documento
			,T1.numero_documento
			,T1.codigo_centro_poblado
			,T1.nombre_corregimiento
			,T1.nombre_vereda
			,T1.direccion_vivienda
			,T1.tipo_vivienda
			,T1.tipo_material_paredes
			,T1.tipo_material_pisos
			,T1.numero_cuartos_vivienda
			,T1.numero_hogares_vivienda
			,T1.tipo_ocupacion_vivienda
			,T1.numero_cuartos_exclusivos
			,T1.numero_cuartos_dormir
			,T1.numero_cuartos_unicos
			,T1.tipo_sanitario
			,T1.indice_tiene_cocina
			,T1.tipo_parentesco
			,T1.tipo_estado_civil
			,T1.identificador_hogar
			,T1.identificador_informante
			,T1.primer_nombre_informante
			,T1.segundo_nombre_informante
			,T1.primer_apellido_informante
			,T1.segundo_apellido_informante
			,T1.email_contacto
			,T1.numero_telefono_contacto
			,T1.marca
			,T1.estado
			,T1.fecha_actualizacion
			,T1.geom
			,T2.predio_t_id
			,T2.predio_id_operacion
			,T2.predio_numero_predial
			,T2.terreno_t_id
			,T2.terreno_area_terreno
			,T2.codigo_unidad_intervencion
			,ST_Intersects(T1.geom, T2.geometria)
		from sisben_georreferenciado_lote_6 T1, colombiaseg_lote6.terrenos_digitalizados_lote_6 T2
		where ST_Intersects(T1.geom, T2.geometria) is true
	);
	
end;
$cuerpo_procedimiento$;