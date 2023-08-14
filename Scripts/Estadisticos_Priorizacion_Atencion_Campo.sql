-- ************************************* DERIVADOS *************************************

select distinct T1.folio_matricula
from
(
	select *
	from public.folio_matriz_folio_derivado_lote_6
	-- where folio_matricula = '340-39260'
	-- where folio_derivado = '340-101211'
) T1;
-- 2672 Folios Derivados
-- 8466 Folios Derivados

-- El Paso
select distinct T1.predio_t_id
from
(
	select distinct predio_t_id
		,predio_numero_predial
		--,terreno_t_id
		--,codigo_unidad_intervencion
		,folio_matricula
		,folio_derivado
		,numero_derivados_x_folio_matriz
	from public.vw_terrenos_folio_matriz_folio_derivado_lote_6
	where substring(predio_numero_predial, 1, 5) = '20250'
) T1;
-- 201 Predios Matrices
-- 920 Predios Derivados

-- San Onofre
select distinct T1.predio_t_id
	,T1.predio_numero_predial
	,T1.folio_matricula
from
(
	select distinct predio_t_id
		,predio_numero_predial
		--,terreno_t_id
		--,codigo_unidad_intervencion
		,folio_matricula
		,folio_derivado
		,numero_derivados_x_folio_matriz
	from public.vw_terrenos_folio_matriz_folio_derivado_lote_6
	where substring(predio_numero_predial, 1, 5) = '70713'
) T1
where T1.folio_matricula = '340-39260'
;
-- 1472 Predios Matrices
-- 51258 Predios Derivados

-- Toluviejo
select distinct T1.predio_t_id
from
(
	select predio_t_id
			,predio_numero_predial
			--,terreno_t_id
			--,codigo_unidad_intervencion
			,folio_matricula
			,folio_derivado
			,numero_derivados_x_folio_matriz
	from public.vw_terrenos_folio_matriz_folio_derivado_lote_6
	where substring(predio_numero_predial, 1, 5) = '70823'
) T1;
-- 512 Predios Matrices
-- 4210 Predios Derivados

-- ************************************* SALDOS DE CONSERVACIÓN *************************************

select * from public.saldos_conservacion_lote_6;

-- El Paso
select * 
from public.vw_terrenos_saldos_conservacion_lote_6
where substring(predio_numero_predial, 1, 5) = '20250';
-- Total: 123

-- San Onofre
select * 
from public.vw_terrenos_saldos_conservacion_lote_6
where substring(predio_numero_predial, 1, 5) = '70713';
-- Total: 13

-- Toluviejo
select * 
from public.vw_terrenos_saldos_conservacion_lote_6
where substring(predio_numero_predial, 1, 5) = '70823';
-- Total: 17









