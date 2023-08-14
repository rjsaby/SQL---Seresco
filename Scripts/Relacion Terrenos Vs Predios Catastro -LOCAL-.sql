-- Procesamiento de datos temporal
-- Funcionará hasta tanto se visualicen en base de datos la información oficial y vigente de los terrenos digitalizados


-- ********************** PREDIOS, TERRENOS, MÁS DE UN PREDIO EN TERRENO **********************
-- Export de BD MapHurricane
select *
from SERESCO_Lote_6.dbo.npn_terreno;

-- Export de BD MapHurricane
select *
from SERESCO_Lote_6.dbo.numero_predios_por_terreno;

-- Export de BD MapHurricane
select *
from SERESCO_Lote_6.dbo.ua_predio;

drop table if exists #ue_terreno_con_npn_nuevo_elpaso;

-- Export de BD MapHurricane
-- Se crea tabla ue_terreno_con_npn_nuevo_elpaso
select ueterreno.*
	,uapredio.numero_predial
--into #ue_terreno_con_npn_nuevo_elpaso
from rel_uepredio reluepredio
left join ua_predio uapredio on reluepredio.id_predio = uapredio.t_id
left join SERESCO_Lote_6.dbo.UE_TERRENOS_MAPHURRICANE_SERLADM ueterreno on reluepredio.id_terreno = ueterreno.t_id
where substring(ueterreno.local_id,1,5) = '20250'

-- Espacialización de tabla con terrenos y conteo de predios
drop table if exists predios_por_terreno_elpaso;

select numeroprediosporterreno.npn_terreno
	,numeroprediosporterreno.predios_en_terreno
	,ueterrenoconnpnnuevo.t_id
	,ueterrenoconnpnnuevo.area_terre area_terreno
	,ueterrenoconnpnnuevo.id_dimensi id_dimension
	,ueterrenoconnpnnuevo.id_relacio id_relacion
	,ueterrenoconnpnnuevo.local_id
	,ueterrenoconnpnnuevo.id_direcci id_direccion
	,ueterrenoconnpnnuevo.avaluo_ter avaluo_terreno
	,ueterrenoconnpnnuevo.Shape
	,ueterrenoconnpnnuevo.numero_predial
into predios_por_terreno_elpaso
from numero_predios_por_terreno numeroprediosporterreno
left join #ue_terreno_con_npn_nuevo_elpaso ueterrenoconnpnnuevo
on numeroprediosporterreno.npn_terreno = ueterrenoconnpnnuevo.numero_predial
where ueterrenoconnpnnuevo.numero_predial is not null

-- ********************** ESPAÑA: CRUCE GC\SNR W DERIVADOS **********************

drop table if exists terreno_vs_derivado;

select distinct concat(substring([Número predial],1,21),'000000000') numero_predial
	,(case when Tiene_Derivados = '#N/A' then 'No reporta'
		when Tiene_Derivados = 'Tiene_Derivado' then 'Con derivado'
		else Tiene_Derivados
		end) derivados
into terreno_vs_derivado
from SERESCO_Lote_6.dbo.[espania_cruce_gc_snr_derivados.csv]

-- ********************** INFORMALIDAD **********************
drop table if exists terrenos_vs_informalidad;

select *
into terrenos_vs_informalidad
from npn_terreno
where condicion_predio <> '0'
