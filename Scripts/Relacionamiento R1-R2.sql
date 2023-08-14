-- Unificación R1 R2 El Paso
select distinct r1.*
	,r2.dpto r2_dpto
	,r2.mpio r2_mpio
	,r2.nopredial r2_nopredial
	,r2.tiporegistro r2_tiporegistro
	,r2.noorden r2_noorden
	,r2.totalregistro r2_totalregistro
	,r2.matriculainmobiliaria r2_matriculainmobiliaria
	,r2.areaterreno r2_areaterreno
	,r2.zonafisica_1 r2_zonafisica_1
	,r2.zonaeconomica_1 r2_zonaeconomica_1
	,r2.areaterreno_1 r2_areaterreno_1
	,r2.habitaciones r2_habitaciones
	,r2.banios r2_banios
	,r2.locales r2_locales
	,r2.pisos r2_pisos
	,r2.estrato r2_estrato
	,r2.uso r2_uso
	,r2.puntaje r2_puntaje
	,r2.areaconstruida r2_areaconstruida
	,r2.habitaciones_1 r2_habitaciones_1
	,r2.banios_1 r2_banios_1
	,r2.locales_1 r2_locales_1
	,r2.pisos_1 r2_pisos_1
	,r2.estrato_1 r2_estrato_1
	,r2.uso_1 r2_uso_1
	,r2.puntaje_1 r2_puntaje_1
	,r2.areaconstruida_1 r2_areaconstruida_1
	,r2.habitaciones_2 r2_habitaciones_2
	,r2.banios_2 r2_banios_2
	,r2.locales_2 r2_locales_2
	,r2.pisos_2 r2_pisos_2
	,r2.estrato_2 r2_estrato_2
	,r2.uso_2 r2_uso_2
	,r2.puntaje_2 r2_puntaje_2
	,r2.areaconstruida_2 r2_areaconstruida_2
	,r2.vigencia r2_vigencia
	,r2.nopredialanterior r2_nopredialanterior
into r1r2_elpaso
from public.r1_elpaso r1
left join public.r2_elpaso r2 on r1.nopredial = r2.nopredial;

-- Unificación R1 R2 San Onofre
select distinct r1.*
	,r2.dpto r2_dpto
	,r2.mpio r2_mpio
	,r2.nopredial  r2_nopredial
	,r2.tiporegistro r2_tiporegistro
	,r2.noorden r2_noorden
	,r2.totalregistro r2_totalregistro
	,r2.matriculainmobiliaria r2_matriculainmobiliaria
	,r2.areaterreno r2_areaterreno
	,r2.zonafisica_1 r2_zonafisica_1
	,r2.zonaeconomica_1 r2_zonaeconomica_1
	,r2.areaterreno_1 r2_areaterreno_1
	,r2.habitaciones r2_habitaciones
	,r2.banios r2_banios
	,r2.locales r2_locales
	,r2.pisos r2_pisos
	,r2.estrato r2_estrato
	,r2.uso r2_uso
	,r2.puntaje r2_puntaje
	,r2.areaconstruida r2_areaconstruida
	,r2.habitaciones_1 r2_habitaciones_1
	,r2.banios_1 r2_banios_1
	,r2.locales_1 r2_locales_1
	,r2.pisos_1 r2_pisos_1
	,r2.estrato_1 r2_estrato_1
	,r2.uso_1 r2_uso_1
	,r2.puntaje_1 r2_puntaje_1
	,r2.areaconstruida_1 r2_areaconstruida_1
	,r2.habitaciones_2 r2_habitaciones_2
	,r2.banios_2 r2_banios_2
	,r2.locales_2 r2_locales_2
	,r2.pisos_2 r2_pisos_2
	,r2.estrato_2 r2_estrato_2
	,r2.uso_2 r2_uso_2
	,r2.puntaje_2 r2_puntaje_2
	,r2.areaconstruida_2 r2_areaconstruida_2
	,r2.vigencia r2_vigencia
	,r2.nopredialanterior r2_nopredialanterior
into r1r2_sanonofre
from public.r1_sanonofre r1
left join public.r2_sanonofre r2
on r1.nopredial = r2.nopredial;

-- Unificación R1 R2 Toluviejo
select distinct r1.*
	,r2.dpto r2_dpto
	,r2.mpio r2_mpio
	,r2.nopredial r2_nopredial
	,r2.tiporegistro r2_tiporegistro
	,r2.noorden r2_noorden
	,r2.totalregistro r2_totalregistro
	,r2.matriculainmobiliaria r2_matriculainmobiliaria
	,r2.areaterreno r2_areaterreno
	,r2.zonafisica_1 r2_zonafisica_1
	,r2.zonaeconomica_1 r2_zonaeconomica_1
	,r2.areaterreno_1 r2_areaterreno_1
	,r2.habitaciones r2_habitaciones
	,r2.locales r2_locales
	,r2.pisos r2_pisos
	,r2.estrato r2_estrato
	,r2.uso r2_uso
	,r2.puntaje r2_puntaje
	,r2.areaconstruida r2_areaconstruida
	,r2.habitaciones_1 r2_habitaciones_1
	,r2.banios_1 r2_banios_1
	,r2.locales_1 r2_locales_1
	,r2.pisos_1 r2_pisos_1
	,r2.estrato_1 r2_estrato_1
	,r2.uso_1 r2_uso_1
	,r2.puntaje_1 r2_puntaje_1
	,r2.areaconstruida_1 r2_areaconstruida_1
	,r2.habitaciones_2 r2_habitaciones_2
	,r2.banios_2 r2_banios_2
	,r2.locales_2 r2_locales_2
	,r2.pisos_2 r2_pisos_2
	,r2.estrato_2 r2_estrato_2
	,r2.uso_2 r2_uso_2
	,r2.puntaje_2 r2_puntaje_2
	,r2.areaconstruida_2 r2_areaconstruida_2
	,r2.vigencia r2_vigencia
	,r2.nopredialanterior r2_nopredialanterior
into r1r2_toluviejo
from public.r1_toluviejo r1
left join public.r2_toluviejo r2
on r1.nopredial = r2.nopredial;

select * 
from public.predio_registro_w_derecho_elpaso

select *
from public.r1_elpaso

