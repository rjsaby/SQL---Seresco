select *
from public.predio_registro_w_derecho_elpaso

-- Cruce: Tipo 1 - FMI SNR - Matricula Inmobiliaria IGAC ; Número Predial IGAC - Número predial SNR ; Número predial Anterior IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
   -- Número Predial Nuevo
	and (gc.numero_predial = snr.numero_predial_nuevo_en_fmi)
   -- Número Predial Anterior
	and (gc.numero_predial_anterior = snr.numero_predial_anterior_en_fmi)

-- Cruce: Tipo 2 - FMI SNR - Matricula Inmobiliaria IGAC ; Número Predial IGAC - Número predial SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
   -- Número Predial Nuevo
	and (gc.numero_predial = snr.numero_predial_nuevo_en_fmi)

-- Cruce: Tipo 3 - FMI SNR - Matricula Inmobiliaria IGAC ; Número predial Anterior IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
   -- Número Predial Anterior
	and (gc.numero_predial_anterior = snr.numero_predial_anterior_en_fmi)
	
-- Cruce: Tipo 4 - FMI SNR - Matricula Inmobiliaria IGAC ; Número Predial IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
   -- Número Predial Nuevo
	and (gc.numero_predial = snr.numero_predial_anterior_en_fmi)
	
-- Cruce: Tipo 5 - FMI SNR - Matricula Inmobiliaria IGAC ; Número predial Anterior IGAC - Número predial SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
	and (gc.numero_predial_anterior = snr.numero_predial_nuevo_en_fmi)	

-- Cruce: Tipo 6 - Número Predial IGAC - Número predial SNR ; Número predial Anterior IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on  -- Número Predial Nuevo
	(gc.numero_predial = snr.numero_predial_nuevo_en_fmi)
   -- Número Predial Anterior
	and (gc.numero_predial_anterior = snr.numero_predial_anterior_en_fmi)
	
-- Cruce: Tipo 7 - Número Predial IGAC - Número predial SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on  -- Número Predial Nuevo
	(gc.numero_predial = snr.numero_predial_nuevo_en_fmi)

-- Cruce: Tipo 8 - Número predial Anterior IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on  -- Número Predial Anterior
	(gc.numero_predial_anterior = snr.numero_predial_anterior_en_fmi)
	
-- Cruce: Tipo 9 - Número Predial IGAC - Número predial Anterior SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on  -- Número Predial Nuevo
	(gc.numero_predial = snr.numero_predial_anterior_en_fmi)

-- Cruce: Tipo 10 - Número predial Anterior IGAC - Número predial SNR
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on  -- Número Predial Anterior
	(gc.numero_predial_anterior = snr.numero_predial_nuevo_en_fmi)
	
-- Cruce: Tipo 11 - FMI SNR - Matricula Inmobiliaria IGAC
select gc.numero_predial
	,gc.numero_predial_anterior
	,gc.circulo_registral
	,gc.matricula_inmobiliaria_catastro
	,snr.numero_predial_nuevo_en_fmi 
	,snr.numero_predial_anterior_en_fmi
	,snr.codigo_orip
	,snr.matricula_inmobiliaria 
from public.predio_ladmelpaso gc
inner join public.predio_registro_w_derecho_elpaso snr
on -- Folio Matricula
	(concat(gc.circulo_registral,gc.matricula_inmobiliaria_catastro) = concat(snr.codigo_orip,snr.matricula_inmobiliaria))	
	
	
select *
from ladmelpaso.ini_emparejamientotipo