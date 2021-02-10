INSERT INTO   ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt 
( 
	PERIODO, COD_APLICACION_ORIGEN, DESCRIPCION_APLICACION_ORIGEN, rgst_nro_solicitud, 
	DIRECCION_IP_DISPOSITIVO, NRO_IDENTIFICACION_CLIENTE, COD_TIPO_IDENTIFICACION_CLIENTE, FECHA_SOLICITUD,
	HORA_SOLICITUD, FECHA_PROCESO, HORA_PROCESO, FECHA_APERTURA, 
	FECHA_CREACION, HORA_CREACION, FECHA_ACTLZCN, HORA_ACTLZCN,
	FLG_REALIZA_PERFILAMIENTO,FLG_PROGRAMACION_APORTES_FUTUROS
)

SELECT  

OPE.PERIODO AS PERIODO, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN OPE.cod_canal END) AS COD_APLICACION_ORIGEN,

MAX(CASE 
WHEN OPE.cod_servicio='ConsultaClientePNV2' AND OPE.cod_canal = '16' AND OPE.nro_id_terminal = '4545' THEN 'Davivienda.com' 
WHEN OPE.cod_servicio='ConsultaClientePNV2' AND OPE.cod_canal = '37' AND OPE.nro_id_terminal = '4884' THEN 'App Davivienda' 
WHEN OPE.cod_servicio='ConsultaClientePNV2' AND OPE.cod_canal = '37' AND OPE.nro_id_terminal = '4900' THEN 'App Inversores' 
ELSE concat('NO DEFINIDO ', OPE.cod_canal, ' y ', OPE.nro_id_terminal)
END) AS DESCRIPCION_APLICACION_ORIGEN, 

OPE.nro_id_sesion  AS rgst_nro_solicitud, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN OPE.direccion_ip END) AS DIRECCION_IP_DISPOSITIVO, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN OPE.nro_identificacion END) AS NRO_IDENTIFICACION_CLIENTE, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN OPE.tipo_identificacion END)  AS COD_TIPO_IDENTIFICACION_CLIENTE, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN  TRUNC(OPE.FECHA_HORA_OPERACION, 'dd') END) AS FECHA_SOLICITUD, 

MAX(CASE WHEN OPE.cod_servicio='ConsultaClientePNV2' THEN  CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING) END) AS HORA_SOLICITUD, 

MAX(CASE WHEN OPE.cod_servicio='ProcesoActual' THEN  TRUNC(OPE.FECHA_HORA_OPERACION, 'dd') END) AS FECHA_PROCESO, 

MAX(CASE WHEN OPE.cod_servicio='ProcesoActual' THEN CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING) END) AS HORA_PROCESO, 

MAX(CASE WHEN OPE.cod_servicio='AperturaProductosBancaPatrimonial' THEN TRUNC(OPE.FECHA_HORA_OPERACION, 'dd')  END) AS FECHA_APERTURA, 

MAX(CASE WHEN OPE.cod_servicio='AperturaProductosBancaPatrimonial' THEN TRUNC(OPE.FECHA_HORA_OPERACION, 'dd') END) AS FECHA_CREACION,

MAX(CASE WHEN OPE.cod_servicio='AperturaProductosBancaPatrimonial' THEN CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING)  END) AS HORA_CREACION, 

MAX(CASE WHEN OPE.cod_servicio='AperturaProductosBancaPatrimonial' THEN TRUNC(OPE.FECHA_HORA_OPERACION, 'dd')  END) AS FECHA_ACTLZCN, 

MAX(CASE WHEN OPE.cod_servicio='AperturaProductosBancaPatrimonial' THEN CAST(from_timestamp(OPE.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING)  END) AS HORA_ACTLZCN, 

'No'  AS FLG_REALIZA_PERFILAMIENTO,

MAX(CASE WHEN OPE.cod_servicio='PeriodicidadAportes' THEN 'Si' ELSE 'No' END) AS FLG_PROGRAMACION_APORTES_FUTUROS

FROM ${var:bd_cruda}.mbaas_log_operacional OPE

WHERE OPE.ID_APLICACION IN ('FIC')    
AND OPE.PERIODO  = ${var:periodo_inicial}
GROUP BY  OPE.PERIODO,  OPE.NRO_ID_SESION ;
 
