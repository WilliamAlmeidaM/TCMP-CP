UPDATE S 
SET  S.servicios = T.SERVICIOS 
FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
    SELECT GROUP_CONCAT(CONCAT(SERVICIO, '(',  CAST(CANTIDAD AS STRING), ')') ) SERVICIOS, NRO_ID_SESION
    FROM (
            SELECT COD_SERVICIO SERVICIO, COUNT(3)  AS  CANTIDAD, OPE.NRO_ID_SESION    
            FROM ${var:bd_cruda}.mbaas_log_operacional OPE 
            WHERE OPE.ID_APLICACION IN ('FIC','EPR')    
            AND PERIODO  = ${var:periodo_inicial} 
            GROUP BY COD_SERVICIO, NRO_ID_SESION 
            ) SERVICIOS

     GROUP BY SERVICIOS.NRO_ID_SESION
        
    ) T 

    ON S.rgst_nro_solicitud = T.nro_id_sesion;


UPDATE S 
SET  S.terminos_condiciones = T.DOCUMENTOS 
FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
    SELECT GROUP_CONCAT(CONCAT(DOCUMENTO, '(',  CAST(CANTIDAD AS STRING), ')') ) DOCUMENTOS, NRO_ID_SESION
    FROM (
            SELECT tipo_documento DOCUMENTO, COUNT(3)  AS  CANTIDAD, OPE.NRO_ID_SESION    
            FROM ${var:bd_cruda}.mbaas_log_juridico OPE 
            WHERE OPE.ID_APLICACION IN ('FIC','EPR')    
            AND PERIODO  = ${var:periodo_inicial}
            GROUP BY tipo_documento, NRO_ID_SESION 
        ) SERVICIOS

    GROUP BY SERVICIOS.NRO_ID_SESION
    ) T 

    ON S.rgst_nro_solicitud = T.nro_id_sesion;    