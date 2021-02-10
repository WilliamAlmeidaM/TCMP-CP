UPDATE S 
SET  
S.CORREO_ELECTRONICO_ENVIO_INFO_FONDO = T.CORREO_ELECTRONICO_ENVIO_INFO_FONDO

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 	

		FI.correo_electronico_cliente_ent AS CORREO_ELECTRONICO_ENVIO_INFO_FONDO,	 
		FI.nro_id_sesion
		
		FROM ${var:bd_cruda}.mbaas_notificacion_correo_nrt FI
		WHERE FI.ID_APLICACION IN ('FIC')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.mbaas_notificacion_correo_nrt T 
            WHERE T.id_aplicacion IN ('FIC')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;

