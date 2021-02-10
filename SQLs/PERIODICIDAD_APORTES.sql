UPDATE S 
SET  
S.PERIODICIDAD_APORTE = T.PERIODICIDAD_APORTE,
S.DIA_DEBITO_AUTOMATICO_APORTE = T.DIA_DEBITO_AUTOMATICO_APORTE,
S.NRO_PRODUCTO_ORIGEN_APORTE = T.NRO_PRODUCTO_ORIGEN_APORTE,
S.MONTO_APORTE = T.MONTO_APORTE

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 	
		FI.periodicidad_debito_automatico AS PERIODICIDAD_APORTE, 
		FI.dia_inicio_debito  AS DIA_DEBITO_AUTOMATICO_APORTE, 
		FI.producto_origen AS NRO_PRODUCTO_ORIGEN_APORTE,
		FI.monto_aporte_periodico AS MONTO_APORTE,
		FI.nro_id_sesion
		
		FROM ${var:bd_cruda}.fic_traza_periodicidad_aportes_nrt FI
		WHERE FI.ID_APLICACION IN ('FIC')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.fic_traza_periodicidad_aportes_nrt T 
            WHERE T.id_aplicacion IN ('FIC')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;




