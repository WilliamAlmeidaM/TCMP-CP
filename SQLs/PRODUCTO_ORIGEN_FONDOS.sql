UPDATE S 
SET  
S.NRO_PRODUCTO_ORIGEN = T.NRO_PRODUCTO_ORIGEN,
S.FLG_PRODUCTO_CON_FONDOS = T.FLG_PRODUCTO_CON_FONDOS,
S.FLG_ORIGEN_OTROS_BANCOS = T.FLG_ORIGEN_OTROS_BANCOS,
S.NOMBRE_BANCO_ORIGEN = T.NOMBRE_BANCO_ORIGEN,
S.MONTO_INVERSION_INICIAL = T.MONTO_INVERSION_INICIAL,
S.COD_AGENTE_VENDEDOR = T.COD_AGENTE_VENDEDOR

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 	
		FI.nro_producto_origen AS NRO_PRODUCTO_ORIGEN, 
		CASE WHEN FI.flg_tiene_saldo = 'S'  THEN 'Si' ELSE 'No' END AS FLG_PRODUCTO_CON_FONDOS,
		CASE WHEN FI.flg_pse_otro_banco = 'S'  THEN 'Si' ELSE 'No' END AS FLG_ORIGEN_OTROS_BANCOS,
		FI.banco_origen AS NOMBRE_BANCO_ORIGEN, 
		FI.monto_aporte_inicial AS MONTO_INVERSION_INICIAL,
		FI.cod_agente_vendedor AS COD_AGENTE_VENDEDOR,
		FI.nro_id_sesion
		
		FROM ${var:bd_cruda}.fic_traza_producto_origen_fondos_nrt FI
		WHERE FI.ID_APLICACION IN ('FIC')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.fic_traza_producto_origen_fondos_nrt T 
            WHERE T.id_aplicacion IN ('FIC')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;


