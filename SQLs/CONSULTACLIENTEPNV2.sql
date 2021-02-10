UPDATE S 
SET  
S.NRO_CELULAR_CLIENTE = T.NRO_CELULAR_CLIENTE, 
S.NOMBRE_CLIENTE = T.NOMBRE_CLIENTE,
S.PRIMER_APELLIDO_CLIENTE = T.PRIMER_APELLIDO_CLIENTE,
S.SEGUNDO_APELLIDO_CLIENTE = T.SEGUNDO_APELLIDO_CLIENTE, 
S.CORREO_ELECTRONICO_CLIENTE = T.CORREO_ELECTRONICO_CLIENTE


FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 

		CASE 
		WHEN find_in_set('Y',translate(FI.flg_celular_principal_sal,'[] ','')) > 0
		THEN CASE when SPLIT_PART(translate(FI.flg_activo_celular_sal,'[] ',''),',',find_in_set('Y',translate(FI.flg_celular_principal_sal,'[] ',''))) = 'Y'
			 THEN SPLIT_PART(translate(FI.nro_celular_sal,'[] ',''),',',find_in_set('Y',translate(FI.flg_celular_principal_sal,'[] ','')))
			 END
		ELSE translate(FI.flg_celular_principal_sal,'[] ','')
		END AS NRO_CELULAR_CLIENTE,

	    FI.nombre_cliente_sal AS NOMBRE_CLIENTE,

	    FI.primer_apellido_cliente_sal AS PRIMER_APELLIDO_CLIENTE,

	    FI.segundo_apellido_cliente_sal AS SEGUNDO_APELLIDO_CLIENTE,
	   
		CASE
		WHEN find_in_set('Y',translate(FI.flg_correo_electronico_principal_sal,'[] ','')) > 0  
		THEN CASE WHEN SPLIT_PART(translate(FI.cod_estado_correo_electronico_sal,'[] ',''),',',find_in_set('Y',translate(FI.flg_correo_electronico_principal_sal,'[] ',''))) = 'ACTIVA'
			 THEN SPLIT_PART(translate(FI.correo_electronico_sal,'[] ',''),',',find_in_set('Y',translate(FI.flg_correo_electronico_principal_sal,'[] ','')))
			 END 
		ELSE translate(FI.flg_correo_electronico_principal_sal,'[] ','')
		END AS CORREO_ELECTRONICO_CLIENTE,
				
		FI.nro_id_sesion

		FROM ${var:bd_cruda}.mbaas_consulta_cliente_pn_1_nrt FI  
		WHERE ID_APLICACION IN ('FIC')
		AND PERIODO = ${var:periodo_inicial}

	 --Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.mbaas_consulta_cliente_pn_1_nrt T 
            WHERE T.id_aplicacion IN ('FIC')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  
ON S.rgst_nro_solicitud = T.nro_id_sesion;

