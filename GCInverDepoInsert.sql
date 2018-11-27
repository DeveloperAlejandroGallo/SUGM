if exists(select * from sysobjects where type = 'p' and name = 'GCInverDepoInsert') 
 begin 
     drop procedure GCInverDepoInsert
 end 

 go 
 
CREATE PROCEDURE GCInverDepoInsert
(
	@idSocGte int,
	@trama varchar(500),
	@AltaReproceso char(1) = 'A',
	@idReproceso int = null
)
as
begin

Declare
	@_CodigoAuditoria		varchar(15),	--int 
	@CodigoAuditoria        bigint,			--1
	@IdSalida				varchar(3),		--2
	@Accion					char(1),		--3 -A,B,M,R
	@CodigoDepositario		varchar(30),	--4-Sociedades Depositarias
	@IdentificadorEspecie	varchar(30),	--5
	@CodigoISIN				varchar(30),	--6
	@CodigoCajaValoresCAFCI varchar(15),	--7
	@CodCAFCIMonedaConcert	varchar(3),		--8
	@_IdFondoDepositaria	varchar(10),	--int
	@IdFondoDepositaria     int,			--9
	@_FechaConcertacion		varchar(8),		--date
	@FechaConcertacion	    date,			--10
	@_FechaLiquidacion		varchar(8),		--date
	@FechaLiquidacion       date,			--11
	@_ValorNominal			varchar(23),	--decimal(14,8)
	@ValorNominal			decimal(22,8),	--12
	@_ImporteNeto			varchar(20),	--decimal(17,2)
	@ImporteNeto			decimal(19,2),	--13
	@CAFCIMonedaGastos		varchar(3),		--14
	@_ImporteGastos			varchar(20),	--decimal(17,2)
	@ImporteGastos			decimal(19,2),	--15          
	@_ImporteBruto			varchar(20),	--decimal(17,2)
	@ImporteBruto			decimal(19,2),	--16
	@_Tasa					varchar(20),	--decimal(17,2)
	@Tasa					decimal(19,2),	--17
	@EntidadLiquidacion		varchar(15),	--18
	@_EsTransferible		varchar(2),		--int -1 => SI
	@EsTransferible			int,			--19 
	@_EsPrecancelable		varchar(2),		--int -1 => SI
	@EsPrecancelable		int,			--20
	@CAFCIMercado			varchar(3),		--21
	@AbreviaCHQ_Pagare		varchar(15),	--22
	@_FechaVto				varchar(8),		--date
	@FechaVto				date,			--23
	@_CodAuditOperRealacion	varchar(15),	--bigint
	@CodAuditOperRealacion	bigint,			--24
	@CodigoTipoOperacion	varchar(6),		--25
	@CodigoInterfazAgente	varchar(15),	--26
	@_FechaAlta				varchar(8),		-- date
	@FechaAlta				date,			--27
	@_FechaActualizacion	varchar(8),		-- date
	@FechaActualizacion		date,			--28
	@_FechaProceso			varchar(8),		-- date
	@FechaProceso			date,			--29
	@CodTipoPapelInstrumento varchar(15),	--30
	@_FechaPrecancelacion	varchar(8),		-- date
	@FechaPrecancelacion	date,			--31
	@CBU_Broker				varchar(22),	--32
	@CUIT_Broker			varchar(11),	--33
	@Depositante			varchar(30),	--34
	@Comitente				varchar(30),	--35
	--
	@error	int =0,
	@errorMsg varchar(4000) = '',
	@esRepetido int = 0,
	--+ Para la transaccion
	@clie_alias varchar(5),
	@TipoInstruccion int,
	@Referencia varchar(16),
	@espe_codigo varchar(6),
	@Cantidad money = NULL,  
	@ccus_id int = NULL,  
	@CasaCustodia int = NULL,  
	@ContraparteDepositante varchar(100) = NULL,  
	@ContraparteComitente varchar(100) = NULL,  
	@espe_codcot varchar(6) = NULL,  
	@MontoLiquidacion money = NULL,  
	@Observaciones varchar(100) = NULL, 
	@Precancelable int, 
	@TipoPlazoFijo int = NULL,  
	@BancoEmisor int = NULL,  
	@Intereses money = NULL,  
	@TNA float = NULL,  
	@NroCliente varchar(100) = NULL,  
	@CuentaDepositante varchar(100) = NULL,  
	@NroLote varchar(100) = NULL,  
	@TipoOtro int = NULL,  
	@Concepto varchar(100) = NULL,  
	@Motivo varchar(100) = NULL,  
	@GeneraMensaje int = NULL,  
	@CodigoMatching int = NULL,  
	@ClienteInterno int = NULL ,  
	@Euclid varchar(100) = NULL,  
	@UsuarioAlta varchar(3)= NULL,  
	@color int = 0,
	@out int = 1,  
	@errorInstruccion int,  
	@err_msgInstruccion varchar(1000),
	---
	@Numero int = null,
	@IDInstr_out int,
	@tramaAnterior varchar(500),
	@tramaUltima varchar(500),
	@camposModificados varchar(4000),
	@ultimoRegistro int,
	@Validada int = 0,
	@idInverDepo int = null,
	@fechaDia datetime = getdate(),
	@fechaDiaHabilxAnt datetime = null,
	@nroInstruccion int,
	@CodInstruccionSGM varchar(4),
	@Alerta int = 0,
	@instruccionManAuto char(1) = '',
	---Especies----
	@EquivalenciaCAFCI	varchar(30) = null,
	@EquivalenciaCV		varchar(30) = null,
	@EquivalenciaISIN	varchar(30) = null,
	--Tipo Operacion ---
	@tipoOperDescripcion varchar(50) = '',
	--ISIN Duplicado---
	@cantidadEspecies int = 0,
	@especieEncontrada varchar(20) = null,
	@espe_codsgm varchar(20)

--Parseo de los campos de la trama.
	set	@_CodigoAuditoria		= substring(@trama,1,15)   
	
	print '01 - Codigo Auditoria ('+@_CodigoAuditoria+')'
	if(isnumeric(@_CodigoAuditoria) = 0 or @_CodigoAuditoria is null)
	begin
		set @error = 1
		set @errorMsg = @errorMsg + '(E)El codigo de auditoria no es numerico.' + '*'
		GOTO FIN
	end
	else
		set @CodigoAuditoria = convert(bigint, @_CodigoAuditoria)
	
	if(LEN(@trama) = 481)
	begin

		--set	@_CodigoAuditoria		= substring(@trama,1,15)   
		set	@IdSalida				= substring(@trama,16,3)
		set	@Accion					= substring(@trama,19,1)
		set	@CodigoDepositario		= substring(@trama,20,30)
		set	@IdentificadorEspecie	= substring(@trama,50,30)
		set	@CodigoISIN				= substring(@trama,80,30)
		set	@CodigoCajaValoresCAFCI = substring(@trama,110,15)
		set	@CodCAFCIMonedaConcert	= substring(@trama,125,3)
		set	@_IdFondoDepositaria	= substring(@trama,128,10)
		set	@_FechaConcertacion		= substring(@trama,138,8)
		set	@_FechaLiquidacion		= substring(@trama,146,8)
		set	@_ValorNominal			= substring(@trama,154,23)
		set	@_ImporteNeto			= substring(@trama,177,20)
		set	@CAFCIMonedaGastos		= substring(@trama,197,3)
		set	@_ImporteGastos			= substring(@trama,200,20)
		set	@_ImporteBruto			= substring(@trama,220,20)
		set	@_Tasa					= substring(@trama,240,20)
		set	@EntidadLiquidacion		= substring(@trama,260,15)
		set	@_EsTransferible		= substring(@trama,275,2)
		set	@_EsPrecancelable		= substring(@trama,277,2)
		set	@CAFCIMercado			= substring(@trama,279,3)
		set	@AbreviaCHQ_Pagare		= substring(@trama,282,15)
		set	@_FechaVto				= substring(@trama,297,8)
		set	@_CodAuditOperRealacion	= substring(@trama,305,15)
		set	@CodigoTipoOperacion	= substring(@trama,320,6)
		set	@CodigoInterfazAgente	= substring(@trama,326,15)
		set	@_FechaAlta				= substring(@trama,341,8)
		set	@_FechaActualizacion	= substring(@trama,349,8)
		set	@_FechaProceso			= substring(@trama,357,8)
		set	@CodTipoPapelInstrumento= substring(@trama,365,15)
		set	@_FechaPrecancelacion	= substring(@trama,380,8)
		set	@CBU_Broker				= substring(@trama,388,22)
		set	@CUIT_Broker			= substring(@trama,410,11)
		set	@Depositante			= substring(@trama,421,30)
		set	@Comitente				= substring(@trama,451,30)
		
		--VALIDACIONES
		print 'id Soc Gte' + '(' + convert(varchar(10),@idSocGte) + ')'
		if(not exists (select sger_id from sociedades_gerente where sger_id = @idSocGte))
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)El id Sociedad Gerente no existe. (' +convert(varchar(10),@idSocGte) +')'+ '*'
			--GOTO INSERTAR
		end
		
		Print 'Valido Previa existencia'
		set @Validada = 0
		set @color = 0
		set @ultimoRegistro = 1

		if(@AltaReproceso != 'R')
		begin

			if(exists (select 1 from GCInversionesDepositaria g where @CodigoAuditoria = g.CodigoAuditoria and @idSocGte = g.idSociedadGerente))
			begin
			--Ya existe uno. Se comparan las tramas del que esta marcado como ultimo registro y el ultimo registro ingresado contra la trama nueva
				Print 'Ya existe uno'
				--Ultimo registro ingresado
				select top 1 @tramaUltima = trama 
				from GCInversionesDepositaria g
				where @CodigoAuditoria = g.CodigoAuditoria 
				  and @idSocGte = g.idSociedadGerente	
				order by g.id desc 

				if(@tramaUltima != @trama) -- Si la trama que vino es igual a la ultima que ingreso, no la tomo en cuenta.
				begin
					--Registro que manda 
					select 	@tramaAnterior = trama,
							@Validada = Validada,
							@nroInstruccion = NroInstruccion
					from GCInversionesDepositaria g
					where @CodigoAuditoria = g.CodigoAuditoria 
					  and @idSocGte = g.idSociedadGerente	
					  and g.UltimoRegistro = 1

					exec GCInverDepoComparaTramas @tramaAnterior, @trama, @camposModificados output
					Print 'Campos Modificados: ' + @camposModificados

					--Si no se modifico ningun campo, salteo el registro.
					if(@camposModificados is not null and ltrim(rtrim(@camposModificados)) != '')
					begin
			
						set @ultimoRegistro = 0
						set @Alerta = 2			-- Alerta en 2, Modifica un registro sin instrucción

						if(exists(select 1 from GCInversionesDepositaria g where @CodigoAuditoria = g.CodigoAuditoria 
						   and @idSocGte = g.idSociedadGerente and g.NroInstruccion is not null)) --No generar nueva instruccion si ya existe una con Nro.
						begin	
							set @error = 1
							set @Alerta = 1
							set @errorMsg = @errorMsg  + '(E)Modifica Registro con Instruccion.' + '*'
						end

					end
					else
					-- Si no hay cambio se descarta.
						GOTO FIN
				end
				else
					GOTO FIN 
			end
			else --No existe, pregunto si es baja y si lo es no la proceso.
			begin
				if(@Accion='B')
				BEGIN
					Print 'No se registra por ser baja de un alta inexistente'
					GOTO FIN 
				END
			end 
		end
		else
		begin
			if(@AltaReproceso = 'R' 
				and exists(select 1 from GCInversionesDepositaria g where @CodigoAuditoria = g.CodigoAuditoria 
				and @idSocGte = g.idSociedadGerente and g.NroInstruccion is not null)) --No generar nueva instruccion si ya existe una con Nro.
			begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)Ya existe instrucción para este Codigo de Auditoria. *'
			end 
			
		end

		print '02 - Accion ('+@Accion+')'
		if(@Accion is null or RTRIM(ltrim(@Accion)) = '' or 
		(@Accion != 'A' and @Accion != 'B' and @Accion != 'M'/* and @Accion != 'R'*/)) --No se recibira R
		begin
			set @errorMsg = @errorMsg + '(W)La accion no es valida' + '*'
		end		
		else
		begin
			if(@Accion='B')
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)No se genera instruccion por baja *'
			end
		end
			 		
		--Valido el codigo de tipo de opeacion previo a utilizarlos.
		set @CodigoTipoOperacion = LTRIM(rtrim(@CodigoTipoOperacion))
		print '25 - Codigo operacion - (' + @CodigoTipoOperacion +')'
		if( not exists (select id from GCInverDepoTipoOper where codigo = @CodigoTipoOperacion))
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)El código de operacion no es válido. (' +@CodigoTipoOperacion+')' + '*'
		end 
		else
		if(not exists (select id from GCInverDepoTipoOper where codigo = @CodigoTipoOperacion and InstruccionSGM is not null))
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)El cod operacion. (' +@CodigoTipoOperacion+') no genera instruccion' + '*'
		end	
		else
		begin
			select  @TipoInstruccion =  i.ID, @CodInstruccionSGM = o.InstruccionSGM
			from GCInverDepoTipoOper o
				inner join GCTiposInstrucciones i 
					on i.Alias = o.InstruccionSGM
			where codigo = @CodigoTipoOperacion
		
		end

		print '04 - Codigo depositario - (' + ltrim(rtrim(@CodigoDepositario)) +')' 
		
		if(@CodigoDepositario is null or @CodigoDepositario = '' )
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)Codigo de Depositario en agente custodia inválido. (' +ltrim(rtrim(@CodigoDepositario)) +')' + '*'
		end
		else if (not exists (select alias from GCCasasCustodia
			where estado = 1 and alias = ltrim(rtrim(@CodigoDepositario))
			and id in (select distinct entecustodia from tiposcuentas_custodia where cg = 1 )))
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)Codigo de Depositario no existe  ('+ltrim(rtrim(@CodigoDepositario))+')'+ '*'
			end
				else
				begin	
					set @CodigoDepositario=ltrim(rtrim(@CodigoDepositario))
					set @CasaCustodia = (select ID from  GCCasasCustodia where alias = @CodigoDepositario)
				end
		/*
		ESPECIE - Campo 5, 6 y 7. --
		Si es cheque o pf no se valida.
		
		Si es PF directamente se pone CGPFJ
		(CLCOMETII-79)
		Si campo 6 (ISIN) está informado, manda…         Busco especie  
			Si ISIN encontrado y campo 30 = ‘FCI’ y (campo 7 = equivalencia CAFCI  or campo 7=blanco or equivalencia CaFCI = blanco/NULL) => ok
			Si ISIN encontrado y campo 30 <> ‘FCI’ y (campo 7 = equivalencia CV  or campo 7=blanco or equivalencia CV = blanco/NULL)      => ok
            Si ISIN NO encontrado y y campo 30 = ‘FCI’ busco especie en código CAFCI
			Si ISIN NO encontrado y y campo 30 <> ‘FCI’ busco especie en código CV

		Si el campo 30 = 'FUTU' buscar la divisa en el campo 5.
		*/

		if(ltrim(rtrim(@CodTipoPapelInstrumento)) != 'FUTU') 
		begin
			if(@CodigoTipoOperacion not in ('CHDC','CHDV','PF', 'PFBDLR','PFPREC','PFTVAR')) --No es cheque ni PF
			begin

				select @cantidadEspecies = count(espe_codsgm) from dts_espe_convert where espe_canal = 'ISIN' and espe_codigo = @CodigoISIN

				if(@cantidadEspecies < 2) --Camino normal
				begin
				
					set @CodigoISIN = ltrim(rtrim(@CodigoISIN))
					set @CodigoCajaValoresCAFCI = isnull(REPLICATE('0',5-LEN(@CodigoCajaValoresCAFCI)),'') + isnull(ltrim(rtrim(@CodigoCajaValoresCAFCI)),'')
					set @CodigoCajaValoresCAFCI = case when @CodigoCajaValoresCAFCI = '00000' then null else @CodigoCajaValoresCAFCI end
					set	@CodTipoPapelInstrumento = ltrim(rtrim(@CodTipoPapelInstrumento)) 

					--Actualizar para que espe_codigo tenga ceros a la izq.
					select @EquivalenciaISIN  = ltrim(rtrim(espe_codSGM)) from dts_espe_convert where espe_canal = 'ISIN'   and espe_codigo = @CodigoISIN
					select @EquivalenciaCV    = ltrim(rtrim(espe_codSGM)) from dts_espe_convert where espe_canal = 'CAJVAL' and isnull(REPLICATE('0',5-LEN(ltrim(rtrim(espe_codigo)))),'') + isnull(ltrim(rtrim(espe_codigo)),'') = @CodigoCajaValoresCAFCI
					select @EquivalenciaCAFCI = ltrim(rtrim(espe_codSGM)) from dts_espe_convert where espe_canal = 'CAFCI'  and isnull(REPLICATE('0',5-LEN(ltrim(rtrim(espe_codigo)))),'') + isnull(ltrim(rtrim(espe_codigo)),'') = @CodigoCajaValoresCAFCI
																   

					print '06 - ISIN Code - (' + @CodigoISIN +')'
					print '07 - CAFCI CV -  (' + @CodigoCajaValoresCAFCI +')'
					if(@CodigoISIN is not null and @CodigoISIN != '') -- ISIN Informado
					begin
						if(@EquivalenciaISIN is not null ) --Equivalencia ISIN encontrada
						begin
							set @espe_codigo = @EquivalenciaISIN						
							if(@CodTipoPapelInstrumento = 'FCI') --Si es FCI debe coincidir el CAFCI si es informado
							begin						 			
								if(@CodigoCajaValoresCAFCI is not null or rtrim(ltrim(@CodigoCajaValoresCAFCI)) = '' or @CodigoCajaValoresCAFCI = '00000' ) 
								begin	
									if(@EquivalenciaCAFCI is not null )	
									begin			
										if(isnull(@EquivalenciaISIN,'') != @EquivalenciaCAFCI)
										begin 	
											set @error = 1
					 						set @errorMsg = @errorMsg + '(E)El código ISIN ('+isnull(@CodigoISIN,'')+') y CAFCI ('+isnull(@CodigoCajaValoresCAFCI,'')+') informados no pertenecen a las mismas especies .('+isnull(@EquivalenciaISIN,'')+') y ('+isnull(@EquivalenciaCAFCI,'')+') *'  							
										end
									end
									else
									begin 
										set @errorMsg = @errorMsg + '(W)Código CAFCI de Especie informado no existe.(' +@CodigoCajaValoresCAFCI+ ')*'		
									end
								end
							end
							else -- No es FCI Debe coincidir CV si es informado
							begin						
								if(@CodigoCajaValoresCAFCI is not null or rtrim(ltrim(@CodigoCajaValoresCAFCI)) = '' or @CodigoCajaValoresCAFCI = '00000' )
								begin 
									if(@EquivalenciaCV is not null )
									begin									
										if(isnull(@EquivalenciaISIN,'') != @EquivalenciaCV)
										begin									
											set @error = 1
					 						set @errorMsg = @errorMsg + '(E)El código ISIN ('+isnull(@CodigoISIN,'')+') y CV ('+isnull(@CodigoCajaValoresCAFCI,'')+') informados no pertenecen a las mismas especies.('+isnull(@EquivalenciaISIN,'')+') y ('+isnull(@EquivalenciaCV,'')+') *'							
										end	
									end
									else
									begin 
										set @errorMsg = @errorMsg + '(W)Código CV de Especie informado no existe.(' +@CodigoCajaValoresCAFCI+ ')*'		
									end
								end
							end
						end
						else -- Se informó algo en ISIN pero no esta  
						begin
							set @error = 1	
							set @errorMsg = @errorMsg + '(E)El código ISIN de Especie informado no existe.(' +@CodigoISIN +')*'	
						end
					end
					else --ISIN no informado
					begin
						if(@CodTipoPapelInstrumento = 'FCI')	
						begin
							if(@EquivalenciaCAFCI is null)
							begin	
								set @error = 1
					 			if(rtrim(ltrim(@CodigoCajaValoresCAFCI)) is null or rtrim(ltrim(@CodigoCajaValoresCAFCI)) = '' or @CodigoCajaValoresCAFCI = '00000')
									set @errorMsg = @errorMsg + '(E)Falta informacion de Especie.' + '*'
								else
									set @errorMsg = @errorMsg + '(E)Código CAFCI de Especie informado no existe.(' +@CodigoCajaValoresCAFCI+ ')*'	
							end
							else
								set @espe_codigo = @EquivalenciaCAFCI
						end
						else
						begin
							if(@EquivalenciaCV is null )
							begin	
								if(rtrim(ltrim(@CodigoCajaValoresCAFCI)) is null or rtrim(ltrim(@CodigoCajaValoresCAFCI)) = '' or @CodigoCajaValoresCAFCI = '00000') 
								begin
									set @IdentificadorEspecie = ltrim(rtrim(@IdentificadorEspecie))
									if(@IdentificadorEspecie is null or @IdentificadorEspecie = '')
									begin
										set @error = 1	
										set @errorMsg = @errorMsg + '(E)Falta informacion de Especie.*'	
									end
									else
									begin
										if(not exists (select espe_codSGM from dts_espe_convert where  espe_codSGM = @IdentificadorEspecie)) --Campo 5 igual a CodSGM
										begin
											set @error = 1	
											set @errorMsg = @errorMsg + '(E)El código de Identificador de Especie informado no existe.(' +@IdentificadorEspecie +')*'	
										end
										else
											set @espe_codigo =  @IdentificadorEspecie
									end
								end
								else
								begin
									set @error = 1	
									set @errorMsg = @errorMsg + '(E)Código CV de Especie informado no existe.('+@CodigoCajaValoresCAFCI+')*'	
								end
							end
							else
								set @espe_codigo = @EquivalenciaCV
						end
					end
				end -- no es CHQ ni PF
			end -- NO ES ISIN DUPLICADO
			else 
			begin --Logica para ISIN Duplicado
				if(@CodigoCajaValoresCAFCI is not null and rtrim(ltrim(@CodigoCajaValoresCAFCI)) != '')
				begin
					select @cantidadEspecies = count(espe_codigo) from dts_espe_convert where espe_canal = 'CAJVAL' and espe_codigo = @CodigoCajaValoresCAFCI

					if(@cantidadEspecies < 2)
					begin
						declare cEspecies cursor for
						select espe_codsgm from dts_espe_convert
						where espe_codigo = @CodigoISIN

						open cEspecies 
						fetch next from cEspecies into @espe_codsgm
						while @@FETCH_STATUS = 0
						begin

							select @especieEncontrada = espe_codsgm 
							from dts_espe_convert 
							where espe_codsgm = @espe_codsgm
							and espe_canal = 'CAJVAL'
							and espe_codigo = @CodigoCajaValoresCAFCI

							if(@especieEncontrada is not null)
								break

							fetch next from cEspecies into @espe_codsgm
						end
						close cEspecies
						deallocate cEspecies

						if(@especieEncontrada is not null)
						begin
							set @espe_codigo = @especieEncontrada
						end 
						else
						begin
							set @error = 1	
							set @errorMsg = @errorMsg + '(E)Codigo CV no encontrado para ISIN con mas de una especie asociada*'	
						end
					end
					else
					begin
						set @error = 1	
						set @errorMsg = @errorMsg + '(E)No es posible identificar un único codigo de especie. ISIN y CV duplicados*'	
					end
				end
				else
				begin
					set @error = 1	
					set @errorMsg = @errorMsg + '(E)Código CV de especie NO informado, para ISIN con mas de una especie asociada*'	
				end
			end
		end
		else
		begin
			--Es futuro y se busca con el campo 5
			set @IdentificadorEspecie = ltrim(rtrim(@IdentificadorEspecie))
			if(@IdentificadorEspecie is null or @IdentificadorEspecie = '')
			begin
				set @error = 1	
				set @errorMsg = @errorMsg + '(E)Debe informar el Identificador de Especie para Futuro.*'	
			end
			else
			begin
				if(not exists (select espe_codSGM from dts_espe_convert where espe_canal = 'CAFCI'  and espe_codigo = @IdentificadorEspecie))
				begin
					set @error = 1	
					set @errorMsg = @errorMsg + '(E)El código de Identificador de Especie informado no existe.(' +@IdentificadorEspecie +')*'	
				end
				else
					select @espe_codigo = espe_codSGM from dts_espe_convert where espe_canal = 'CAFCI'  and espe_codigo = @IdentificadorEspecie
			end
		end
		
		
		print '08 - Codigo CAFCI moneda Concertacion ('+right(REPLICATE(' ',3) + ltrim(rtrim(@CodCAFCIMonedaConcert)), 3)+')'
		if(@CodCAFCIMonedaConcert is null or @CodCAFCIMonedaConcert = '')
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)Codigo CAFCI Moneda concertación inválido.' + '*'
		end
		else
		begin
		 	if( not exists(
			select espe_codigo from dts_espe_convert
			where espe_canal = 'CAFCI'
			and ltrim(rtrim(espe_codigo)) = right(REPLICATE(' ',3) + ltrim(rtrim(@CodCAFCIMonedaConcert)), 3)))
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)Codigo CAFCI Moneda concertación no existe  (' +right(REPLICATE(' ',3) + ltrim(rtrim(@CodCAFCIMonedaConcert)), 3)+ ')'+ '*'
			end 	
			else
				select @espe_codcot =  espe_codsgm from dts_espe_convert
				where espe_canal = 'CAFCI' and ltrim(rtrim(espe_codigo)) = right(REPLICATE(' ',3) + ltrim(rtrim(@CodCAFCIMonedaConcert)), 3)
		end

		--No se analiza si es futuro o no para estas condiciones, ya que se asume que NO informaran FUTU en el campo 30 para Cheque y Plazo fijo.
		if(@CodigoTipoOperacion in ('CHDC','CHDV')) --Es Cheque
			set @espe_codigo = @espe_codcot--'CGCPD'

		if(@CodigoTipoOperacion in ('PF', 'PFBDLR','PFPREC','PFTVAR')) --Es Plazo fijo.
			set @espe_codigo = @espe_codcot--'CGPFJ' 


		print '09 - Identificador Fondo depositaria - Cliente - (' + convert(varchar,convert(int,right(@_IdFondoDepositaria,5))) +')'
		if(@_IdFondoDepositaria is null or @_IdFondoDepositaria = '' )
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)Codigo de Cliente inválido (' + convert(varchar,convert(int,right(@_IdFondoDepositaria,5))) +')' + '*'
		end
		else
		begin
			if(not exists (select clie_alias from clientes where clie_nrodoc = convert(varchar,convert(int,right(@_IdFondoDepositaria,5)))))
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)Codigo de Cliente no existe  (' + convert(varchar,convert(int,right(@_IdFondoDepositaria,5))) +')' + '*'
			end
			else 
			begin
				select @clie_alias = clie_alias from clientes where clie_nrodoc = convert(varchar,convert(int,right(@_IdFondoDepositaria,5)))
				set @IdFondoDepositaria = convert(varchar,convert(int,right(@_IdFondoDepositaria,5)))
			end
		end 


		/**********************/
		set @fechaDia=convert(varchar(8),@fechaDia,112)
		exec calc_fecha_xdiashabiles @fechaDia,1,1, @fechaDiaHabilxAnt output
		set @fechaDiaHabilxAnt = convert(varchar(8),@fechaDiaHabilxAnt,112)

		print '10 - Fecha Concertacion ('+@_FechaConcertacion+')'

		if(@_FechaConcertacion is null or @_FechaConcertacion = '' or ISDATE(@_FechaConcertacion) = 0)
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)La Fecha de concertación es inválida.('+@_FechaConcertacion+')' + '*'
		end
		else
		begin
			set @FechaConcertacion = CONVERT(date,@_FechaConcertacion)
			
			if @FechaConcertacion < @fechaDiaHabilxAnt 
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)La fecha de concertacion no es mayor o igual a dia habil anterior.('+@_FechaConcertacion+')' + '*'
			end		
		end


		print '11 - Fecha Liquidacion ('+@_FechaLiquidacion+')'
		if(@_FechaLiquidacion is null or @_FechaLiquidacion = '' or ISDATE(@_FechaLiquidacion) = 0)
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)La fecha de liquidación es inválida.('+@_FechaLiquidacion+')' + '*'
		end
		else
		begin
			set @FechaLiquidacion = CONVERT(date,@_FechaLiquidacion)
			
    		if @fechaLiquidacion < @fechaDiaHabilxAnt 
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)La fecha de liquidación no es mayor o igual a dia habil anterior.('+@_FechaLiquidacion+')' + '*'
			end			
		end

		
		print '12 - Valor Nominal - ' + @_ValorNominal
		if(ISNUMERIC(@_ValorNominal) = 0 )
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)El valor nominal no es numerico.' + '*'
		end
		else
			if (CHARINDEX(',',@_valornominal) > 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)El valor nominal tiene comas. (' +@_ValorNominal +')*'
			end
			else
				set @ValorNominal = CONVERT(decimal(22,8),@_ValorNominal) 
		
		print '13 - Importe Neto - (' + @_ImporteNeto + ')'
		if(ISNUMERIC(@_ImporteNeto) = 0 )
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)El importe neto no es numerico.' + '*'
		end
		else
			if(CHARINDEX(',',@_ImporteNeto) > 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)El importe neto tiene comas(,). (' +@_ImporteNeto +')*'
			end
			else
				set @ImporteNeto = CONVERT(decimal(19,2),@_ImporteNeto)

		--14 - Codigo CAFCI de la moneda de Gastos (Informativo)
		
		print '15 - Importe gastos - (' + @_ImporteGastos +')'
		if(ISNUMERIC(@_ImporteGastos) = 0 and @_ImporteGastos != '')
		begin
			set @errorMsg = @errorMsg + '(W)El importe gastos no es numerico.' + '*'
		end
		else
		begin
			if(CHARINDEX(',',@_ImporteGastos) > 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)El importe gastos tiene comas(,). (' +@_ImporteGastos +')*'
			end
			else
				set @ImporteGastos = CONVERT(decimal(19,2),@_ImporteGastos)
		end	
		
		-- 16 importe bruto
		print '16 - Importe Bruto - (' + @_ImporteBruto +')'
		if(ISNUMERIC(@_ImporteBruto) = 0 and @_ImporteBruto != '')
		begin
			set @errorMsg = @errorMsg + '(W)El importe bruto no es numerico.' + '*'
		end
		else
		begin
			if(CHARINDEX(',',@_ImporteBruto) > 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)El importe bruto tiene comas(,). ('+@_ImporteBruto+ ')*'
			end
			else
				set @ImporteBruto = CONVERT(decimal(19,2),@_ImporteBruto)
		end	
			
		
		-- 17 tasa
		print '17 - Tasa - (' + @_Tasa +')'
		if(@CodigoTipoOperacion in ('PF','PFBDLR','PFPREC','PFTVAR','PCC','PCF','CCC','CCF','APCC','APCF') )
		begin
			if(ISNUMERIC(@_Tasa) = 0 and @_Tasa != '' and @_Tasa is not null)
			begin
				set @errorMsg = @errorMsg + '(W)El tasa no es numerica.' + '*'
			end
			else
			begin
				if(CHARINDEX(',',@_Tasa) > 0)
				begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)La Tasa tiene comas(,) (.'+@_Tasa + ')*'
				end
				else
					set @Tasa = CONVERT(decimal(19,2),@_Tasa)
			end	
				
		end
		else
			if(ISNUMERIC(@_Tasa)=1 and @_Tasa != '')
			begin
				if(CHARINDEX(',',@_Tasa) > 0)
				begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)La Tasa tiene comas(,) (.'+@_Tasa + ')*'
				end
				else
					set @Tasa = CONVERT(decimal(19,2),@_Tasa)
			
			end	
		
		--18 Entidad Liquidacion - validar para operaciones bursátiles
		set @EntidadLiquidacion = ltrim(rtrim(@EntidadLiquidacion))
		if(@CodInstruccionSGM in ('RVP','DVP'))
		begin
			if(@EntidadLiquidacion is null or @EntidadLiquidacion = '')
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)Debe informar Entidad Liquidacion.' + '*'
			end
			else
			begin
				if(not exists(select * from GCCasasCustodia where Alias = @EntidadLiquidacion))
				begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)La Entidad Liquidacion no existe.('+@EntidadLiquidacion + ') *'
				end
			end	

		end
		

		print '19 - Es Transferible - ' + @_EsTransferible
		if(ISNUMERIC(@_EsTransferible) = 0 or (@_EsTransferible != '00' and @_EsTransferible != '-1'))
		begin
			if(@CodigoTipoOperacion IN ('PF' ,'PFBDLR' ,'PFPREC', 'PFTVAR') )
				set @errorMsg = @errorMsg + '(W)Es transferible distinto de 0 o -1.' + '*'
		end
		else 
			set @EsTransferible = CONVERT(int, @_EsTransferible)

			
		print '20 - Es Precancelable - ' + @_EsPrecancelable
		if(ISNUMERIC(@_EsPrecancelable) = 0 or (@_EsPrecancelable != '00' and @_EsPrecancelable != '-1'))
		begin
			if(@CodigoTipoOperacion IN ('PF' ,'PFBDLR' ,'PFPREC', 'PFTVAR') )
				set @errorMsg = @errorMsg + '(W)Es precancelable distinto de 0 o -1.' + '*'
		end
		else 
		begin
			set @EsPrecancelable = CONVERT(int, @_EsPrecancelable)
			set @Precancelable = case when @EsPrecancelable = -1 then 1 else 0 end
		end
	


		
		print '21 - Codigo CAFCI del Mercardo - (' + LTRIM(rtrim(@CAFCIMercado)) +')'
		if(@CodigoTipoOperacion NOT IN ('CHDC' ,'CHDV') )
			if(RTRIM(LTRIM(@CAFCIMercado)) != '' AND @CAFCIMercado is not null )
				if(not exists(select merc_codigo from mercados where merc_codigo_CAFCI = LTRIM(rtrim(@CAFCIMercado))))
				begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)El código de mercado informado no existe  ('+LTRIM(rtrim(@CAFCIMercado))+').' + '*'
				end
		
		--Fecha vto solo para cheques.
		print '23 - Fecha Vencimiento - ' + @_FechaVto 
		if(@CodigoTipoOperacion in ('CHDC','CHDV'))
		begin
			if(@_FechaVto is null or @_FechaVto = '' or ISDATE(@_FechaVto) = 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)La fecha de vencimiento es inválida.' + '*'
			end
			else
				set @FechaVto = CONVERT(date,@_FechaVto)
		end
		else
		begin
			--la fecha de Vto del PF  es la fecha de liquidacion.
			if(@CodigoTipoOperacion in ('PF', 'PFBDLR','PFPREC','PFTVAR') )
				set @FechaVto = @FechaLiquidacion
			else --para todo el resto de los tipos.
			begin
				if(ltrim(rtrim(@_FechaVto)) is not null and ltrim(rtrim(@_FechaVto)) != '')
					if(ISDATE(@_fechaVto)= 1)
						set @FechaVto = CONVERT(date,@_FechaVto)
					else
						set @errorMsg = @errorMsg + '(W)La fecha de vencimiento informada es inválida ('+@_FechaVto+').' + '*'
			end
		end 


		print '26 - Codigo de Interfaz en Agente de Custodia Agente (' + @CodigoInterfazAgente + ').'
		if(@CodigoTipoOperacion in ('PF', 'PFBDLR','PFPREC','PFTVAR')) --Para PF
		begin
			if(@CodigoInterfazAgente is null or ISNUMERIC(@CodigoInterfazAgente) = 0)
			begin
				set @error = 1
				set @errorMsg = @errorMsg + '(E)El Codigo Interfaz Agente de Custodia informado no es un Banco Emisor valido para PF.(' +@CodigoInterfazAgente+')*'
			end
			else
			begin
				set @CodigoInterfazAgente = convert(varchar(3),CONVERT(int,@CodigoInterfazAgente))
				if(not exists (select * from GCBancos where Alias = @CodigoInterfazAgente))
				begin
					set @error = 1
					set @errorMsg = @errorMsg + '(E)El Codigo Interfaz Agente de Custodia informado no es un Banco Emisor existente.' + '*'
				end
					select @BancoEmisor = Alias from GCBancos where Alias = @CodigoInterfazAgente
			end
		end

		print '27 - Fecha de Alta - ' + @_FechaAlta
		if(@_FechaAlta is null or @_FechaAlta = '' or ISDATE(@_FechaAlta) = 0)
		begin
			set @errorMsg = @errorMsg + '(W)La fecha de alta es inválida.' + '*'
		end
		else
			set @FechaAlta = CONVERT(date,@_FechaAlta)		
		
		print '28 - Fecha de Actualizacion -' + @_FechaActualizacion
		if(@_FechaActualizacion is not null and @_FechaActualizacion != '' )
			if(ISDATE(@_FechaActualizacion) = 0)
				set @errorMsg = @errorMsg + '(W)La fecha de actualizacion es inválida. ('+@_FechaActualizacion + ') *'
			else
				set @FechaActualizacion = CONVERT(date,@_FechaActualizacion)	
			
		print '29 - Fecha de Proceso - ' + @_FechaProceso
		if(@_FechaProceso is null or @_FechaProceso = '' or ISDATE(@_FechaProceso) = 0)
		begin
			set @errorMsg = @errorMsg + '(W)La fecha de proceso es inválida.' + '*'
		end
		else
			set @FechaProceso = CONVERT(date,@_FechaProceso)	
		
		
		print '31 - Fecha precancelacion - ('+ @_FechaPrecancelacion +')'
		if(@_FechaPrecancelacion is null or @_FechaPrecancelacion = '' or ISDATE(@_FechaProceso) = 0)
		begin
			if(@CodigoTipoOperacion in ('PF', 'PFBDLR', 'PFPREC','PFTVAR') )
				set @errorMsg = @errorMsg + '(W)No informa fecha de precancelación para PF.' + '*'
		end
		else
			set @FechaPrecancelacion = CONVERT(date,@_FechaPrecancelacion)


		select top 1 @ccus_id = cc.ccus_id  
		from clientes cli 
		inner join cuentas_custodia_vinculadas ccv
		on ccv.clie_alias = cli.clie_alias
		inner join cuentas_custodia cc
		on ccv.ccus_id = cc.ccus_id
		inner join tiposcuentas_custodia tc
		on tc.tccus_alias = cc.tccus_alias
		inner join GCCasasCustodia casa
		on casa.ID = tc.EnteCustodia
		where cli.clie_nrodoc = convert(varchar(5),@IdFondoDepositaria)
		and tc.cg = 1
		and ccv.tvcc_id = 1
		and casa.Alias = @CodigoDepositario
		and casa.Estado = 1
	
		if(@ccus_id is null)
		begin
			set @error = 1
			set @errorMsg = @errorMsg + '(E)No existe Cuenta Custodia.(Cliente: '+convert(varchar(5),isnull(@IdFondoDepositaria,''))+' - Casa Custodia: '+isnull(@CodigoDepositario,'')+' *'
		end	
			
					
	end
	else
	begin
		set @error = 1
		set @errorMsg = @errorMsg +'(E)LA LONGITUD DEL REGISTRO NO ES CORRECTA.('+convert(varchar(10),LEN(@trama)) + ')*'
	end

	if(@TipoInstruccion is null or @TipoInstruccion = '')
	begin
		select @tipoOperDescripcion = UPPER(Descripcion) from GCInverDepoTipoOper where codigo =  @CodigoTipoOperacion
		set @error = 1
		set @errorMsg = '--- ' + isnull(@tipoOperDescripcion,'') + ' NO GENERA INSTRUCCION ---*' + @errorMsg 

	end
	-- GENERACIÓN DE INSTRUCCION
	if(@error = 0) 
	begin
	    print 'GENERO INSTRUCCION'
		set @Observaciones ='Alta: Inversiones Depositaria SG: '+ convert(varchar(15),@idSocGte) + ' - Auditoria: ' + convert(varchar(16),@CodigoAuditoria)  
		select @TipoPlazoFijo = id from GCTiposPlazoFijo where alias = case when @EsTransferible = -1 then 'T' else 'I' end 

		set @Referencia = convert(varchar(16),@CodigoAuditoria) 
		set @CuentaDepositante = @IdFondoDepositaria -- Ver si es correcto
		

		set @MontoLiquidacion = @ImporteBruto
		set @Cantidad = @ValorNominal

 		/* Valores: https://sgmtrade.atlassian.net/browse/CLBANCOBC-341 */

		set @Cantidad = 
			case  
				when @CodigoTipoOperacion IN ('AC','AV','C','CFC','CFV','CGC','CGV','CONTC','CONTV','IC','IV','PARC','PARV','PFC','PFV','V','PF','PFBDLR','PFPREC','PFTVAR','FC','FV') then @ValorNominal
				when @CodigoTipoOperacion IN ('CHDC','CHDV') then @ImporteNeto 
			end
			
		set @MontoLiquidacion = 
			case  
				when @CodigoTipoOperacion IN ('AC','AV','C','CFC','CFV','CGC','CGV','CONTC','CONTV','IC','IV','PARC','PARV','PFC','PFV','V','PF','PFBDLR','PFPREC','PFTVAR') then @ImporteNeto
				when @CodigoTipoOperacion IN ('CHDC','CHDV') then  @ValorNominal
				when @CodigoTipoOperacion IN ('FC','FV') then 0
			end
		

		EXEC GCInstruccionesAlta 
		@clie_alias,
		@TipoInstruccion,
		@FechaConcertacion,
		@FechaLiquidacion,
		@Referencia,			--OK-,  --Nro Ref @CodigoAuditoria - 
		@espe_codigo,			--OK-ISIN o CV dts_espe_convert - 
		@Cantidad,			    --OK-, --Ver si para cheques es lo mimso 
		@ccus_id,				--id Cuenta Custodia
		@CasaCustodia,			--OK-@CasaCustodia,
		@Depositante,			--OK-@ContraparteDepositante, 
		@Comitente,				--OK-@ContraparteComitente, 
		@espe_codcot,			--OK-RESUELTO
		@MontoLiquidacion, 		--OK-,	
		@Observaciones,			--OK-
		@Precancelable,			--OK-Lo tengo
		@TipoPlazoFijo,			--OK-id de tabla GCTiposPlazoFijo por @estransferible
		@BancoEmisor,			--OK-Solo para PF.
		@Intereses,				-- Averiguar con Ceci - PF 
		@Tasa,					--OK-@TNA,
		@NroCliente,			-- Para el Cheque . De donde sale?
		@CuentaDepositante,		-- 
		@NroLote,				-- Dato del cheque. De donde sale?
		NULL,					--OK-@TipoOtro,
		NULL,					--OK-@Concepto,
		NULL,					--OK-@Motivo,	
		0,						--OK-@GeneraMensaje,	
		null,					--OK-@CodigoMatching,		
		null,					--OK-@ClienteInterno,		
		'',
		'SGM',
		0,
		@errorInstruccion output,
		@err_msgInstruccion output,
		@Numero output -- NRO DE INSTRUCCION A GUARDAR EN LA TABLA  
	    
		IF @errorInstruccion > 0  
		begin  
			set @error = 1
			set @errorMsg = @errorMsg + '(E)ERROR AL GENERAR INSTRUCCION: ' + @err_msgInstruccion
		end  
	    else
		begin
			SELECT @IDInstr_out = ID FROM GCInstrucciones where Numero = @Numero  
			Print 'INSTRUCCION GENERADA ID: ' + convert(varchar(10),@IDInstr_out) + ' - Numero: ' + convert(varchar(10),@Numero)

			set @ultimoRegistro = 1
			set @instruccionManAuto = 'A'

			update GCInversionesDepositaria
			set UltimoRegistro = 0
			where CodigoAuditoria = @CodigoAuditoria
			  and UltimoRegistro = 1 

		end
	end

INSERTAR:
	begin
		
		if(@AltaReproceso != 'R')
		begin
			insert into GCInversionesDepositaria
			(
				idSociedadGerente,
				NroInstruccion,
				--
				CodigoAuditoria			,
				IdSalida				,
				Accion					,
				CodigoDepositario		,
				IdentificadorEspecie	,
				CodigoISIN				,
				CodigoCajaValoresCAFCI  ,
				CodCAFCIMonedaConcert	,
				IdFondoDepositaria		,
				FechaConcertacion		,
				FechaLiquidacion		,
				ValorNominal			,
				ImporteNeto				,
				CAFCIMonedaGastos		,
				ImporteGastos			,
				ImporteBruto			,
				Tasa					,
				EntidadLiquidacion		,
				EsTransferible			,
				EsPrecancelable			,
				CAFCIMercado			,
				AbreviaCHQ_Pagare		,
				FechaVto				,
				CodAuditOperRealacion	,
				CodigoTipoOperacion		,
				CodigoInterfazAgente	,
				FechaAlta				,
				FechaActualizacion		,
				FechaProceso			,
				CodTipoPapelInstrumento ,
				FechaPrecancelacion		,
				CBU_Broker				,
				CUIT_Broker				,
				Depositante				,
				Comitente				,
				---
				Trama,
				UltimoRegistro,
				MensajeError,
				ConError,
				Validada,
				CamposModificados,
				Modificada,
				AuditFechaModif,
				AuditUsrModif,
				Alerta,
				instruccionManAuto		
				)
				values
				(
				@idSocGte,
				@Numero,
				--
				@CodigoAuditoria        ,
				@IdSalida				,
				@Accion					,
				@CodigoDepositario		,
				@IdentificadorEspecie	,
				@CodigoISIN				,
				@CodigoCajaValoresCAFCI ,
				@CodCAFCIMonedaConcert	,
				@IdFondoDepositaria		,
				@FechaConcertacion	    ,
				@FechaLiquidacion       ,
				@ValorNominal			,
				@ImporteNeto			,
				@CAFCIMonedaGastos		,
				@ImporteGastos			,
				@ImporteBruto			,
				@Tasa					,
				@EntidadLiquidacion		,
				@EsTransferible			,
				@EsPrecancelable		,
				@CAFCIMercado			,
				@AbreviaCHQ_Pagare		,
				@FechaVto				,
				@CodAuditOperRealacion	,
				@CodigoTipoOperacion	,
				@CodigoInterfazAgente	,
				@FechaAlta				,
				@FechaActualizacion		,
				@FechaProceso			,
				@CodTipoPapelInstrumento,
				@FechaPrecancelacion	,
				@CBU_Broker				,
				@CUIT_Broker			,
				@Depositante			,
				@Comitente				,		
				---
				@trama,
				@ultimoRegistro, 
				@errorMsg,
				@error,
				0, -- Procesado
				@camposModificados,
				0, --Modificada
				null,
				null,
				@Alerta,  --(1 - Rojo: Modifica oper con instruccion / 2 - Amarillo: Modifia Oper sin instruccion.)
				@instruccionManAuto
				)
 		end
		else
		begin
			update GCInversionesDepositaria 
			set 
			MensajeError = @errorMsg,
			ConError = @error
			where Id = @idReproceso

		end

		if(@Numero is not null)
		begin
			select @idInverDepo = max(Id) 
			from GCInversionesDepositaria
			Where NroInstruccion = @Numero
			and idSociedadGerente = @idSocGte
			and CodigoAuditoria = @CodigoAuditoria
			
			insert into GCInstruccionInverDepo
			values (@IDInstr_out, @idInverDepo, @ImporteBruto,@ImporteGastos,@CAFCIMonedaGastos,@CBU_Broker,@CUIT_Broker)
			
			if(@idReproceso != 0)
			begin
				update GCInversionesDepositaria 
				set 
				NroInstruccion = @Numero,
				instruccionManAuto = 'A'
				where Id = @idReproceso
			end
		end

	end
	FIN:
	Print @errorMsg
end
