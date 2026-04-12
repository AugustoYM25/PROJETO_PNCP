
CREATE SCHEMA silver;
GO;

SELECT 
    
    numeroControlePncpCompra,
    numeroControlePNCP,
    processo,
    numeroContratoEmpenho,
    TRY_CAST(anoContrato AS INT) AS anoContrato,
    TRY_CAST(sequencialContrato AS INT) AS sequencialContrato,

    
    niFornecedor,
    tipoPessoa,
    nomeRazaoSocialFornecedor,
    COALESCE(codigoPaisFornecedor, 'NÃO INFORMADO') AS codigoPaisFornecedor,

    
    TRY_CAST(dataAtualizacao AS DATETIME2) AS dataAtualizacao,
    TRY_CAST(dataAtualizacaoGlobal AS DATETIME2) AS dataAtualizacaoGlobal,
    TRY_CAST(dataAssinatura AS DATE) AS dataAssinatura,
    TRY_CAST(dataVigenciaInicio AS DATE) AS dataVigenciaInicio,
    TRY_CAST(dataVigenciaFim AS DATE) AS dataVigenciaFim,
    TRY_CAST(dataPublicacaoPncp AS DATETIME2) AS dataPublicacaoPncp,

    
    TRY_CAST(valorInicial AS DECIMAL(18,2)) AS valorInicial,
    TRY_CAST(valorParcela AS DECIMAL(18,2)) AS valorParcela,
    TRY_CAST(valorGlobal AS DECIMAL(18,2)) AS valorGlobal,
    COALESCE(TRY_CAST(valorAcumulado AS DECIMAL(18,2)), 0) AS valorAcumulado,
    TRY_CAST(numeroParcelas AS INT) AS numeroParcelas,
    TRY_CAST(numeroRetificacao AS INT) AS numeroRetificacao,
    TRY_CAST(receita AS BIT) AS receita,

    
    objetoContrato,
    COALESCE(informacaoComplementar, 'SEM INFORMAÇÃO') AS informacaoComplementar,
    TRY_CAST(categoriaProcesso_id AS INT) AS categoriaProcesso_id,
    categoriaProcesso_nome,
    TRY_CAST(tipoContrato_id AS INT) AS tipoContrato_id,
    tipoContrato_nome,
    usuarioNome,

    
    orgaoEntidade_cnpj,
    orgaoEntidade_razaoSocial,
    orgaoEntidade_poderId,
    orgaoEntidade_esferaId,
    unidadeOrgao_codigoUnidade,
    unidadeOrgao_nomeUnidade,
    unidadeOrgao_ufSigla,
    unidadeOrgao_ufNome,
    unidadeOrgao_municipioNome,
    TRY_CAST(unidadeOrgao_codigoIbge AS INT) AS unidadeOrgao_codigoIbge,

    
    GETDATE() AS dt_processamento,
    CAST('PNCP_RAW' AS VARCHAR(100)) AS fonte_arquivo

INTO silver.contratos 
FROM bronze.contratos_raw
WHERE numeroControlePncpCompra IS NOT NULL;
