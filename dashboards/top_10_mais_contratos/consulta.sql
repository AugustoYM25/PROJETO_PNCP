SELECT TOP 10
    orgaoEntidade_razaoSocial AS Orgao,
    COUNT(numeroControlePncpCompra) AS Quantidade_Contratos
FROM silver.contratos
GROUP BY orgaoEntidade_razaoSocial
ORDER BY Quantidade_Contratos DESC;
