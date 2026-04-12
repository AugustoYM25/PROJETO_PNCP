SELECT TOP 10
    nomeRazaoSocialFornecedor AS Razao_Social,
    niFornecedor AS CNPJ_Fornecedor,  
    COUNT(numeroControlePncpCompra) AS Total_Contratos
FROM silver.contratos
WHERE niFornecedor IS NOT NULL
GROUP BY nomeRazaoSocialFornecedor,niFornecedor
ORDER BY Total_Contratos DESC;
