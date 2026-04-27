WITH Amostra AS (
    SELECT 
        c.numeroControlePNCP,
        d.id_perfil_ml,
        d.nome_categoria,
        c.objetoContrato,
        ROW_NUMBER() OVER(PARTITION BY c.id_perfil_ml ORDER BY NEWID()) as LinhaSorteada
    FROM gold.contratos_enriquecidos c
    JOIN gold.dim_perfis_ia d ON c.id_perfil_ml = d.id_perfil_ml
    WHERE d.tipo_cluster NOT IN ('Vazio', 'Lixeira') 
)
SELECT 
    id_perfil_ml,
    nome_categoria,
    objetoContrato
FROM AmostraEstratificada
WHERE LinhaSorteada <= 20
ORDER BY id_perfil_ml, LinhaSorteada;
