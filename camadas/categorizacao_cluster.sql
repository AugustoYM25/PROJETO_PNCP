
CREATE TABLE gold.dim_perfis_ia (
    id_perfil_ml INT PRIMARY KEY,
    nome_categoria VARCHAR(100),
    tipo_cluster VARCHAR(50) -- P
);


INSERT INTO gold.dim_perfis_ia (id_perfil_ml, nome_categoria, tipo_cluster) VALUES 

(1, 'Saúde: Materiais Hospitalares e Ambulatoriais', 'Alta Coesão'),
(7, 'Saúde: Soluções Fisiológicas', 'Alta Coesão'),
(10, 'Saúde: Odontologia', 'Alta Coesão'),
(12, 'Saúde: Materiais Hospitalares e Ambulatoriais', 'Alta Coesão'),
(4, 'Saúde: Medicamentos e Suplementos', 'Alta Coesão'),


(3, 'Alimentação: Nutrição e Suplementos', 'Alta Coesão'),
(15, 'Alimentação: Gêneros (Café, etc)', 'Alta Coesão'),


(2, 'Logística: Abastecimento de Água (Carro Pipa)', 'Hiper-segmentado'),
(8, 'Logística: Abastecimento de Água (Carro Pipa)', 'Hiper-segmentado'),
(13, 'Logística: Abastecimento de Água (Carro Pipa)', 'Hiper-segmentado'),


(0, 'Insumos Mistos (Papelaria e Medicamentos)', 'Colisão de Domínio'),
(6, 'Insumos Mistos (Didáticos e Odontologia)', 'Colisão de Domínio'),
(14, 'Insumos Diversos (Termo Genérico)', 'Colisão de Domínio'),


(5, 'Contratos Agrupados por Template', 'Baixa Coesão'),
(16, 'Serviços e Aquisições Diversas ', 'Lixeira '),
(18, 'Serviços e Aquisições Diversas ', 'Lixeira '),


(9, 'Não Utilizado (Zerado)', 'Vazio'),
(11, 'Não Utilizado (Zerado)', 'Vazio'),
(17, 'Não Utilizado (Zerado)', 'Vazio'),
(19, 'Não Utilizado (Zerado)', 'Vazio');
