-- PRIMEIRA PARTE DO CODIGO

-- Caso exista tabela comercial deletar essa tabela;

IF EXISTS (SELECT name FROM sys.tables WHERE name = 'vendas_comercial')
    BEGIN
        DROP TABLE 'vendas_comercial';
    END

-- Criar a tabela registro comercial

CREATE TABLE vendas_comercial (
    vendas_id VARCHAR(50),
    clientes_id INT,
    Nome_cliente VARCHAR(100),
    Pais_cliente VARCHAR(50),
    Continente_cliente VARCHAR(50),
    Tipo_mercado VARCHAR(50),
    regiao_vendas VARCHAR(50),
    produto_id INT,
    categoria_produto VARCHAR(100),
    data_venda DATE,
    ano INT,
    mesNome VARCHAR(20),
    dia INT,
    preco_unitario DECIMAL(18,2),
    quantidadade INT,
    desconto DECIMAL(18,2),
    total_vendido DECIMAL(18,2),
    dataregistro DATETIME
);

-- criar tabela para registro de logs

IF EXISTS (SELECT name from sys.tables where name = 'log_carga_vendas')
BEGIN
    DROP TABLE 'log_carga_vendas'
END

CREATE TABLE log_carga_vendas (
    id_log INT IDENTITY(1,1) PRIMARY KEY,
    data_execucao DATETIME DEFAULT GETDATE(),
    tipo_carga VARCHAR(20),
    status VARCHAR(20),
    registros_inseridos INT,
    mensagem VARCHAR(255)
);
 
-- SEGUNDA PARTE 

-- insercao de dados na tabela comericial primeira carga

IF (SELECT COUNT(*) FROM vendas_comercial) = 0

BEGIN
    -- Carga inicial: dados até agosto de 2024
    INSERT INTO vendas_comercial (
        vendas_id,
        clientes_id,
        Nome_cliente,
        Pais_cliente,
        Continente_cliente,
        Tipo_mercado,
        regiao_vendas,
        produto_id,
        categoria_produto,
        data_venda,
        ano,
        mesNome,
        dia,
        preco_unitario,
        quantidadade,
        desconto,
        total_vendido,
        dataregistro
    )
    SELECT 
        v.vendas_id,
        c.cliente_id,
        c.nome,
        c.pais,
        CASE 
            WHEN c.pais IN ('Brasil', 'EUA', 'Canadá', 'Venezuela') THEN c.pais
            ELSE 'Europa'
        END AS Continente_cliente,
        CASE 
            WHEN LEFT(v.vendas_id, 2) = '01' THEN 'Cliente Mercado Local'
            WHEN LEFT(v.vendas_id, 2) = '03' THEN 'Clientes Mercado Hotel'
            ELSE 'Distribuidores'
        END AS Tipo_mercado,
        c.regiao,
        d.produto_id,
        ct.categoria_nome,
        v.data_venda,
        YEAR(v.data_venda) AS ano,
        DATENAME(month, v.data_venda) AS mesNome,
        DAY(v.data_venda) AS dia,
        COALESCE(d.preco_unitario, 0) AS preco_unitario,
        COALESCE(d.quantidade, 0) AS quantidadade,
        COALESCE(d.desconto, 0) AS desconto,
        (COALESCE(d.quantidade, 0) * COALESCE(d.preco_unitario, 0)) AS total_vendido,
        GETDATE() AS dataregistro
    FROM vendas AS v 
    LEFT JOIN detalhes_pedidos AS d ON d.vendas_id = v.vendas_id
    LEFT JOIN clientes AS c ON c.cliente_id = v.cliente_id
    LEFT JOIN produtos AS p ON p.produto_id = d.produto_id
    LEFT JOIN categoria AS ct ON ct.categoriaid = p.categoriaid
    WHERE v.data_venda <= '2024-08-31';
END

-- TERCEIRA PARTE
-- inicia da carga incremental na tabela comercial
ELSE

BEGIN
    DECLARE @ultimaData DATE;
    DECLARE @ultimaMenosUmaSemana DATE;

    SELECT @ultimaData = MAX(data_venda) FROM vendas_comercial;
    SET @ultimaMenosUmaSemana = DATEADD(DAY, -7, @ultimaData);

    -- Apaga dados de ultima semana antes de incluir novos dados 
    DELETE FROM vendas_comercial 
    WHERE data_venda BETWEEN @ultimaMenosUmaSemana AND @ultimaData;

    -- Carga incremental: dados após maximo carregado anteriormente

    BEGIN TRY

        DECLARE @registrosInseridos INT = 0;

        INSERT INTO vendas_comercial (
            vendas_id,
            clientes_id,
            Nome_cliente,
            Pais_cliente,
            Continente_cliente,
            Tipo_mercado,
            regiao_vendas,
            produto_id,
            categoria_produto,
            data_venda,
            ano,
            mesNome,
            dia,
            preco_unitario,
            quantidadade,
            desconto,
            total_vendido,
            dataregistro
        )
        SELECT 
            v.vendas_id,
            c.cliente_id,
            c.nome,
            c.pais,
            CASE 
                WHEN c.pais IN ('Brasil', 'EUA', 'Canadá', 'Venezuela') THEN c.pais
                ELSE 'Europa'
            END AS Continente_cliente,
            CASE 
                WHEN LEFT(v.vendas_id, 2) = '01' THEN 'Cliente Mercado Local'
                WHEN LEFT(v.vendas_id, 2) = '03' THEN 'Clientes Mercado Hotel'
                ELSE 'Distribuidores'
            END AS Tipo_mercado,
            c.regiao,
            d.produto_id,
            ct.categoria_nome,
            v.data_venda,
            YEAR(v.data_venda) AS ano,
            DATENAME(month, v.data_venda) AS mesNome,
            DAY(v.data_venda) AS dia,
            COALESCE(d.preco_unitario, 0) AS preco_unitario,
            COALESCE(d.quantidade, 0) AS quantidadade,
            COALESCE(d.desconto, 0) AS desconto,
            (COALESCE(d.quantidade, 0) * COALESCE(d.preco_unitario, 0)) AS total_vendido,
            GETDATE() AS dataregistro
        FROM vendas AS v 
        LEFT JOIN detalhes_pedidos AS d ON d.vendas_id = v.vendas_id
        LEFT JOIN clientes AS c ON c.cliente_id = v.cliente_id
        LEFT JOIN produtos AS p ON p.produto_id = d.produto_id
        LEFT JOIN categoria AS ct ON ct.categoriaid = p.categoriaid
        WHERE v.data_venda >= @ultimaMenosUmaSemana;

        -- INSERIR DADOS NA TABELA DE LOG CASO SUCESSO
        SET @registrosInseridos = @@ROWCOUNT;
        INSERT INTO log_carga_vendas (tipo_carga, status, registros_inseridos, mensagem)
        VALUES ('INCREMENTAL', 'SUCESSO', @registrosInseridos, 'Carga inicial concluída');

    END TRY

    -- INSERIR DADOS NA TABELA DE LOG CASO ERRO
    BEGIN CATCH
        INSERT INTO log_carga_vendas (tipo_carga, status, registros_inseridos, mensagem)
        VALUES ('INCREMENTAL', 'ERRO', 0, ERROR_MESSAGE());
    END CATCH

END

-- Fim projecto
