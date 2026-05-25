# NYC Taxi Study - Pipeline de Dados

Projeto de estudo que processa dados de táxis amarelos de Nova York (NYC TLC) através de uma pipeline batch com orientação a objetos.

## Visão Geral

Este projeto implementa uma pipeline de dados que transforma arquivos parquet brutos em tabelas analíticas, seguindo o padrão Medallion Architecture (Bronze → Silver → Gold). A implementação usa processamento batch com PySpark, orientação a objetos para reutilização de código, e Delta Lake para armazenamento ACID.

**Período dos dados**: Janeiro a Maio de 2023

## Arquitetura

```
Arquivos Parquet (Landing Zone)
    ↓
Bronze: Ingestão e normalização
    ↓
Silver: Limpeza e enriquecimento
    ↓
Gold: Agregações e KPIs
```

**Catálogo**: `frt_am` (Fleet Rates Taxi - Americas)  
**Origem**: `/Volumes/frt_am/landing/data_prd/yellow_tripdata/`

**Tabelas**:
- Bronze: `frt_am.bronze.nyc_tlc__yellowtripdata`
- Silver: `frt_am.silver.nyc_tlc__yellowtripdata`
- Gold: `frt_am.gold.nyc_tlc__yellowtripdata`

**Convenção de nomenclatura**:
- `frt`: Fleet Rates Taxi (domínio de negócio)
- `am`: Americas (região geográfica)
- Suporta multi-região: `frt_am`, `frt_eu`, `frt_ap`, etc.

## Estrutura do Projeto

```
case-nyc-taxi-study/
├── src/
│   ├── data_extraction/
│   │   ├── bronze/yellow_trip_data.py
│   │   ├── silver/yellow_trip_data.ipynb
│   │   └── gold/yellow_trip_data.ipynb
│   └── tools/
│       ├── model/
│       │   ├── entity.py
│       │   └── extract.py
│       └── utils/
│           ├── deltalake_manager.py
│           ├── utils.py
│           └── widget.py
├── pipeline/
│   └── get_data_ny_tlc_lambda_to_s3/
└── notebooks/
```

## Camadas da Pipeline

### Bronze - Ingestão

Lê arquivos parquet da landing zone e carrega no Delta Lake com normalização de schema.

**Responsabilidades**:
- Normalização de schema entre diferentes arquivos
- Conversão de tipos para formato padrão
- Adição de metadados de rastreamento
- Suporte a carga completa ou incremental

**Metadados adicionados**:
- `_ingestion_timestamp`: Timestamp da ingestão
- `_src_file_path`: Caminho do arquivo original
- `_src_file_name`: Nome do arquivo
- `_src_file_mod_time`: Data de modificação do arquivo
- `_src_row_hash`: Hash MD5 da linha
- `_pipeline_name`: Nome da pipeline
- `_environment`: Ambiente de execução
- `_load_mode`: FULL ou INCREMENTAL

**Execução**:
```python
# Carga completa
YellowTripData(full_load=True).run(operation="overwrite")

# Carga incremental (últimos 7 dias)
YellowTripData(full_load=False, offset_days=-7).run(operation="merge")
```

### Silver - Limpeza e Enriquecimento

Processa dados da bronze aplicando limpeza, validação e criação de campos derivados.

**Transformações**:
- Remoção de registros inválidos
- Cálculo de campos derivados (duração, velocidade, categorias)
- Criação de campos de data para análise temporal
- Validação de regras de negócio

**Validações aplicadas**:
- Valores monetários positivos
- Distância maior que zero
- Data de pickup anterior à dropoff
- Número de passageiros entre 1 e 8
- Filtro de valores extremos (viagens > 500 milhas, fare > $1000)

**Campos derivados**:
- `dt_ref`: Data de referência para particionamento
- `trip_duration_minutes`: Duração em minutos
- `avg_speed_mph`: Velocidade média
- `trip_category`: Categoria da viagem
- `time_of_day`: Período do dia

### Gold - Agregações e KPIs

Cria tabelas agregadas prontas para consumo analítico.

**KPIs disponíveis**:
- Métricas diárias: viagens, receita, distância média
- Análise de gorjetas: percentual médio, taxa
- Distribuição geográfica: zonas mais movimentadas
- Padrões temporais: horários de pico, dias da semana
- Performance por vendor: comparação entre empresas

## Implementação Técnica

### Orientação a Objetos

A pipeline usa herança de classes para reutilização de código:

```
Entity (classe base com logging)
    ↓
Extract (classe de extração batch)
    ↓
YellowTripData (implementação específica)
```

**Benefícios**:
- Código reutilizável entre diferentes entidades
- Logging automático de execuções
- Padrão consistente de implementação
- Facilita manutenção e extensão

### Processamento Batch

**Full Load**: Processa todos os arquivos disponíveis
```python
YellowTripData(full_load=True).run(operation="overwrite")
```

**Incremental**: Processa apenas arquivos modificados nos últimos N dias
```python
YellowTripData(full_load=False, offset_days=-7).run(operation="merge")
```

### Schema Evolution

A pipeline lida automaticamente com mudanças no schema:
- Novos campos são adicionados com valor `null`
- Campos ausentes são preenchidos com `null`
- Tipos de dados são convertidos para o schema canônico

## Monitoramento e Logs

### Sistema de Logging

Cada execução gera logs automáticos com:
- UUID único da execução
- Timestamp de início e fim
- Contagem de registros processados
- Status (OK ou FAILED)
- Duração em segundos
- Mensagem de erro e stack trace (em caso de falha)

### Consultar Logs

```sql
-- Últimas execuções
SELECT 
    id,
    date,
    entity,
    message,
    count,
    duration,
    tag
FROM frt_am.bronze.nyc_tlc__track
WHERE entity = 'YellowTripdata'
ORDER BY date DESC
LIMIT 10;

-- Apenas falhas
SELECT * 
FROM frt_am.bronze.nyc_tlc__track
WHERE message = 'FAILED'
ORDER BY date DESC;
```

### Métricas de Qualidade

```sql
-- Volume de dados por dia
SELECT 
    DATE(tpep_pickup_datetime) as data,
    COUNT(*) as total_viagens,
    SUM(total_amount) as receita_total
FROM frt_am.bronze.nyc_tlc__yellowtripdata
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY data DESC;
```

## Pré-requisitos

1. Databricks workspace configurado
2. Unity Catalog habilitado
3. Volume criado:
   ```sql
   CREATE VOLUME IF NOT EXISTS frt_am.landing.data_prd;
   ```
4. Dados parquet de Janeiro a Maio de 2023 disponíveis no volume

## Execução

### Bronze
```python
%run ./src/data_extraction/bronze/yellow_trip_data
YellowTripData(full_load=True).run(operation="overwrite")
```

### Silver
Abrir e executar: `src/data_extraction/silver/yellow_trip_data.ipynb`

### Gold
Abrir e executar: `src/data_extraction/gold/yellow_trip_data.ipynb`

## Troubleshooting

### Path does not exist

Verificar se volume existe:
```sql
DESCRIBE VOLUME frt_am.landing.data_prd;
```

Listar arquivos:
```python
%fs ls /Volumes/frt_am/landing/data_prd/yellow_tripdata/
```

### No files found

- Verificar se há arquivos parquet no path
- Ajustar `offset_days` para período maior
- Usar `full_load=True` para processar todos os arquivos

### Schema mismatch

- A pipeline normaliza automaticamente o schema
- Verificar se `CANONICAL_SCHEMA` está atualizado
- Executar com `operation="overwrite"` para recriar tabela

### Performance lenta

- Reduzir número de arquivos (usar incremental)
- Aumentar tamanho do cluster
- Verificar particionamento
- Usar `cache()` em DataFrames reutilizados

## Tecnologias

- **Databricks**: Plataforma de processamento
- **PySpark**: Processamento distribuído
- **Delta Lake**: Armazenamento ACID
- **Unity Catalog**: Governança de dados
- **Python OOP**: Orientação a objetos
- **Terraform**: Infraestrutura como código

## Referências

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Databricks Delta Lake](https://docs.databricks.com/delta/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
