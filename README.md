# Documentação do Case: ETL Medallion Breweries

## Introdução

Este projeto implementa um pipeline ETL utilizando a arquitetura Medallion. O objetivo é consumir dados da API disponibilizada no case, transformá-los e armazená-los em um Data Lake(nos notebooks foi utilizado o drive parar armazenamento), seguindo boas práticas de particionamento e organização de dados.

## Arquitetura do Pipeline

- **Camada Bronze**:
  - Armazena os dados brutos extraídos da API, no formato original ou em um formato mais adequado (por exemplo, JSON).

- **Camada Silver**:
  - Transforma os dados da camada Bronze em um formato columnar, particionando-os por localização (nesse caso foi criada uma coluna que concatenava os valores de City e State, chamada brewery_location).

- **Camada Gold**:
  - Cria uma visão agregada com a quantidade de cervejarias por tipo e localização, como solicitava a demanda.

## Monitoramento e Alertas

O monitoramento do pipeline é realizado através do Apache Airflow, com as seguintes abordagens:

- **Verificação de Qualidade de Dados**:
  - Implementar checks de qualidade em pontos críticos do pipeline, como contagem de registros e checagem de valores nulos.

- **Alertas de Falhas**:
  - Configurar alertas por email para notificar sobre falhas no pipeline ou problemas de qualidade de dados.

- **Dashboard de Monitoramento**:
  - Integrar o Airflow com ferramentas de monitoramento, como Prometheus e Grafana, para visualização em tempo real da execução do pipeline.

## Considerações Finais

Este projeto demonstra a implementação de um pipeline ETL seguindo a arquitetura Medallion, enfatizando a organização, transformação e otimização dos dados para consumo analítico.
