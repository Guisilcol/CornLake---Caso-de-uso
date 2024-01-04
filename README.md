# CornLake - Plataforma de Dados Fictícia

## Objetivo de Negócio

O objetivo principal deste projeto é criar uma plataforma de dados fictícia que sirva como um exemplo de caso de uso. Uma parte essencial do projeto é tornar os dados acessíveis a todos, promovendo a democratização do acesso.

## Casos de Uso - Utilização do Datalake

Nossa plataforma de dados terá diversos casos de uso, abrangendo a utilização do Data Lake em cenários práticos.

## Arquitetura do Projeto

A arquitetura do projeto será dividida em duas partes principais: Ingestão de Dados (Arquitetura de Ingestão) e Transformação de Dados (Arquitetura de Transformação).

- **Ingestão**: Esse processo envolve a inserção, cópia e ingestão de dados da mesma forma que existem na origem, em um formato de fácil localização e padronização.

- **Transformação**: Esta parte incluirá todos os processos de agregação, separação, análise, modelos de Machine Learning e modelos de IA que possam fazer parte do projeto, sendo separados dos processos de ingestão.

## Segurança

A segurança dos dados será uma consideração fundamental. Embora nossos protocolos de segurança sejam básicos no momento, planejamos criar usuários e permissões adequadas.

Além disso, estudaremos questões de acesso fora do ambiente e exploraremos como criar uma rede privada na AWS.

## Governança de Dados

A governança de dados será uma parte crucial do projeto. Devemos estudar como usar as informações disponíveis para estabelecer a melhor governança possível.

Isso inclui aprofundar a compreensão dos metadados, que são discutidos em detalhes [aqui](https://www.notion.so/CornLake-94cee82def22402d8a924f63181aaca1?pvs=21).

## Integração de Dados

Desenvolveremos uma estratégia eficiente de integração de dados para lidar com diversas fontes de dados. Abordaremos questões como o processo ETL (Extração, Transformação e Carga) e a necessidade de ferramentas específicas.

Discutiremos o ambiente mínimo e o ambiente máximo necessários para a integração de dados.

## Metadados

A gestão de metadados não se limitará a tabelas e colunas, incluirá também o monitoramento de dados e processamento. Consulte mais informações sobre isso [aqui](https://www.notion.so/CornLake-94cee82def22402d8a924f63181aaca1?pvs=21).

## Escalabilidade

Um aspecto fundamental deste projeto é a escalabilidade. Devemos criar uma plataforma que seja capaz de lidar com o aumento no volume de dados ao longo do tempo.

## Monitoramento e Manutenção

Estudaremos ferramentas que possam ser utilizadas com eficiência e baixo custo. Estabeleceremos processos de manutenção para validar a eficácia de processos mais antigos ao longo do tempo.

## Como configurar o projeto e desenvolver localmente?

### Pré-requisitos

- Docker
- Docker Compose
- AWS CLI
- AWS SAM CLI
- Python 3.10
- Poetry (Se possível)

### Configuração
... 

### Estrutura do projeto 

Pastas que iniciam com `lambda_` correspondem ao código fonte das funções lambda do Projeto. Cada função lambda é equivalente a um "projeto" separado, com seu próprio `requirements.txt` e `pyproject.toml` (este último é gerado pelo Poetry). Utilizamos o Poetry para gerenciar os ambientes virtuais DE CADA função lambda.
