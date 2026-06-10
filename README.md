# Sistema Acadêmico Distribuído

Este repositório centraliza a documentação do projeto final da disciplina de Sistemas Distribuídos.

## Objetivo

O projeto tem como objetivo demonstrar uma arquitetura distribuída baseada em microsserviços para um sistema acadêmico, utilizando Spring Boot, Eureka Server, RabbitMQ, gRPC e bancos H2 isolados por serviço.

## Microsserviços

- Eureka Server
- Aluno Service
- Disciplina Service
- Turma Service
- Matrícula Service
- Notificação Service
- Histórico Service

## Organização

Cada microsserviço possui seu próprio repositório. Este repositório contém a documentação central, evidências, diagramas, arquivos de apoio, collection do Postman, relatório e slides.

## Subindo o ambiente com Docker Compose

O arquivo `docker-compose.yml` deste repositório sobe **todos** os serviços de uma vez (não é preciso rodar um a um). Só é necessário ter o **Docker** instalado — o build compila cada serviço em containers (Maven + Java 21), você não precisa instalar Java nem Maven localmente.

### 1. Clonar todos os repositórios na mesma pasta

O compose espera a seguinte estrutura de pastas (todos os repos lado a lado):

```
Trabalho de Sistemas Distribuidos/
├── sd_academico_docs/              <- rode o compose a partir daqui
├── sd_academico_eureka_server/
├── sd_academico_aluno_service/
├── sd_academico_disciplina_service/
├── sd_academico_turma_service/
├── sd_academico_matricula_service/
├── sd_academico_notificacao_service/
└── sd_academico_historico_service/
```

Os links dos repositórios estão em [links-dos-repos.md](links-dos-repos.md).

### 2. Subir tudo

A partir da pasta `sd_academico_docs`:

```bash
docker compose up --build        # constrói e sobe tudo (use -d para rodar em segundo plano)
docker compose down              # derruba tudo
docker compose logs -f matricula-service   # acompanha os logs de um serviço
```

A primeira execução demora alguns minutos (compila os 7 serviços); depois fica em cache.

### Portas (no host)

| Serviço             | Endereço                          |
|---------------------|-----------------------------------|
| Eureka (painel)     | http://localhost:8761             |
| matricula-service   | http://localhost:8081             |
| aluno-service       | http://localhost:8082             |
| disciplina-service  | http://localhost:8083             |
| turma-service       | http://localhost:8084 (HTTP) / 9093 (gRPC) |
| notificacao-service | http://localhost:8085             |
| historico-service   | http://localhost:8086             |
| RabbitMQ (painel)   | http://localhost:15672 (guest/guest) |

> Observação: o `turma-service` usa a porta **8084** (HTTP) para não conflitar com o `disciplina-service` (8083); o servidor gRPC do turma continua na **9093**.