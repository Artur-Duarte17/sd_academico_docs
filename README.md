# Sistema Acadêmico Distribuído

Este repositório centraliza a documentação do projeto final da disciplina de Sistemas Distribuídos.

## Objetivo

O projeto demonstra uma arquitetura distribuída baseada em microsserviços para um sistema acadêmico, aplicando na prática os principais mecanismos de comunicação estudados em sala:

- **Invocação Remota (RPC)** com **gRPC** para comunicação **síncrona** entre serviços;
- **Mensageria e Eventos** com **RabbitMQ**, cobrindo **filas** (processamento assíncrono) e **Publish/Subscribe** (eventos de domínio);
- **Serviço de Nomes / Service Discovery** com **Eureka Server** (Spring Cloud Netflix).

Stack: Spring Boot 3 / Java 21, Spring Cloud (Eureka + OpenFeign), gRPC, RabbitMQ e bancos H2 isolados por serviço.

---

## 1. Microsserviços

| Serviço | Papel | Banco |
|---------|-------|-------|
| **Eureka Server** | Serviço de nomes / Service Discovery (registra e localiza os demais serviços) | - |
| **Aluno Service** | CRUD de alunos; expõe consultas de existência/estado (ATIVO) | H2 `alunodb` |
| **Disciplina Service** | CRUD de disciplinas | H2 `disciplinadb` |
| **Turma Service** | CRUD de turmas e controle de **vagas**; expõe o **servidor gRPC** | H2 `turmadb` |
| **Matrícula Service** | Orquestra a matrícula (cliente gRPC + Feign + produtor de eventos) | H2 `matriculadb` |
| **Notificação Service** | Consome filas/eventos e registra notificações | H2 `notificacaodb` |
| **Histórico Service** | Consome eventos e registra o histórico acadêmico | H2 `historicodb` |

Cada microsserviço possui seu próprio repositório. Este repositório (`sd_academico_docs`) contém a documentação central, evidências, relatório, slides e o `docker-compose.yml` que sobe todo o ambiente.

---

## 2. Descrição Técnica

### 2.1. Invocação Remota (RPC): gRPC

A comunicação **síncrona** que exige resposta imediata e forte acoplamento de contrato é feita por **gRPC**, entre o **Matrícula Service** (cliente) e o **Turma Service** (servidor).

**Contrato (`turma.proto`)** define o serviço e as mensagens trocadas (Protocol Buffers):

```protobuf
service TurmaGrpcService {
  rpc ReservarVaga (ReservaVagaRequest) returns (ReservaVagaResponse);
  rpc LiberarVaga  (LiberaVagaRequest)  returns (LiberaVagaResponse);
}
```

**Servidor gRPC, Turma Service** (`TurmaGrpcServiceImpl`, porta **9093**):

```java
@GrpcService
public class TurmaGrpcServiceImpl extends TurmaGrpcServiceGrpc.TurmaGrpcServiceImplBase {
    @Override
    public void reservarVaga(ReservaVagaRequest request, StreamObserver<ReservaVagaResponse> obs) {
        boolean sucesso = turmaService.reservarVaga(request.getTurmaId());
        obs.onNext(ReservaVagaResponse.newBuilder().setSucesso(sucesso)...build());
        obs.onCompleted();
    }
}
```

**Cliente gRPC, Matrícula Service** (`GrpcClientConfiguration` cria o *stub* síncrono / *blocking stub*):

```java
@Bean
TurmaGrpcServiceGrpc.TurmaGrpcServiceBlockingStub turmaGrpcStub(GrpcChannelFactory factory) {
    return TurmaGrpcServiceGrpc.newBlockingStub(factory.createChannel("turma-service"));
}
```

O Matrícula Service chama o *stub* como se fosse um método local (transparência de acesso). Ao criar uma matrícula ele **reserva** a vaga; ao cancelar, **libera** a vaga:

```java
ReservaVagaResponse reserva = turmaGrpcStub.reservarVaga(
        ReservaVagaRequest.newBuilder().setTurmaId(turmaId).build());
```

> **Saga / compensação:** como a reserva da vaga e a persistência da matrícula estão em serviços diferentes, o Matrícula Service implementa **compensação**: se a vaga é reservada via gRPC mas o `save` local falha, ele chama `LiberarVaga` para não deixar a vaga presa (e vice-versa no cancelamento). É um exemplo prático de **consistência em sistemas distribuídos** sem transação global.

Além do gRPC, há também invocação síncrona via **REST + OpenFeign** (ver Serviço de Nomes, abaixo): Matrícula → Aluno e Turma → Disciplina. O gRPC foi escolhido para o fluxo de vagas por ser binário, tipado e de baixa latência; o Feign, para validações simples de existência.

### 2.2. Mensageria e Eventos: RabbitMQ

O RabbitMQ é usado de **duas formas distintas**, atendendo aos dois padrões pedidos.

#### a) Publish/Subscribe (eventos de domínio): comunicação assíncrona desacoplada

O **Matrícula Service** publica eventos de negócio em um **Topic Exchange** `academico.events`, sem conhecer quem os consome:

```java
// Produtor: MatriculaService.publicarEvento(...)
rabbitTemplate.convertAndSend(
        "academico.events",            // exchange
        "matricula.criada",            // routing key (ou "matricula.cancelada")
        mensagemJson);                 // { alunoId, turmaId, tipo, descricao }
```

**Dois assinantes**, cada um com **sua própria fila** ligada (binding) ao mesmo exchange, recebem **cópias independentes** do evento (fan-out por routing key):

| Serviço | Fila | Binding (routing keys) |
|---------|------|------------------------|
| Notificação Service | `queue.evento.notificacao` | `matricula.criada`, `matricula.cancelada` |
| Histórico Service | `queue.evento.historico` | `matricula.criada`, `matricula.cancelada` |

```java
// Consumidor: Histórico Service
@RabbitListener(queues = "queue.evento.historico")
public void consumirEvento(String mensagem) { /* grava no histórico */ }

// Consumidor: Notificação Service
@RabbitListener(queues = "queue.evento.notificacao")
public void consumirEvento(String mensagem) { /* registra notificação */ }
```

Isso é **Publish/Subscribe**: uma publicação, vários consumidores independentes. Adicionar um novo assinante (ex.: relatórios) **não exige alterar o produtor**: basta declarar uma nova fila e binding.

> A publicação é **best-effort**: uma indisponibilidade do RabbitMQ é registrada em log mas **não desfaz** a matrícula já persistida, evitando acoplar a disponibilidade dos consumidores ao fluxo principal.

#### b) Fila direta (processamento assíncrono ponto-a-ponto): Work Queue

Além dos eventos, há a fila direta `fila.notificacoes`, consumida pelo `NotificacaoListener`. É o padrão **fila de trabalho** (point-to-point): a mensagem é entregue a **um** consumidor da fila, desacoplando produção e processamento.

```java
@RabbitListener(queues = "fila.notificacoes")
public void consumirNotificacao(String mensagem) { /* envia/registra notificação */ }
```

#### c) Pub/Sub de logs (centralização): extra

Todos os serviços publicam seus logs num Topic Exchange `logs.academico` (via `AmqpAppender` do Logback) com routing key `logs.<nome-do-serviço>`. Uma fila `logs.all` faz binding com `logs.#`, centralizando os logs de todo o sistema (outro uso de **Pub/Sub** para observabilidade distribuída).

### 2.3. Serviço de Nomes: Service Discovery (Eureka)

O **Eureka Server** (porta **8761**) é o **serviço de nomes**: cada microsserviço se **registra** na inicialização com seu `spring.application.name` e descobre os demais **pelo nome lógico**, sem URLs/IPs fixos.

```java
@EnableEurekaServer          // habilita o Eureka Server
@SpringBootApplication
public class EurekaServerApplication { ... }
```

```properties
# Cliente (cada serviço)
spring.application.name=matricula-service
eureka.client.service-url.defaultZone=http://localhost:8761/eureka/
eureka.client.register-with-eureka=true
eureka.client.fetch-registry=true
```

Com a descoberta ativa, o **OpenFeign** resolve o serviço **pelo nome** (com balanceamento de carga client-side), sem o chamador conhecer o endereço:

```java
@FeignClient(name = "aluno-service")          // Matrícula chama Aluno
public interface AlunoClient {
    @GetMapping("/alunos/{id}/existe") boolean alunoExiste(@PathVariable Long id);
    @GetMapping("/alunos/{id}/ativo")  boolean alunoAtivo(@PathVariable Long id);
}

@FeignClient(name = "disciplina-service")     // Turma chama Disciplina
public interface DisciplinaClient {
    @GetMapping("/disciplinas/{id}") void buscarPorId(@PathVariable Long id);
}
```

O nome `aluno-service` no `@FeignClient` é **o mesmo** `spring.application.name` registrado no Eureka: é assim que o nome lógico vira endereço em tempo de execução (**transparência de localização**).

---

## 3. Análise Conceitual

### 3.1. Descrição dos serviços e do fluxo

Fluxo central, **criar matrícula** (exercita os três mecanismos de uma vez):

1. **Matrícula → Aluno** (REST/Feign, via Eureka): valida se o aluno existe e está ATIVO.
2. **Matrícula → Turma** (gRPC, síncrono): `ReservarVaga`. Se não há vaga, a matrícula é interrompida.
3. **Matrícula** persiste a matrícula no seu H2. Se falhar, **compensa** liberando a vaga (gRPC).
4. **Matrícula → RabbitMQ** (Pub/Sub): publica `matricula.criada` no exchange `academico.events`.
5. **Notificação** e **Histórico** consomem o evento **de forma assíncrona e independente**, cada um na sua fila, gravando nos respectivos bancos.

O **cancelamento** é simétrico: `LiberarVaga` via gRPC mais o evento `matricula.cancelada`.

### 3.2. Principais conceitos aplicados

- **Arquitetura de microsserviços** com banco isolado por serviço (*database per service*), garantindo autonomia e baixo acoplamento.
- **Comunicação síncrona x assíncrona**: gRPC/REST (síncrona, requer resposta imediata) vs RabbitMQ (assíncrona, desacoplada no tempo).
- **RPC com IDL**: contrato `.proto` (Protocol Buffers) gerando *stubs* cliente/servidor.
- **Mensageria**: Exchange/Queue/Binding, Topic Exchange, routing keys, Pub/Sub e Work Queue.
- **Service Discovery / serviço de nomes**: registro e resolução por nome lógico (Eureka) + balanceamento client-side (Feign).
- **Tolerância a falhas**: tratamento de indisponibilidade (`*ServiceIndisponivelException`), publicação *best-effort* e **compensação (saga)** para manter a consistência sem transação distribuída.

### 3.3. Mapeamento entre código e teoria

| Conceito (teoria de sala) | Onde está no código |
|---------------------------|---------------------|
| Invocação remota (RPC) | `turma.proto`; `GrpcClientConfiguration` (stub cliente); `TurmaGrpcServiceImpl` (servidor) |
| IDL / serialização binária | `turma.proto` (Protocol Buffers) gera as classes `*Request`/`*Response`/`*Grpc` |
| Chamada síncrona | `turmaGrpcStub.reservarVaga(...)` em `MatriculaService` |
| Comunicação por REST remota | `AlunoClient`, `DisciplinaClient` (OpenFeign) |
| Serviço de nomes / discovery | `@EnableEurekaServer`; `eureka.client.*` nos `application.properties`; `@FeignClient(name=...)` |
| Transparência de localização | resolução por `spring.application.name` (Feign) e canal `"turma-service"` (gRPC) |
| Mensageria (produtor) | `rabbitTemplate.convertAndSend("academico.events", ...)` em `MatriculaService` |
| Mensageria, Pub/Sub (Topic Exchange) | `RabbitMQConfig` de Notificação/Histórico (Exchange + Queue + Binding) |
| Mensageria (consumidor assíncrono) | `@RabbitListener` em `EventoNotificacaoListener`, `EventoHistoricoListener` |
| Fila de trabalho (point-to-point) | `fila.notificacoes` + `NotificacaoListener` |
| Tolerância a falhas / saga | `compensarLiberandoVaga` / `compensarReservandoVaga`; publicação *best-effort* |

### 3.4. Transparências aplicadas

| Transparência | Como é alcançada no projeto |
|---------------|-----------------------------|
| **Acesso** | gRPC e Feign expõem chamadas remotas como métodos/interfaces locais; o código cliente não trata serialização nem protocolo de rede explicitamente. |
| **Localização** | Serviços são acessados por **nome lógico** (`aluno-service`, `turma-service`) resolvido pelo Eureka, sem nenhum IP/porta fixo no código de negócio. |
| **Falhas** | Indisponibilidades viram exceções tratadas; eventos *best-effort* não derrubam o fluxo; **compensação (saga)** mantém a consistência mesmo com falha parcial. |
| **Concorrência** | Cada serviço tem seu próprio H2; o RabbitMQ enfileira e entrega mensagens sem que produtores/consumidores coordenem acesso direto a dados compartilhados. |
| **Replicação / escala** | O balanceamento client-side do Feign sobre o registro do Eureka permite múltiplas instâncias de um serviço sob o mesmo nome lógico. |

---

## 4. Evidências

As evidências das comunicações ficam na pasta [`evidencias/`](evidencias/):

- **Service Discovery (Eureka):** painel em `http://localhost:8761` mostrando os 7 serviços `UP` registrados.
- **Mensageria (RabbitMQ):** painel em `http://localhost:15672` (guest/guest), ver [`evidencias/rabbitmq-management-home.png`](evidencias/rabbitmq-management-home.png), exibindo o exchange `academico.events`, as filas `queue.evento.notificacao`, `queue.evento.historico`, `fila.notificacoes` e os logs em `logs.all`.
- **RPC (gRPC):** logs do `turma-service` (`[TURMA-SERVICE] gRPC ReservarVaga chamado...`) e do `matricula-service` ao criar/cancelar uma matrícula.
- **Eventos (Pub/Sub):** logs `[NOTIFICACAO-SERVICE] Evento recebido` e `[HISTORICO-SERVICE] Evento registrado no histórico` após uma matrícula, comprovando que **uma** publicação chega a **dois** consumidores independentes.

**Onde cada conceito foi aplicado (resumo):**

| Conceito | Serviço(s) | Porta / recurso |
|----------|-----------|-----------------|
| Service Discovery | Eureka Server | `8761` |
| RPC síncrono (gRPC) | Matrícula (cliente) → Turma (servidor) | gRPC `9093` |
| REST/Feign (via discovery) | Matrícula → Aluno; Turma → Disciplina | HTTP |
| Pub/Sub (eventos) | Matrícula → Notificação + Histórico | exchange `academico.events` |
| Fila direta (work queue) | Notificação | fila `fila.notificacoes` |
| Pub/Sub de logs | Todos | exchange `logs.academico` → fila `logs.all` |

> Sugestão de roteiro de demonstração: subir o ambiente (seção 5) → abrir Eureka e RabbitMQ → criar uma matrícula → observar nos logs a chamada gRPC, e em seguida os dois consumidores recebendo o evento; cancelar a matrícula e repetir.

---

## 5. Subindo o ambiente com Docker Compose

O arquivo `docker-compose.yml` deste repositório sobe **todos** os serviços de uma vez (não é preciso rodar um a um). Só é necessário ter o **Docker** instalado: o build compila cada serviço em containers (Maven + Java 21), você não precisa instalar Java nem Maven localmente.

### 5.1. Clonar todos os repositórios na mesma pasta

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

### 5.2. Subir tudo

A partir da pasta `sd_academico_docs`:

```bash
docker compose up --build        # constrói e sobe tudo (use -d para rodar em segundo plano)
docker compose down              # derruba tudo
docker compose logs -f matricula-service   # acompanha os logs de um serviço
```

A primeira execução demora alguns minutos (compila os 7 serviços); depois fica em cache.

### 5.3. Portas (no host)

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

### 5.4. Documentação das APIs (Swagger / OpenAPI)

Cada serviço REST expõe sua documentação interativa via **Springdoc OpenAPI** (Swagger UI). Após subir o ambiente, acesse:

| Serviço | Swagger UI | OpenAPI JSON |
|---------|------------|--------------|
| matricula-service   | http://localhost:8081/swagger-ui.html | http://localhost:8081/v3/api-docs |
| aluno-service       | http://localhost:8082/swagger-ui.html | http://localhost:8082/v3/api-docs |
| disciplina-service  | http://localhost:8083/swagger-ui.html | http://localhost:8083/v3/api-docs |
| turma-service       | http://localhost:8084/swagger-ui.html | http://localhost:8084/v3/api-docs |
| notificacao-service | http://localhost:8085/swagger-ui.html | http://localhost:8085/v3/api-docs |
| historico-service   | http://localhost:8086/swagger-ui.html | http://localhost:8086/v3/api-docs |

> O Swagger UI permite testar os endpoints REST direto do navegador (ex.: criar/cancelar matrícula no `matricula-service`), sendo uma alternativa prática para a demonstração das comunicações.
