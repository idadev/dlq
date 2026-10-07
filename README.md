# kafka-dlq-spring-boot-starter

Retraitement contrôlé des dead letter topics pour les services Spring Boot (3.x et 4.x) qui consomment Kafka avec
`@RetryableTopic` et `@DltHandler`.

Le service garde les annotations Spring Kafka. La lib prend en charge le reste :

- les topics de retry et de DLQ portent les noms donnés en configuration ;
- les containers DLQ ne démarrent pas avec l'application : ils sont lancés par cron, si les conditions de
  déclenchement sont réunies, ou à la demande ;
- une exécution ne traite que les messages présents dans la DLQ à son démarrage, puis arrête le container ;
- un message que le `@DltHandler` n'arrive pas à traiter est remis dans la DLQ pour une exécution ultérieure puis,
  après `max-attempts` échecs, déplacé dans un topic parking lot qui n'est jamais rejoué.

## Utilisation

```xml
<dependency>
    <groupId>io.github.idird</groupId>
    <artifactId>kafka-dlq-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```java
@Component
class OrdersListener {

    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = "orders")
    public void consume(OrderEvent event) { ... }

    @DltHandler
    public void reprocess(OrderEvent event) { ... }
}
```

```yaml
kafka:
  dlq:
    defaults:                    # valeurs communes, surchargeables par topic
      cron: "0 0 2 * * *"
      zone: Europe/Paris
      max-attempts: 3
    topics:
      orders:                    # nom logique (logs, événements, API)
        topic: orders            # topic consommé, par défaut la clé
        retry-topic: orders.retry
        dlt-topic: orders.dlq
        parking-lot-topic: orders.parking
        min-pending-messages: 10
      payments:
        dlt-topic: payments.dlq
        cron: "-"                # déclenchement manuel uniquement
```

Un service qui consomme plusieurs topics déclare une entrée par topic, avec un `@KafkaListener` par topic géré.

| Propriété (par topic ou sous `defaults`) | Défaut | Rôle |
|---|---|---|
| `cron`, `zone` | aucun | Planification. Sans cron, ou avec `-`, le retraitement n'est lancé qu'à la demande. |
| `min-pending-messages` | 1 | Nombre minimal de messages en attente dans la DLQ pour que le cron déclenche. |
| `idle-timeout` | 30s | L'exécution s'arrête si aucun message n'est reçu pendant cette durée. |
| `max-duration` | aucune | Durée maximale d'une exécution. |
| `max-attempts` | 3 | Échecs du `@DltHandler` avant envoi au parking lot. Sans `parking-lot-topic`, le message reste dans la DLQ. |

| Propriété par topic | Défaut | Rôle |
|---|---|---|
| `retry-topic` | suffixe de `@RetryableTopic` | Nom du topic de retry. S'il y en a plusieurs, l'index est ajouté (`orders.retry-0`). |
| `dlt-topic` | `<topic>-dlt` | Nom de la DLQ. |
| `parking-lot-topic` | aucun | Topic des messages en échec définitif, créé au démarrage (`kafka.dlq.auto-create-parking-lot-topics`). |

`kafka.dlq.enabled=false` désactive la lib.

## Conditions de déclenchement

Le cron ne lance une exécution que si `min-pending-messages` est atteint et si tous les beans
`DltReprocessingCondition` l'acceptent :

```java
@Bean
DltReprocessingCondition approved(ReplayApprovals approvals) {
    return context -> approvals.isApproved(context.name());
}
```

Un bean `DltRecordFilter` choisit les messages rejoués ; les autres sont remis dans la DLQ, inchangés.

## Pilotage

Le bean `DltReprocessingManager` expose `pendingMessages(name)`, `start(name)` (à la demande, sans conditions),
`trigger(name)` (avec conditions), `stop(name)` et `status(name)`. Chaque exécution publie un
`DltReprocessingStartedEvent` et un `DltReprocessingCompletedEvent` portant le bilan (traités, en échec, parqués,
ignorés).

Les messages remis en DLQ ou parqués portent les headers `x-dlt-reprocessing-attempts`,
`x-dlt-reprocessing-exception-fqcn`, `x-dlt-reprocessing-exception-message` et, au parking lot, `x-dlt-parked-from`.

## À savoir

- Un message en échec est republié en fin de DLQ : l'ordre des messages n'y est pas conservé.
- L'exception du `@DltHandler` n'est pas propagée une fois le message republié : la `DltStrategy` de
  `@RetryableTopic` n'intervient que si cette republication échoue.
- Si l'application définit son propre `RetryTopicComponentFactory`, les noms de topics ne sont plus imposés par la
  lib : `dlt-topic` doit alors correspondre au nom réel de la DLQ, sinon le démarrage échoue.
- Avec plusieurs instances, chaque instance évalue son cron ; les partitions de la DLQ sont réparties par le
  consumer group.

## Build

```
mvn verify              # Spring Boot 3.5, tests unitaires + tests d'intégration (Docker requis)
mvn verify -Pboot4      # idem avec Spring Boot 4.1
```
