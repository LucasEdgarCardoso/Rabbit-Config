package com.furb.folha.rabbitmq.config.conections;

import jakarta.annotation.PostConstruct;
import org.springframework.amqp.core.*;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQConection {
    private static final String EXCHANGE_CALC_DIRECT = "calc.direct";
    private static final String EXCHANGE_EVENTOS_TOPIC = "eventos.topic";

    private static final String Q_CALC_REQUISICOES = "q.calc.requisicoes";
    private static final String Q_SALVAR_CALCULO = "q.salvar.calculo";

    private final AmqpAdmin amqpAdmin;

    public RabbitMQConection(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    private Queue fila(String nomeFila) {
        return new Queue(nomeFila, true, false, false);
    }

    private DirectExchange directExchange(String nomeExchange) {
        return new DirectExchange(nomeExchange);
    }

    private TopicExchange topicExchange(String nomeExchange) {
        return new TopicExchange(nomeExchange);
    }

    private Binding bindingDirect(Queue fila, DirectExchange exchange, String routingKey) {
        return new Binding(fila.getName(),
                Binding.DestinationType.QUEUE,
                exchange.getName(), // Use getName() ao invés de toString()
                routingKey,
                null);
    }

    private Binding bindingTopic(Queue fila, TopicExchange exchange, String routingKey) {
        return new Binding(fila.getName(),
                Binding.DestinationType.QUEUE,
                exchange.getName(), // Use getName() ao invés de toString()
                routingKey,
                null);
    }

    @PostConstruct
    private void inicializar() {
        // 1. Criar objetos das filas
        Queue q_calc_requisicoes = fila(Q_CALC_REQUISICOES);
        Queue q_salvar_calculo = fila(Q_SALVAR_CALCULO);

        // 2. Criar objetos dos exchanges
        DirectExchange exchange_calc_direct = directExchange(EXCHANGE_CALC_DIRECT);
        TopicExchange exchange_eventos_topic = topicExchange(EXCHANGE_EVENTOS_TOPIC);

        // 3. Declarar filas no RabbitMQ
        amqpAdmin.declareQueue(q_calc_requisicoes);
        amqpAdmin.declareQueue(q_salvar_calculo);

        // 4. Declarar exchanges no RabbitMQ
        amqpAdmin.declareExchange(exchange_calc_direct);
        amqpAdmin.declareExchange(exchange_eventos_topic);

        // 5. Criar bindings
        Binding binding_q_calc_requisicoes = bindingDirect(q_calc_requisicoes,
                exchange_calc_direct,
                "requisicao.salario");

        Binding binding_q_salvar_calculo = bindingTopic(q_salvar_calculo,
                exchange_eventos_topic,
                "cliente.calculado");

        // 6. Declarar bindings no RabbitMQ
        amqpAdmin.declareBinding(binding_q_calc_requisicoes);
        amqpAdmin.declareBinding(binding_q_salvar_calculo);
    }
}