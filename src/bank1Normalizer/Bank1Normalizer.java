package bank1Normalizer;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import dk.cphbusiness.connection.ConnectionCreator;
import java.io.IOException;
import org.w3c.dom.Document;
import utilities.xml.xmlMapper;

/**
 *
 * @author mhck
 */
public class Bank1Normalizer {

    private static final String IN_QUEUE = "bank_one_normalizer";
    private static final String OUT_QUEUE = "aggregator";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        Channel channelIn = creator.createChannel();
        Channel channelOut = creator.createChannel();
        channelIn.queueDeclare(IN_QUEUE, false, false, false, null);
        channelOut.queueDeclare(OUT_QUEUE, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channelIn);
        channelIn.basicConsume(IN_QUEUE, true, consumer);
//        String testMessage = "{\"ssn\":1605789787,\"loanAmount\":10.0,\"loanDuration\":360,\"rki\":false}"; //test sender besked til sig selv.
//        String testMessage = "{\"ssn\":1605789787,\"creditScore\":598,\"loanAmount\":10.0,\"loanDuration\":360}";
//        channel.basicPublish("", QUEUE_NAME, null, testMessage.getBytes()); // test

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            //channelIn.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.println(new String(delivery.getBody()));
            System.out.println("CorrelationID: " + delivery.getProperties().getCorrelationId());
            String message = translateMessage(delivery);
            BasicProperties prop = new BasicProperties().builder().correlationId(delivery.getProperties().getCorrelationId()).build();
            channelOut.basicPublish("", OUT_QUEUE, prop, message.getBytes());
        }
    }

    private static String translateMessage(QueueingConsumer.Delivery delivery) {
        String message = new String(delivery.getBody());
        Document doc = xmlMapper.getXMLDocument(message);
        doc.getFirstChild().appendChild(doc.createElement("bankName")).appendChild(doc.createTextNode("Glorious XML Bank of the Republic of Antartica"));
        //String endTarget = "<";
        //int startIndex = message.indexOf(startTarget) + startTarget.length() -1;
        //int endIndex = message.indexOf(endTarget);
        //String resultMessage = message.substring(0, startIndex) + "<bankName>" + "Glorious XML Bank of the Republic of Antartica" + "</bankName>" + message.substring(endIndex);
        return xmlMapper.getStringFromDoc(doc);
    }
}