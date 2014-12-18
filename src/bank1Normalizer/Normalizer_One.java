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
public class Normalizer_One {

    private static final String IN_QUEUE = "bank_one_normalizer_gr1";
    private static final String OUT_QUEUE = "aggregator_gr1";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        Channel channelIn = creator.createChannel();
        Channel channelOut = creator.createChannel();
        channelIn.queueDeclare(IN_QUEUE, false, false, false, null);
        channelOut.queueDeclare(OUT_QUEUE, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channelIn);
        channelIn.basicConsume(IN_QUEUE, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            System.out.println(new String(delivery.getBody()));
            System.out.println("CorrelationID: " + delivery.getProperties().getCorrelationId());
            String message = translateMessage(delivery);
            BasicProperties prop = new BasicProperties().builder().correlationId(delivery.getProperties().getCorrelationId()).build();
            System.out.println("Publish stuff?: " + message);
            channelOut.basicPublish("", OUT_QUEUE, prop, message.getBytes());
            System.out.println("test er det koldt her ");
            
        }
    }

    private static String translateMessage(QueueingConsumer.Delivery delivery) {
        String message = new String(delivery.getBody());
        Document doc = xmlMapper.getXMLDocument(message);
        doc.getFirstChild().appendChild(doc.createElement("bankName")).appendChild(doc.createTextNode("Glorious XML Bank of the Republic of Antartica"));
        return xmlMapper.getStringFromDoc(doc);
    }
}