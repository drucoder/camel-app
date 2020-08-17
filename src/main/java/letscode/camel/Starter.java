package letscode.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultMessage;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

public class Starter {
    public static void main(String[] args) throws Exception {
        CamelContext camel = new DefaultCamelContext();

        camel.getPropertiesComponent().setLocation("classpath:application.properties");

        DataSource dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://localhost:5432/catalizator?user=postgres&password=123"
        );
        camel.getRegistry().bind("catalizator", dataSource);

        ProducerTemplate template = camel.createProducerTemplate();

        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file:{{from}}")
                        .routeId("File processing")
//                        .log(">>>> ${body}")
                        .convertBodyTo(String.class)
                        .to("log:?showBody=true&showHeaders=true")
                        .choice()
                        .when(exchange -> ((String) exchange.getIn().getBody()).contains("=a"))
                        .to("file:{{toA}}")
                        .when(exchange -> ((String) exchange.getIn().getBody()).contains("=b"))
                        .to("file:{{toB}}")
                        .otherwise()
                        .to("file:{{toA}}");
            }
        });

        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:base?period=60000")
                        .routeId("JDBC route")
                        .setHeader("key", constant(1))
                        .setBody(simple("select id, data from message where id > :?key"))
                        .to("jdbc:catalizator?useHeadersAsParameters=true")
                        .log(">>> ${body}")
                        .process(exchange -> {
                            Message in = exchange.getIn();
                            Object body = in.getBody();

                            DefaultMessage message = new DefaultMessage(exchange);
                            message.setHeaders(in.getHeaders());
                            message.setHeader("rnd", "kek");
                            message.setBody(body.toString() + "\n" + in.getHeaders().toString());

                            exchange.setMessage(message);
                        })
                        .toD("file://D:/dev/camel-app/files/toB?fileName=done-${date:now:yyyyMMdd}-${headers.rnd}.txt");
            }
        });

        camel.start();

        template.sendBody(
                "file://D:/dev/camel-app/files?filename=event-${date:now:yyyyMMdd-HH-mm}.html",
                "<hello>world!</hello>"
        );

        Thread.sleep(4_000);
        camel.stop();
    }
}
