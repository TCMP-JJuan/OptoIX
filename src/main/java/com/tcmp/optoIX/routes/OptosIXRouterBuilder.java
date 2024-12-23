package com.tcmp.optoIX.routes;

import com.tcmp.optoIX.processors.CsvWriter;
import com.tcmp.optoIX.processors.OptosIXRecordTransformer;
import com.tcmp.optoIX.services.OptoIXService;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component

public class OptosIXRouterBuilder extends RouteBuilder {

    @Autowired
    private OptoIXService optoIXService;

    @Autowired
    private OptosIXRecordTransformer optosIXRecordTransformer;

    @Autowired
    private CsvWriter csvWriter;

    @Override
    public void configure() throws Exception {
        log.info("Camel route is being initialized...");

        from("direct:start")
                .routeId("mongoServiceRoute")
                .log("Iniciando procesamiento de datos desde MongoService...")
                .bean(optoIXService, "printRealtimeData")
                .log("Datos obtenidos de MongoDB: ${body}")
                .bean(optosIXRecordTransformer)
                .log("Datos procesados con éxito desde TradeRecordTransformer.")
                .bean(csvWriter, "writeToCsv")
                .log("Datos exportados con éxito desde CsvWriter.")
                .end();
    }
}
