package com.tcmp.optoIX.services;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class OptoIXService {

    @Autowired
    private MongoTemplate mongoTemplate;

    public void printRealtimeData(Exchange exchange) {
        // Obtener la base de datos
        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<Document> collection1 = database.getCollection("LegalEntity");
        MongoCollection<Document> collection2 = database.getCollection("Realtime");

        // Crear la proyección de los campos que queremos obtener
        Document projection1 = new Document("_id", 1)
                .append("LegalEntityMessage.legalEntity.legalEntityIdentifier.leiLegalEntityIdentifier", 1);

        Document projection2 = new Document("TradeMessage.header.batchId", 1)
                .append("TradeMessage.trade.tradeHeader.tradeDate", 1)
                .append("TradeMessage.trade.product_FXOption.startDate", 1)
                .append("TradeMessage.trade.product_FXOption.endDate", 1)
                .append("TradeMessage.trade.tradeHeader.settlement.settlementDate", 1)
                .append("TradeMessage.trade.product_FXOption.buySell", 1)
                .append("TradeMessage.trade.product_FXOption.optionType", 1)
                .append("TradeMessage.trade.product_FXOption.isDigital", 1)
                .append("TradeMessage.trade.product_FXOption.exerciseStyle.optionExerciseStyle", 1)
                .append("TradeMessage.trade.tradeHeader.regulatory.isHedgeTrade", 1)
                .append("TradeMessage.trade.product_FXOption.underlyingAmount", 1)
                .append("TradeMessage.trade.product_FXOption.underlyingCurrencyCode", 1)
                .append("TradeMessage.trade.tradeHeader.settlement.deliveryMethod", 1)
                .append("TradeMessage.trade.product_FXOption.payOutCurrency", 1)
                .append("TradeMessage.trade.product_FXOption.payOffCurrency", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.digitalFeature.digitalRatePercentage", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.knockType", 1)
                .append("TradeMessage.trade.product_FXOption.premiumPaymentAmount", 1)
                .append("TradeMessage.trade.product_FXOption.premiumPaymentCurrency", 1)
                .append("TradeMessage.trade.product_FXOption.premiumPaymentDate", 1)
                .append("TradeMessage.trade.product_FXOption.underlyingInstrumentName", 1)
                .append("TradeMessage.trade.product_FXOption.historicalFixingObservations.observationDate", 1)
                .append("TradeMessage.trade.product_FXOption.strikeRate", 1)
                .append("TradeMessage.trade.product_FXOption.baseCurrencyCode", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.barrierUpRate", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.barrierDownRate", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.startDate", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.endDate", 1)
                .append("TradeMessage.trade.product_FXOption.barrierFeature.rebateAmount", 1)
                .append("TradeMessage.trade.parties.executingBroker.partyName", 1)
                .append("TradeMessage.trade.product_FXOption.fxOptionMeasures.volatilitySurface", 1)
                .append("TradeMessage.trade.tradeHeader.tradeIdentifiers.tradeId", 1)
                .append("TradeMessage.trade.tradeHeader.tradeIdentifiers.originalUniqueTransactionId", 1)
                .append("_id", 0);


        // Consulta para obtener los documentos con proyección
        List<Document> results1 = collection1.find(new Document()).projection(projection1).into(new ArrayList<>());
        List<Document> results2 = collection2.find(new Document()).projection(projection2).into(new ArrayList<>());

        List<Document> combinedResults = new ArrayList<>();

        // Agregar los resultados de la primera colección
        combinedResults.addAll(results1);

        // Agregar los resultados de la segunda colección
        combinedResults.addAll(results2);

        // Loguear los resultados
        combinedResults.forEach(doc -> log.info("Datos: {}", doc.toJson()));

        // Colocar los resultados en el cuerpo del Exchange
        exchange.getIn().setBody(combinedResults);
    }
}
