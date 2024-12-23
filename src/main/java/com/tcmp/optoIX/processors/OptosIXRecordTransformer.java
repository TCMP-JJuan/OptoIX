package com.tcmp.optoIX.processors;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.tcmp.optoIX.model.OptosIXRecord;  // Asegúrate de que esta clase esté definida
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class OptosIXRecordTransformer {

    public static OptosIXRecord transform(List<Document> docs) {

        Document legalEntityDoc = docs.get(0);
        Document tradeDoc = docs.get(1);

        OptosIXRecord optosIXRecord = new OptosIXRecord();

        // Mapeo de campos según las rutas definidas
        optosIXRecord.setInst("040044");
        optosIXRecord.setOficina("R"); // Este campo no tiene ruta definida en el ejemplo
        optosIXRecord.setLei(getEmbeddedString(legalEntityDoc, List.of("LegalEntityMessage", "legalEntity", "legalEntityIdentifier", "leiLegalEntityIdentifier"))); //?? LEI pero de Scotia Mex
        optosIXRecord.setContraparte(getEmbeddedString(legalEntityDoc, List.of("LegalEntityMessage", "legalEntity", "legalEntityIdentifier", "leiLegalEntityIdentifier"))); // LEI de contraparte o banxico ID si no hay LEI
        optosIXRecord.setFeConOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeDate"))); //yyyy/MM/dd
        optosIXRecord.setFeIniOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "startDate"))); //yyyy/MM/dd
        optosIXRecord.setFeVenOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "endDate"))); //yyyy/MM/dd

        optosIXRecord.setOtroDer(1); // Sin ruta definida

        //*********** FECHAS ***********
        String fecha1 = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "settlement", "settlementDate")); // Ejemplo de fecha 1
        String fecha2 = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "endDate")); // Ejemplo de fecha 2

        // Definir el formato de fecha
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // Convertir las fechas de String a LocalDate
        LocalDate date1 = LocalDate.parse(fecha1, formatter);
        LocalDate date2 = LocalDate.parse(fecha2, formatter);

        // Llamar al método para calcular días hábiles
        int diasHabiles = calcularDiasHabiles(date1, date2);

        optosIXRecord.setDiasLiq(diasHabiles);
        /// ////////////////////////////////////////////////////

        String posicion = "";
        String buySell = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "buySell"));

        if(buySell.equals("buy")) {
            posicion = "A";
        } else if(buySell.equals("sell")) {
            posicion = "E";
        } else {
            posicion = "UNDEFINED";
        }
        optosIXRecord.setPosicion(posicion); // si buy entonces A si sell E

        ///////////////////////////////////////////////////////

        String tipOpc = "";
        String optionType = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "optionType"));
        String isDigital = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "isDigital"));

        if(optionType.equals("call") && isDigital.equals("false")) {
            tipOpc = "C";
        } else if(optionType.equals("put") && isDigital.equals("false")) {
            tipOpc = "V";
        } else if(optionType.equals("call") && isDigital.equals("true")) {
            tipOpc = "OC";
        }else if(optionType.equals("put") && isDigital.equals("true")) {
            tipOpc = "OV";
        } else {
            tipOpc = "CH";
        }// ???????????? eoc CH (puede ser call o put)

        optosIXRecord.setTipOpc(tipOpc);

        ///////////////////////////////////////////////

        optosIXRecord.setTipOpc2(StringUtils.EMPTY); // Sin ruta definida

        /////////////////////

        String opcEjer = "";
        String optionStyle = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "exerciseStyle", "optionExerciseStyle"));

        if(optionStyle.equals("European")) {
            opcEjer = "E";
        } else if(optionStyle.equals("American")) {
            opcEjer = "A";
        } else if(optionStyle.equals("Bermudan")) {
            opcEjer = "B";
        } else {
            opcEjer = "UNDEFINED";
        }

        optosIXRecord.setOpcEjer(opcEjer);

        /////////////////

        String objetivo = "";
        String isHedgeTrade = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "regulatory", "isHedgeTrade"));

        objetivo = isHedgeTrade.equals("No Hedge") ? "NE" : "CO";

        optosIXRecord.setObjetivo(objetivo);
        /////////////////
        optosIXRecord.setImpBase(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingAmount"))));
        optosIXRecord.setMdaImp(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingCurrencyCode"))));
        optosIXRecord.setLiquida(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "settlement", "deliveryMethod")));
        optosIXRecord.setMdaLiquida(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "payOutCurrency"))));

        optosIXRecord.setFeFlu(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setFenFlu(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setDetFlujo(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setNuFlujo(null); // Sin ruta definida
        optosIXRecord.setIntFlujo(null); // Sin ruta definida
        optosIXRecord.setTasaRef(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setRevTref(null); // Sin ruta definida
        optosIXRecord.setAnio(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setFeRef(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setFacTasa(null); // Sin ruta definida
        optosIXRecord.setSpread(null); // Sin ruta definida
        optosIXRecord.setTasaFija(null); // Sin ruta definida

        ///////////////

        int calLiq = 0;
        int deliveryMethod = 0; // Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "settlement", "deliveryMethod"))); SE DEBE DEFINIR deliveryMethod CUAL ES SU VALOR EN DB
        //"TradeMessage.trade.product(FXOption).isDigital NO SE QUE HACER CON ESTOS CAMPOS SEGUN EXCEL
        //TradeMessage.trade.tradeHeader.settlement.deliveryMethod" NO SE QUE HACER CON ESTOS CAMPOS SEGUN EXCEL

        ///////////////////// FALTA LOGICA
        //si vanilla y cash settl 2
        //si digital 3
        //si vanilla y delivery 6

        optosIXRecord.setCalLiq(calLiq); // ????
        ////////////////

        optosIXRecord.setCalLiq2(1); // PROBABLMENTE 1
        optosIXRecord.setFacLiq(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "digitalFeature", "digitalRatePercentage")))); //????
        optosIXRecord.setDif(BigDecimal.valueOf(0)); // Sin ruta definida PROBABLEMENTE 0
        optosIXRecord.setFacLiq2(new BigDecimal(0)); // ?? PROBABLEMENTE 0
        optosIXRecord.setDif2(BigDecimal.valueOf(0)); // Sin ruta definida

        optosIXRecord.setMmriaPago("N"); // Sin ruta definida

        ///////////////////////
        int conTerm = 0;

        String knockType = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "knockType"));

        //"ANEXO AL
        //Double no touch
        //Up In
        //Down Out
        //Down In
        //Up Out
        //Out
        //In
        //Up and In
        //One touch up
        //One touch down"


        optosIXRecord.setConTerm(conTerm); //????????????
        ///  ////////////////

        optosIXRecord.setnCan(!knockType.isEmpty() ? 1 : 0);
        optosIXRecord.setQuanto("N"); // Sin ruta definida
        optosIXRecord.setTcQuant(BigDecimal.valueOf(000000000000.000000)); // Sin ruta definida

        //////////////////////////
        String premiumPaymentAmount = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "premiumPaymentAmount"));

        optosIXRecord.setPrima(!premiumPaymentAmount.isEmpty() ? new BigDecimal(000000000000000000001.00000000) : new BigDecimal(000000000000.000000));
        //////////////////////////
        optosIXRecord.setMdaPrima(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "premiumPaymentCurrency")))); // ??????

        ////////////////////////
        String premiumPaymentDate = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "premiumPaymentDate"));

        optosIXRecord.setFePrim(!premiumPaymentDate.isEmpty() ? premiumPaymentDate : ""); //???????????????? Si premiumpaymentdate<> vacio entonces 1 else 2

        ////////////////////////


        optosIXRecord.setPaqEst(54); // Sin ruta definida PENDIENTE DE CREAR
        optosIXRecord.setIdPaqEst(StringUtils.EMPTY); // Sin ruta definida PENDIENTE DE CREAR
        optosIXRecord.setConPaqEst(006); // Sin ruta definida CONTEO DE MISMO LOGICAL PACKAGE ID

        optosIXRecord.setTipDer("21"); // Sin ruta definida
        optosIXRecord.setSuby(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingCurrencyCode")));
        optosIXRecord.setCveTit(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingInstrumentName")));

        //////////////////////// ????????????????????????????????????????? PONER 0 PORQUE NO SE SABE COMO CALCULAR
        optosIXRecord.setIntEje(0); // Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "historicalFixingObservations", "observationDate")))); // ??? revisar si tiene un pago o menos entonces 0 en otro caso calcular la diferencia de fechas de pago
        optosIXRecord.setIntMon(0); // Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "historicalFixingObservations", "observationDate")))); // ?? si europea entonces 0 eoc calcular fixing final - fixing inicial
        optosIXRecord.setnMonSuby(0); // Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "historicalFixingObservations", "observationDate")))); // ?? si average entonces fecha fin- fecha inicio else 0
        optosIXRecord.setNuToEje(0); // Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "historicalFixingObservations", "observationDate")))); // contar fechas de pago
        //////////////////////////////////////////////////////////////////


        optosIXRecord.setNumIdOpSby(StringUtils.EMPTY); // Sin ruta definida
        optosIXRecord.setSeccionSwp(null); // Sin ruta definida
        optosIXRecord.setNumSuby(000000000); // Sin ruta definida
        optosIXRecord.setMdaSuby(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingCurrencyCode"))));
        optosIXRecord.setPrecioEjer(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "strikeRate"))));
        optosIXRecord.setTipPrecio(2); // Sin ruta definida
        optosIXRecord.setMdaPrecio(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "baseCurrencyCode"))));

        /////////////////////////////////////
        String underlyingInstrumentName = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "underlyingInstrumentName"));

        optosIXRecord.setCalEjercicio(underlyingInstrumentName.equals("Asian") ? 4 : 1);
        /////////////////

        optosIXRecord.setnEjercicio(000); // Sin ruta definida

        optosIXRecord.setPrecioEjer2(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "strikeRate")))); // ??????????????????????????????? Strike cuando sea estrategia

        optosIXRecord.setTipPrecio2(2); // Sin ruta definida
        optosIXRecord.setMdaPrecio2(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "baseCurrencyCode"))));
        optosIXRecord.setCalEjercicio2(underlyingInstrumentName.equals("Asian") ? 4 : 1);
        optosIXRecord.setnEjercicio2(000); // Sin ruta definida

        optosIXRecord.setPrecioInicial(BigDecimal.valueOf(00000000000000019.0387000000000)); // Sin ruta definida PENDIENTE DE CREAR
        optosIXRecord.setPreSup(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "barrierUpRate"))));
        optosIXRecord.setPreInf(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "barrierDownRate"))));
        optosIXRecord.setModPre(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "knockType")));

        //////////////////////////////
        //*********** FECHAS ***********
        String fechaIni = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "startDate"));
        String fechaFin = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "endDate"));

        // Definir el formato de fecha
        // Convertir las fechas de String a LocalDate
        LocalDate startDate = LocalDate.parse(fechaIni, formatter);
        LocalDate endDate = LocalDate.parse(fechaFin, formatter);

        // Llamar al método para calcular días hábiles
        int diferencia = calcularDiasHabiles(startDate, endDate);

        optosIXRecord.setIntMonBa(diferencia); //????  END DATE - START DATE SI ES 1 ENTONCES FIJO 1
        /////////////////////////////
        optosIXRecord.setFeIniVen(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "startDate")));
        optosIXRecord.setFeVenVen(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "endDate")));

        optosIXRecord.setPreSup2(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "barrierUpRate"))));
        optosIXRecord.setPreInf2(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "barrierDownRate"))));
        optosIXRecord.setModPre2(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "knockType")));

        optosIXRecord.setIntMonBa2(0); // ?????????????????????????????????????????????????????
        optosIXRecord.setFeIniVen2(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "startDate")));
        optosIXRecord.setFeVenVen2(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "endDate")));

        optosIXRecord.setRebate(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "barrierFeature", "rebateAmount"))));

        ///////////////////////////

        //"SA cuando es barrera continua
        //SS cuando es barrera discreta
        //N sin barrera"

        optosIXRecord.setCallable(StringUtils.EMPTY); // Sin ruta definida ????????????????????????
        //////////////////////////////

        //"S si se revisan parametros principales
        //N si no hay revision
        //TARF revisa parametros"


        optosIXRecord.setRevOp(StringUtils.EMPTY); // Sin ruta definida ????????????????????????
        ////////////////////////




        optosIXRecord.setBroker(Integer.parseInt(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "parties", "executingBroker", "partyName"))));
        optosIXRecord.setSocioLiq("07700"); // Sin ruta definida
        optosIXRecord.setCamCom(90); // Sin ruta definida
        optosIXRecord.setRepDev(90); // Sin ruta definida

        optosIXRecord.setAgCal(StringUtils.EMPTY); // Sin ruta definida ??????????? PENDIENTE DE CREAR SI SCOTIA- CLAVE SCOTIA ELSE CLAVE CPTY
        optosIXRecord.setUpi(StringUtils.EMPTY); // Sin ruta definida ?????????? ANEXO UPI
        optosIXRecord.setDelta(BigDecimal.valueOf(000000000000000000051.63490000)); // Sin ruta definida ???????????Si optiondelta<> vacio entonces 1 else 0

        //optosIXRecord.setVol(new BigDecimal(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product_FXOption", "fxOptionMeasures", "volatilitySurface"))));
        optosIXRecord.setVol(BigDecimal.valueOf(0)); // Sin ruta definida ???????????
        optosIXRecord.setIdContAnf(StringUtils.EMPTY); // Sin ruta definida TradeMessage.trade.product(FXOption).fxOptionMeasures.volatilitySurface   PENDING VOL BY TRADE

        optosIXRecord.setNumIdInst(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeIdentifiers", "tradeId", "id")));
        optosIXRecord.setNumId(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeIdentifiers", "originalUniqueTransactionId")));
        optosIXRecord.setUti(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeIdentifiers", "originalUniqueTransactionId")));

        //////////
        //"ON si es operación de hoy
        //A si es cambio de operación y alta
        //B si cancela operación "

        optosIXRecord.setIdentificador(StringUtils.EMPTY); // Sin ruta definida
        /////////////////////////////

        return optosIXRecord;
    }

    // Métodos auxiliares
    private static String getEmbeddedString(Document doc, List<String> path) {
        try {
            Object value = doc;
            for (String key : path) {
                if (value instanceof Map) {
                    value = ((Map<?, ?>) value).get(key);
                } else {
                    return StringUtils.EMPTY;
                }
            }
            return value != null ? value.toString() : StringUtils.EMPTY;
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }

    private static Double getEmbeddedDouble(Document doc, List<String> path) {
        String value = getEmbeddedString(doc, path);
        try {
            return Double.parseDouble(value.replace(",", "."));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public static int calcularDiasHabiles(LocalDate startDate, LocalDate endDate) {
        int diasHabiles = 0;

        // Iterar entre las dos fechas
        while (!startDate.isAfter(endDate)) {
            // Si el día es un lunes a viernes (días laborables)
            if (startDate.getDayOfWeek() != DayOfWeek.SATURDAY && startDate.getDayOfWeek() != DayOfWeek.SUNDAY) {
                diasHabiles++;
            }
            startDate = startDate.plusDays(1); // Avanzamos al siguiente día
        }

        return diasHabiles;
    }
}
