package com.tcmp.optoIX.processors;


import com.opencsv.CSVWriter;
import com.tcmp.optoIX.model.OptosIXRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.CharArrayWriter;
import java.util.List;

@Component
@Slf4j
public class CsvWriter {

    public void writeToCsv(Exchange exchange) {
        // Obtener la lista de TradeRecord desde el Exchange
        List<OptosIXRecord> optoIXRecords = exchange.getIn().getBody(List.class);

        log.info("Datos recibidos en writeToCsv: {}", optoIXRecords.toString());

        if (optoIXRecords == null || optoIXRecords.isEmpty()) {
            // Si la lista está vacía, no hacemos nada
            return;
        }

        // Usar CharArrayWriter para generar el CSV en memoria
        try (CharArrayWriter writer = new CharArrayWriter()) {
            CSVWriter csvWriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

            // Escribir los encabezados del CSV
            String[] header = {
                    "INST", "OFICINA", "LEI", "CONTRAPAR", "FE_CON_OPE", "FE_INI_OPE", "FE_VEN_OPE", "OTRO_DER", "DIASLIQ",
                    "POSICION", "TIP_OPC", "TIP_OPC2", "OPC_EJER", "OBJETIVO", "IMPBASE", "MDAIMP", "LIQUIDA", "MDALIQUIDA",
                    "FE_FLU", "FEN_FLU", "DET_FLUJO", "NU_FLUJO", "INT_FLUJO", "TASA_REF", "REV_TREF", "ANIO", "FE_REF",
                    "FAC_TASA", "SPREAD", "TASA_FIJA", "CAL_LIQ", "CAL_LIQ2", "FAC_LIQ", "DIF", "FAC_LIQ2", "DIF2",
                    "MMRIA_PAGO", "CON_TERM", "N_CAN", "QUANTO", "TC_QUANT", "PRIMA", "MDAPRIMA", "FE_PRIM", "PAQ_EST",
                    "ID_PAQ_EST", "CON_PAQ_EST", "TIP_DER", "SUBY", "CVE_TIT", "INT_EJE", "INT_MON", "N_MONSUBY", "NU_TO_EJE",
                    "NUM_ID_OP_SBY", "SECCION_SWP", "NUMSUBY", "MDASUBY", "PRECIOEJER", "TIP_PRECIO", "MDAPRECIO",
                    "CAL_EJERCICIO", "N_EJERCICIO", "PRECIOEJER2", "TIP_PRECIO2", "MDAPRECIO2", "CAL_EJERCICIO2",
                    "N_EJERCICIO2", "PRECIO_INICIAL", "PRE_SUP", "PRE_INF", "MOD_PRE", "INT_MON_BA", "FE_INI_VEN",
                    "FE_VEN_VEN", "PRE_SUP2", "PRE_INF2", "MOD_PRE2", "INT_MON_BA2", "FE_INI_VEN2", "FE_VEN_VEN2", "REBATE",
                    "CALLABLE", "REV_OP", "BROKER", "SOCIO_LIQ", "CAM_COM", "REP_DEV", "AG_CAL", "UPI", "DELTA", "VOL",
                    "ID_CONT_ANF", "NUM_ID_INST", "NUM_ID", "UTI", "IDENTIFICADOR"
            };


            csvWriter.writeNext(header);

            // Escribir cada TradeRecord como una nueva línea en el archivo CSV
            for (OptosIXRecord record : optoIXRecords) {
                String[] data = {
                        record.getInst(),
                        record.getOficina(),                    // Header: "OFICINA"
                        record.getLei(),                         // Header: "LEI"
                        record.getContraparte(),                 // Header: "CONTRAPARTE"
                        record.getFeConOpe(),                    // Header: "FE CON OPE"
                        record.getFeIniOpe(),                    // Header: "FE INI OPE"
                        record.getFeVenOpe(),                    // Header: "FE VEN OPE"
                        String.valueOf(record.getOtroDer()),                     // Header: "OTRO DER"
                        String.valueOf(record.getDiasLiq()),                     // Header: "DIAS LIQ"
                        record.getPosicion(),                    // Header: "POSICION"
                        record.getTipOpc(),                      // Header: "TIP OPC"
                        record.getTipOpc2(),                     // Header: "TIP OPC 2"
                        record.getOpcEjer(),                     // Header: "OPC EJER"
                        record.getObjetivo(),                    // Header: "OBJETIVO"
                        String.valueOf(record.getImpBase()),                     // Header: "IMP BASE"
                        String.valueOf(record.getMdaImp()),                      // Header: "MDA IMP"
                        record.getLiquida(),                     // Header: "LIQUIDA"
                        String.valueOf(record.getMdaLiquida()),                  // Header: "MDA LIQUIDA"
                        record.getFeFlu(),                       // Header: "FE FLU"
                        record.getFenFlu(),                      // Header: "FEN FLU"
                        record.getDetFlujo(),                    // Header: "DET FLUJO"
                        String.valueOf(record.getNuFlujo() == null ? "" : record.getNuFlujo().toString()),                     // Header: "NU FLUJO"
                        String.valueOf(record.getIntFlujo() == null ? "" : record.getIntFlujo().toString()),                    // Header: "INT FLUJO"
                        record.getTasaRef(),                     // Header: "TASA REF"
                        String.valueOf(record.getRevTref() == null ? "" : record.getRevTref().toString()),                     // Header: "REV TREF"
                        record.getAnio(),                        // Header: "ANIO"
                        record.getFeRef(),                       // Header: "FE REF"
                        String.valueOf(record.getFacTasa() == null ? "" : record.getFacTasa().toString()),                     // Header: "FAC TASA"
                        String.valueOf(record.getSpread() == null ? "" : record.getSpread().toString()),                      // Header: "SPREAD"
                        String.valueOf(record.getTasaFija() == null ? "" : record.getTasaFija().toString()),                    // Header: "TASA FIJA"
                        String.valueOf(record.getCalLiq()),                      // Header: "CAL LIQ"
                        String.valueOf(record.getCalLiq2()),                     // Header: "CAL LIQ 2"
                        String.valueOf(record.getFacLiq()),                      // Header: "FAC LIQ"
                        String.valueOf(record.getDif()),                         // Header: "DIF"
                        String.valueOf(record.getFacLiq2()),                     // Header: "FAC LIQ 2"
                        String.valueOf(record.getDif2()),                        // Header: "DIF 2"
                        record.getMmriaPago(),                   // Header: "MMRIA PAGO"
                        String.valueOf(record.getConTerm()),                     // Header: "CON TERM"
                        String.valueOf(record.getnCan()),                        // Header: "N CAN"
                        record.getQuanto(),                      // Header: "QUANTO"
                        String.valueOf(record.getTcQuant()),                     // Header: "TC QUANT"
                        String.valueOf(record.getPrima()),                       // Header: "PRIMA"
                        String.valueOf(record.getMdaPrima()),                    // Header: "MDA PRIMA"
                        record.getFePrim(),                      // Header: "FE PRIM"
                        String.valueOf(record.getPaqEst()),                      // Header: "PAQ EST"
                        record.getIdPaqEst(),                    // Header: "ID PAQ EST"
                        String.valueOf(record.getConPaqEst()),                   // Header: "CON PAQ EST"
                        record.getTipDer(),                      // Header: "TIP DER"
                        record.getSuby(),                        // Header: "SUBY"
                        record.getCveTit(),                      // Header: "CVE TIT"
                        String.valueOf(record.getIntEje()),                      // Header: "INT EJE"
                        String.valueOf(record.getIntMon()),                      // Header: "INT MON"
                        String.valueOf(record.getnMonSuby()),                    // Header: "N MON SUBY"
                        String.valueOf(record.getNuToEje()),                     // Header: "NU TO EJE"
                        record.getNumIdOpSby(),                  // Header: "NUM ID OPSBY"
                        String.valueOf(record.getSeccionSwp() == null ? "" : record.getSeccionSwp().toString()),                  // Header: "SECCION SWP"
                        String.valueOf(record.getNumSuby()),                     // Header: "NUM SUBY"
                        String.valueOf(record.getMdaSuby()),                     // Header: "MDA SUBY"
                        String.valueOf(record.getPrecioEjer()),                  // Header: "PRECIO EJER"
                        String.valueOf(record.getTipPrecio()),                   // Header: "TIP PRECIO"
                        String.valueOf(record.getMdaPrecio()),                   // Header: "MDA PRECIO"
                        String.valueOf(record.getCalEjercicio()),                // Header: "CAL EJERCICIO"
                        String.valueOf(record.getnEjercicio()),                  // Header: "N EJERCICIO"
                        String.valueOf(record.getPrecioEjer2()),                 // Header: "PRECIO EJER 2"
                        String.valueOf(record.getTipPrecio2()),                  // Header: "TIP PRECIO 2"
                        String.valueOf(record.getMdaPrecio2()),                 // Header: "MDA PRECIO 2"
                        String.valueOf(record.getCalEjercicio2()),               // Header: "CAL EJERCICIO 2"
                        String.valueOf(record.getnEjercicio2()),                 // Header: "N EJERCICIO 2"
                        String.valueOf(record.getPrecioInicial()),               // Header: "PRECIO INICIAL"
                        String.valueOf(record.getPreSup()),                      // Header: "PRE SUP"
                        String.valueOf(record.getPreInf()),                      // Header: "PRE INF"
                        record.getModPre(),                      // Header: "MOD PRE"
                        String.valueOf(record.getIntMonBa()),                    // Header: "INT MON BA"
                        record.getFeIniVen(),                    // Header: "FE INI VEN"
                        record.getFeVenVen(),                    // Header: "FE VEN VEN"
                        String.valueOf(record.getPreSup2()),                     // Header: "PRE SUP 2"
                        String.valueOf(record.getPreInf2()),                     // Header: "PRE INF 2"
                        record.getModPre2(),                     // Header: "MOD PRE 2"
                        String.valueOf(record.getIntMonBa2()),                   // Header: "INT MON BA 2"
                        record.getFeIniVen2(),                   // Header: "FE INI VEN 2"
                        record.getFeVenVen2(),                   // Header: "FE VEN VEN 2"
                        String.valueOf(record.getRebate()),                      // Header: "REBATE"
                        record.getCallable(),                    // Header: "CALLABLE"
                        record.getRevOp(),                       // Header: "REV OP"
                        String.valueOf(record.getBroker()),                      // Header: "BROKER"
                        record.getSocioLiq(),                    // Header: "SOCIO LIQ"
                        String.valueOf(record.getCamCom()),                      // Header: "CAM COM"
                        String.valueOf(record.getRepDev()),                      // Header: "REP DEV"
                        record.getAgCal(),                       // Header: "AG CAL"
                        record.getUpi(),                         // Header: "UPI"
                        String.valueOf(record.getDelta()),                       // Header: "DELTA"
                        String.valueOf(record.getVol()),                         // Header: "VOL"
                        record.getIdContAnf(),                   // Header: "ID CONT ANF"
                        record.getNumIdInst(),                   // Header: "NUM ID INST"
                        record.getNumId(),                       // Header: "NUM ID"
                        record.getUti(),                         // Header: "UTI"
                        record.getIdentificador()                // Header: "IDENTIFICADOR"
                };

                csvWriter.writeNext(data);
            }

            // Agregar el CSV generado al Exchange para enviarlo como respuesta
            exchange.getMessage().setBody(writer.toString());
            exchange.getMessage().setHeader("Content-Type", "text/csv");
            exchange.getMessage().setHeader("Content-Disposition", "attachment; filename=trade_records.csv");

        }
    }
}
