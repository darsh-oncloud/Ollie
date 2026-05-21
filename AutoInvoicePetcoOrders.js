/**
* @NApiVersion 2.x
* @NScriptType MapReduceScript
* @NModuleScope SameAccount
*
*
*/
define(['N/search', 'N/sftp','N/record', 'N/https', 'N/url', 'N/runtime','N/encode','N/log'],
/**
* @param {file} file
* @param {search} search
*/
function(search, sftp,record, https, url, runtime,encode,log) {

  function getInputData(context) {

    return search.create({
      type: "itemfulfillment",
      settings:[{"name":"consolidationtype","value":"ACCTTYPE"}],
      filters:
      [
        ["type","anyof","ItemShip"],
        "AND",
        // ["systemnotes.date","within","06/25/2024 12:00 am"],
        // "AND",
        ["systemnotes.type","is","T"],
        "AND",
        ["systemnotes.name","anyof","273"], //Integration user only
        "AND",
        ["systemnotes.context","anyof","RST"],
        "AND",
        ["mainline","is","T"],
        "AND",
        ["createdfrom.status","anyof","SalesOrd:F"], //Pending Billing
        "AND",
        ["name","anyof","322186","322687"]//Petco, Pet Food Customer
      ],
      columns:
      [
        search.createColumn({name: "internalid", label: "Internal ID"}),
        search.createColumn({name: "tranid", label: "Document Number"}),
        search.createColumn({name: "createdfrom", label: "Created From"}),
        search.createColumn({name: "trandate", label: "Date"}),
        search.createColumn({
          name: "statusref",
          join: "createdFrom",
          label: "Status"
        }),
        search.createColumn({name: "entity", label: "Name"})
      ]
    });



  }

  function map(context) {


    var rowJson = JSON.parse(context.value);
    log.debug({title:'rowJson',details:rowJson});

    var ifID = rowJson.id;
    var salesOrderID = rowJson.values['createdfrom']['value'];
    log.debug({title:'ifID',details:ifID});
    log.debug({title:'salesOrderID',details:salesOrderID});



      var invoiceCreation = record.transform({
        fromType: record.Type.SALES_ORDER,
        fromId: salesOrderID,
        toType: record.Type.INVOICE,
        isDynamic: true
      });
      log.debug({title:'invoiceCreation',details:invoiceCreation});

      invoiceCreation.setValue({
        fieldId:'customform',
        value:304 //SPS TEMPLATE - Invoice
      })

      invoiceCreation.setValue({
        fieldId:'custbodyintegrationstatus',
        value:1 //Ready
      })

      var invID = invoiceCreation.save();
      log.debug({title:'invID',details:invID});

  }

  /**
  * Executes when the reduce entry point is triggered and applies to each group.
  *
  * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
  * @since 2015.1
  */
  function reduce(context) {


  }
  function isEmpty(value) {
    if (value === null) {
      return true;
    } else if (value === undefined) {
      return true;
    } else if (value === '') {
      return true;
    } else if (value === ' ') {
      return true;
    } else if (value === 'null') {
      return true;
    } else {
      return false;
    }
  }




  return {
    getInputData: getInputData,
    map:map,
    reduce:reduce
  };

});
