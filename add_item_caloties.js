/**
* @NApiVersion 2.1
* @NScriptType UserEventScript
*/
define(['N/search', 'N/record', 'N/log'], function(search, record, log) {
  function afterSubmit(context) {
    if (context.type !== context.UserEventType.CREATE && context.type !== context.UserEventType.EDIT) {
      return;
    }
    
    try {
      var newRecord = context.newRecord;
      var salesOrderId = newRecord.id;
      
      var searchObj = search.create({
        type: "salesorder",
        filters: [
          ["type", "anyof", "SalesOrd"],
          "AND",
          ["mainline", "is", "F"],
          "AND",
          ["item.parent", "anyof", "333"],
          "AND",
          ["internalid", "anyof", salesOrderId]
        ],
        columns: [
          search.createColumn({
            name: "formulatext",
            summary: "MAX",
            formula: "SUBSTR({item.name}, LENGTH({item.name}) - 3, 4)",
          })
        ]
      });
      
      var results = searchObj.run().getRange({ start: 0, end: 1 });
      
      if (results.length > 0) {
        var totalCalories = results[0].getValue({ name: "formulatext", summary: "MAX" }) || 0;
        log.debug('Calories Total', totalCalories);
        
        record.submitFields({
          type: "salesorder",
          id: salesOrderId,
          values: {
            custbody_ollie_order_calories: totalCalories
          },
          options: {
            enableSourcing: false,
            ignoreMandatoryFields: true
          }
        });
      }
    } catch (e) {
      log.error('Error in afterSubmit - Calories Script', e.message);
    }
  }
  
  return {
    afterSubmit: afterSubmit
  };
});

