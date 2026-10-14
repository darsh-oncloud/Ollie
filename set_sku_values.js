/**
 * @NApiVersion 2.0
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/log'], function(record, log) {

    function afterSubmit(context) {
        if (context.type === context.UserEventType.CREATE || context.type === context.UserEventType.EDIT) {
            var newRecord = context.newRecord;
            var ID = newRecord.id;

            var lineCount = newRecord.getLineCount({
                sublistId: 'item'
            });

            try {

            if (lineCount > 0) {
                var salesorderrecord = record.load({
                    type: 'salesorder',
                    id: ID,
                    isDynamic: true
                });

                var firstItemSKU = salesorderrecord.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'custcol_mhi_op_bcsku',
                    line: 0
                });
                log.debug('firstItemSKU', firstItemSKU);

                if (firstItemSKU) {
                  salesorderrecord.setValue({
                  fieldId: 'custbody_ollie_bc_product_sku',
                  value: firstItemSKU
                   });                      
                   
                var alphanumericRegex = /[a-zA-Z0-9]/;
                var firstAlphanumeric = firstItemSKU.match(alphanumericRegex);

                if (firstAlphanumeric) {
                    log.debug('firstAlphanumeric', firstAlphanumeric[0]);


                    salesorderrecord.setValue({
                        fieldId: 'custbody_ollie_order_cadence',
                        value: firstAlphanumeric[0] + ' Weeks'
                    });

                    salesorderrecord.save();
                  }
                }
            }
            
          } 
            catch (e) {
            log.error('Error', e);          
          }
        }
    }

    return {
        afterSubmit: afterSubmit
    };

});
