/**
* @NApiVersion 2.x
* @NScriptType UserEventScript
*/

define(['N/record', 'N/search'], function(record, search) {
  
  function afterSubmit(context) {
    if (context.type !== context.UserEventType.CREATE && context.type !== context.UserEventType.EDIT) {
      return;
    }
    
    var newRecord = context.newRecord;
    var salesOrderId = newRecord.id;
    try {
      var salesOrder = record.load({type:'salesorder',id:salesOrderId,isDynamic:true})
      
      var createdDate = salesOrder.getText({fieldId:'createddate'});
      if (createdDate && createdDate.indexOf(" ") != -1) {
        createdDate = createdDate.split(" ")[0];
      }
      var shipDate = ''; //salesOrder.getText({fieldId:'custbody_mhi_op_planshipdate'});
      log.debug('createdDate', createdDate)
      log.debug('shipDate', shipDate)
      
      var department = salesOrder.getValue({fieldId:'department'});
      var classField = salesOrder.getValue({fieldId:'class'});
      
      if (createdDate == '10/29/2024') {
        shipDate = '10/30/2024'
      }
      else if (createdDate == '10/29/2024'){
        shipDate = '10/29/2024'
      }
      else if (createdDate == '10/30/2024'){
        shipDate = '10/30/2024'
      }
      
      if (!shipDate) return;
      
      log.debug('dates', 'Dates Cond. Clear')      
      
      if (department == 107 && classField == 101) {
        log.debug('ClassDepartment', 'Class Department Clear')
        
        var dateParts = shipDate.split('/');
        var month = parseInt(dateParts[0], 10); // Month (10 for October)
        var day = parseInt(dateParts[1], 10);   // Day (30)
        var year = parseInt(dateParts[2], 10);  // Year (2024)
        
        // Create a new Date object (JavaScript Date constructor uses month index starting from 0)
        var newDate = new Date(year, month - 1, day);
        
        salesOrder.setValue({fieldId:'custbody_mhi_op_planshipdate',value:newDate})
        var welkit = false;

        var linecount = salesOrder.getLineCount({sublistId:'item'})

        for (var i = 0; i <linecount ; i++) {

          salesOrder.selectLine({sublistId:'item',line:i})
          var itemName = salesOrder.getCurrentSublistText({sublistId:'item',fieldId:'item'})

          if (itemName.indexOf('WelKit-')) {
            welkit = true;
            break;
          }
        }
        
        var itempresent = salesOrder.findSublistLineWithValue({sublistId:'item',fieldId:'item',value:423})
        if (itempresent == -1 && !welkit) {
          
          log.debug('Item', 'Item was not present')
          
          salesOrder.selectNewLine({ sublistId: 'item' });
          salesOrder.setCurrentSublistValue({
            sublistId: 'item',
            fieldId: 'item',
            value: 423 // Replace with actual internal ID of the item
          });
          salesOrder.setCurrentSublistValue({
            sublistId: 'item',
            fieldId: 'quantity',
            value: 1
          });
          salesOrder.setCurrentSublistValue({
            sublistId: 'item',
            fieldId: 'price',
            value: -1
          });
          salesOrder.setCurrentSublistValue({
            sublistId: 'item',
            fieldId: 'rate',
            value: 0
          });
          salesOrder.commitLine({ sublistId: 'item' });
          
          salesOrder.save();
          log.debug('Item added', 'Item with ID 423 added to Sales Order with ID ' + salesOrderId);
          
        }
        
      }
      
      
      
    } catch (e) {
      log.error('error',e)
    }
  }
  // // Define the search
  // var salesorderSearchObj = search.create({
  //   type: "salesorder",
  //   filters: [
  //     ["type", "anyof", "SalesOrd"],
  //     "AND",
  //     ["department", "anyof", "107"],
  //     "AND",
  //     ["class", "anyof", "101"],
  //     "AND",
  //     ["internalid", "anyof", salesOrderId]
  //   ],
  //   columns: [
  //     search.createColumn({ name: "internalid",summary: "GROUP", label: "Internal ID" }),
  //     search.createColumn({
  //       name: "formulanumeric",
  //       summary: "SUM",
  //       formula: "case when {item.internalid} = '423' then 1 else 0 end",
  //       label: "Formula (Numeric)"
  //     })
  //   ]
  // });
  // 
  // var searchResultCount = salesorderSearchObj.runPaged().count;
  // log.debug("salesorderSearchObj result count", searchResultCount);
  // salesorderSearchObj.run().each(function(result){
  // 
  //   WelcomeItem = result.getValue({
  //     name: "formulanumeric",
  //     summary: "SUM",
  //     formula: "case when {item.internalid} = '423' then 1 else 0 end",
  //     label: "Formula (Numeric)"
  //   })
  //   log.debug('WelcomeItem', WelcomeItem)
  // 
  //     return true;
  //   });
  // 
  //   if (WelcomeItem == 0) {
  // 
  //     try {
  //       var salesOrder = record.load({
  //         type: record.Type.SALES_ORDER,
  //         id: salesOrderId,
  //         isDynamic: true
  //       });
  // 
  //       // Add item to the sales order
  // 
  // 
  //     } catch (e) {
  //       log.error('Error adding item to sales order', e);
  //     }
  // 
  //   }
  // }
  // 
  // function verifyDateRange(createdDate, shipDate){
  // 
  //   if ((createdDate == '10/25/2024' && shipDate == '10/28/2024') || (createdDate == '10/26/2024' && shipDate == '10/29/2024') || (createdDate == '10/27/2024' && shipDate == '10/30/2024')) {
  // 
  //   }
  // 
  // 
  // }
  
  return {
    afterSubmit: afterSubmit
  };
  
});