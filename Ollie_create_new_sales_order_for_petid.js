/**
 * @NApiVersion 2.1
 * @NScriptType UserEventScript
 */
define(['N/record', 'N/search', 'N/log'], function (record, search, log) {

  function afterSubmit(context) {
    try {
      if (context.type !== context.UserEventType.CREATE) return;

      var soId = context.newRecord.id;
      var petIdMap = {};
      var totalOriginalDiscount = 0;

      log.debug('Start', 'Processing Sales Order ID: ' + soId);

      // === 1. Fetch all line-level data ===
      var results = search.create({
        type: "salesorder",
        settings: [{ name: "consolidationtype", value: "ACCTTYPE" }],
        filters: [
          ["type", "anyof", "SalesOrd"],
          "AND", ["internalidnumber", "equalto", soId],
          "AND", ["mainline", "is", "F"],
          "AND", ["custbody_celigo_etail_order_id", "isnotempty", ""]
        ],
        columns: [
          "item",
          "discountamount",
          "custcol_ollie_pet_id",
          "quantity",
          "rate",
          "custcol_celigo_etail_order_line_id"
        ]
      }).run().getRange({ start: 0, end: 1000 });

      log.debug('Line Count from Search', results.length);

      for (var i = 0; i < results.length; i++) {
        var petId = results[i].getValue("custcol_ollie_pet_id");
        var discount = results[i].getValue("discountamount");

        var cleanDiscount = (discount || '').replace(/[()]/g, '');
        totalOriginalDiscount += parseFloat(cleanDiscount) || 0;

        if (!petId) continue;

        var lineData = {
          itemId: results[i].getValue("item"),
          quantity: parseFloat(results[i].getValue("quantity") || "0"),
          rate: parseFloat(results[i].getValue("rate") || "0"),
          discount: cleanDiscount,
          orderLineId: results[i].getValue("custcol_celigo_etail_order_line_id"),
          petId: petId
        };

        if (!petIdMap[petId]) {
          petIdMap[petId] = [];
        }
        petIdMap[petId].push(lineData);
      }

      log.audit('Total Sales Order Summary', {
        totalLines: results.length,
        totalOriginalDiscount: totalOriginalDiscount
      });

      var petIds = Object.keys(petIdMap);
      if (petIds.length <= 1) {
        log.debug('No split needed', 'Only one Pet ID found.');
        return;
      }

      var keepPetId = petIds[0];
      var removePetIds = petIds.slice(1);

      log.audit('Pet ID Group Count', {
        totalPetIDs: petIds.length,
        keepPetID: keepPetId,
        splittingPetIDs: removePetIds
      });

      var orderStatus = context.newRecord.getValue('orderstatus');
      var totalSplitCount = 0;
      var totalDiscountAppliedToNewSOs = 0;

      // === NEW: Base doc number (strip trailing ".N" if present) ===
      var baseTranId = String(context.newRecord.getValue('tranid') || '').replace(/\.\d+$/, '');      

      // === 3. Create new SOs for other Pet IDs ===
      for (var p = 1; p < petIds.length; p++) {
        var currPetId = petIds[p];
        var lines = petIdMap[currPetId];

        log.debug('Creating SO for Pet ID', currPetId);

        var newSO = record.copy({
          type: 'salesorder',
          id: soId,
          isDynamic: true
        });

        var lineCount = newSO.getLineCount('item');
        for (var j = lineCount - 1; j >= 0; j--) {
          var petId = newSO.getSublistValue({ sublistId: 'item', fieldId: 'custcol_ollie_pet_id', line: j });
          if (petId !== currPetId) {
            newSO.removeLine({ sublistId: 'item', line: j });
          }
        }

        if (newSO.getLineCount('item') === 0) {
          log.error('Skipping Save', 'No matching items for Pet ID: ' + currPetId);
          continue;
        }

        // Calculate discount to apply to this SO
        var totalDiscountForSO = 0;
        for (var d = 0; d < lines.length; d++) {
          totalDiscountForSO += parseFloat(lines[d].discount || 0);
        }

        if (totalDiscountForSO !== 0) {
          newSO.setValue({ fieldId: 'discountrate', value: -Math.abs(totalDiscountForSO) });
        } else {
          newSO.setValue({ fieldId: 'discountrate', value: '' });
          newSO.setValue({ fieldId: 'discountitem', value: '' });
        }

        newSO.setValue({ fieldId: 'custbody_ollie_created_from', value: soId });
        newSO.setValue({ fieldId: 'custbody_ds_warehouse_order', value: 1 });
        newSO.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: true });

        // ✅ Set order status from original
        if (orderStatus) {
          newSO.setValue({ fieldId: 'orderstatus', value: orderStatus });
        }

        // === NEW: Set custom tranid BEFORE save (Base.p) ===
        var newTranId = baseTranId + '.' + p;
        try {
          newSO.setValue({ fieldId: 'tranid', value: newTranId });
        } catch (preIdErr) {
          log.debug('Pre-save tranid set skipped', preIdErr);
        }
                

        var newId = newSO.save({ ignoreMandatoryFields: true });
        log.audit('Created New SO for Pet ID: ' + currPetId, 'New SO ID: ' + newId);

                // === NEW: Post-save fallback to enforce tranid ===
        try {
          record.submitFields({
            type: 'salesorder',
            id: newId,
            values: { tranid: newTranId },
            options: { enableSourcing: false, ignoreMandatoryFields: true }
          });
        } catch (postIdErr) {
          log.debug('Post-save tranid override failed (Allow Override may be off)', postIdErr);
        }
        

        // Load saved SO and read discountrate (to avoid mismatch)
        var savedSO = record.load({ type: 'salesorder', id: newId });
        var appliedDiscount = parseFloat(savedSO.getValue('discountrate')) || 0;
        totalDiscountAppliedToNewSOs += Math.abs(appliedDiscount);

        totalSplitCount++;
      }

      // === 4. Update original SO with remaining discount ===
      var finalDiscountOnOriginal = totalOriginalDiscount - totalDiscountAppliedToNewSOs;

      var original = record.load({ type: 'salesorder', id: soId, isDynamic: true });
      for (var i = original.getLineCount('item') - 1; i >= 0; i--) {
        var petId = original.getSublistValue({ sublistId: 'item', fieldId: 'custcol_ollie_pet_id', line: i });
        if (removePetIds.includes(petId)) {
          original.removeLine({ sublistId: 'item', line: i });
        }
      }

      log.audit('Original SO Update Summary', {
        totalOriginalDiscount: totalOriginalDiscount,
        discountAppliedToNewSOs: totalDiscountAppliedToNewSOs,
        finalDiscountOnOriginal: finalDiscountOnOriginal
      });

      if (finalDiscountOnOriginal !== 0) {
        original.setValue({ fieldId: 'discountrate', value: -Math.abs(finalDiscountOnOriginal) });
      } else {
        original.setValue({ fieldId: 'discountrate', value: '' });
        original.setValue({ fieldId: 'discountitem', value: '' });
      }

      original.setValue({ fieldId: 'custbody_ds_warehouse_order', value: 1 });
      original.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: true });

      var updatedId = original.save({ ignoreMandatoryFields: true });
      log.audit('Updated Original SO with Remaining Discount', 'Updated SO ID: ' + updatedId);

      log.audit('Split Complete', {
        totalPetIDs: petIds.length,
        splitOrdersCreated: totalSplitCount
      });

    } catch (e) {
      log.error('Pet ID Split Error', e);
    }
  }

  return {
    afterSubmit: afterSubmit
  };
});
