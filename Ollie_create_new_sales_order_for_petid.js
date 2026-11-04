/**
 * @NApiVersion 2.1
 * @NScriptType UserEventScript
 *
 * Splits a Shopify sales order into one sales order per Pet ID.
 *
 * DISCOUNT SAFETY FIX (hybrid):
 *  - Normal case (99%): line-level discounts from the search add up to the
 *    order's real discount (discounttotal) -> split exactly as before.
 *  - Mismatch case (e.g. 1797070, where the search returned the full order
 *    discount on more than one row): ignore line values and split the real
 *    discount by each order's share of the subtotal.
 *  - Every order's discount is capped at its own subtotal, so no order can be
 *    zeroed out by another pet's discount.
 *  - Amounts are rounded to 2 decimals before comparing.
 */
define(['N/record', 'N/search', 'N/log'], function (record, search, log) {

  var MATCH_TOLERANCE = 0.05; // dollars

  function round2(n) {
    return Math.round((Number(n) || 0) * 100) / 100;
  }

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
        totalOriginalDiscount += Math.abs(parseFloat(cleanDiscount) || 0);

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
      totalOriginalDiscount = round2(totalOriginalDiscount);

      var petIds = Object.keys(petIdMap);
      if (petIds.length <= 1) {
        log.debug('No split needed', 'Only one Pet ID found.');
        return;
      }

      var keepPetId = petIds[0];
      var removePetIds = petIds.slice(1);

      // === 2. NEW: Compare line discounts with the order's real discount ===
      var origRec = record.load({ type: 'salesorder', id: soId });
      var headerDiscount = round2(Math.abs(parseFloat(origRec.getValue('discounttotal')) || 0));
      var origSubtotal = round2(origRec.getValue('subtotal'));
      var useLineDiscounts = Math.abs(totalOriginalDiscount - headerDiscount) <= MATCH_TOLERANCE;

      log.audit('Total Sales Order Summary', {
        totalLines: results.length,
        lineDiscountSum: totalOriginalDiscount,
        headerDiscount: headerDiscount,
        originalSubtotal: origSubtotal,
        mode: useLineDiscounts ? 'LINE (same as before)' : 'PROPORTIONAL (line discounts did not match)'
      });

      if (!useLineDiscounts) {
        // Line values are unreliable for this order; use the real discount instead
        totalOriginalDiscount = headerDiscount;
      }

      log.audit('Pet ID Group Count', {
        totalPetIDs: petIds.length,
        keepPetID: keepPetId,
        splittingPetIDs: removePetIds
      });

      var orderStatus = context.newRecord.getValue('orderstatus');
      var totalSplitCount = 0;
      var totalDiscountAppliedToNewSOs = 0;

      // Base doc number (strip trailing ".N" if present)
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
          var linePetId = newSO.getSublistValue({ sublistId: 'item', fieldId: 'custcol_ollie_pet_id', line: j });
          if (linePetId !== currPetId) {
            newSO.removeLine({ sublistId: 'item', line: j });
          }
        }

        if (newSO.getLineCount('item') === 0) {
          log.error('Skipping Save', 'No matching items for Pet ID: ' + currPetId);
          continue;
        }

        var childSubtotal = round2(newSO.getValue('subtotal'));

        // Calculate discount to apply to this SO
        var totalDiscountForSO = 0;
        if (useLineDiscounts) {
          for (var d = 0; d < lines.length; d++) {
            totalDiscountForSO += Math.abs(parseFloat(lines[d].discount || 0));
          }
        } else if (origSubtotal > 0) {
          totalDiscountForSO = totalOriginalDiscount * childSubtotal / origSubtotal;
        }
        totalDiscountForSO = Math.min(round2(totalDiscountForSO), childSubtotal); // NEW: cap

        if (totalDiscountForSO > 0) {
          newSO.setValue({ fieldId: 'discountrate', value: -totalDiscountForSO });
        } else {
          newSO.setValue({ fieldId: 'discountrate', value: '' });
          newSO.setValue({ fieldId: 'discountitem', value: '' });
        }

        log.audit('Split SO Discount', {
          petId: currPetId,
          childSubtotal: childSubtotal,
          childDiscount: totalDiscountForSO
        });

        newSO.setValue({ fieldId: 'custbody_ollie_created_from', value: soId });
        newSO.setValue({ fieldId: 'custbody_ds_warehouse_order', value: 1 });
        newSO.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: true });

        if (orderStatus) {
          newSO.setValue({ fieldId: 'orderstatus', value: orderStatus });
        }

        // Set custom tranid BEFORE save (Base.p)
        var newTranId = baseTranId + '.' + p;
        try {
          newSO.setValue({ fieldId: 'tranid', value: newTranId });
        } catch (preIdErr) {
          log.debug('Pre-save tranid set skipped', preIdErr);
        }

        var newId = newSO.save({ ignoreMandatoryFields: true });
        log.audit('Created New SO for Pet ID: ' + currPetId, 'New SO ID: ' + newId);

        // Post-save fallback to enforce tranid
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
        totalDiscountAppliedToNewSOs = round2(totalDiscountAppliedToNewSOs + Math.abs(appliedDiscount));

        totalSplitCount++;
      }

      // === 4. Update original SO with remaining discount ===
      var original = record.load({ type: 'salesorder', id: soId, isDynamic: true });
      for (var k = original.getLineCount('item') - 1; k >= 0; k--) {
        var origLinePetId = original.getSublistValue({ sublistId: 'item', fieldId: 'custcol_ollie_pet_id', line: k });
        if (removePetIds.indexOf(origLinePetId) !== -1) {
          original.removeLine({ sublistId: 'item', line: k });
        }
      }

      var remainingSubtotal = round2(original.getValue('subtotal'));
      var finalDiscountOnOriginal = round2(totalOriginalDiscount - totalDiscountAppliedToNewSOs);
      finalDiscountOnOriginal = Math.max(0, Math.min(finalDiscountOnOriginal, remainingSubtotal)); // NEW: cap

      log.audit('Original SO Update Summary', {
        totalOriginalDiscount: totalOriginalDiscount,
        discountAppliedToNewSOs: totalDiscountAppliedToNewSOs,
        remainingSubtotal: remainingSubtotal,
        finalDiscountOnOriginal: finalDiscountOnOriginal
      });

      if (finalDiscountOnOriginal > 0) {
        original.setValue({ fieldId: 'discountrate', value: -finalDiscountOnOriginal });
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
