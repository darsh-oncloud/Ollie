/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * Ollie | Fix Split Order Discounts
 *
 * Finds Pet ID split groups (original .0 order + its split orders) where the
 * full Shopify discount was applied to every order, which zeroes the totals
 * and blocks deposit creation. Re-splits that discount by each order's share
 * of the combined subtotal.
 *
 * A group is FIXED only when it matches the confirmed broken pattern:
 *   1. Every order in the group has a discount item and a flat (non-%) rate.
 *   2. Every order carries the SAME discount rate (D = full order discount).
 *   3. At least one order's total is 0.
 *   4. D is not more than the combined subtotal of the group.
 *   5. No order in the group is billed, closed or cancelled.
 * Any group with a $0 order that doesn't match is logged as NEEDS REVIEW.
 */
define(['N/search', 'N/record', 'N/log'], function (search, record, log) {

  var RATE_TOLERANCE = 0.01;
  var BLOCKED_STATUSES = ['fullyBilled', 'closed', 'cancelled'];

  function round2(n) {
    return Math.round((Number(n) || 0) * 100) / 100;
  }

  // === 1. Fetch qualifying split orders from Saved Search ===
  function getInputData() {

    log.audit('Start', 'Searching for Sales Orders with duplicated discounts');

    return search.create({
      type: "salesorder",
      settings: [{ name: "consolidationtype", value: "ACCTTYPE" }],
      filters: [
        ["type", "anyof", "SalesOrd"],
        "AND", ["mainline", "is", "T"],
        "AND", ["custbody_ollie_created_from", "noneof", "@NONE@"],
        "AND", ["custbody_ollie_created_from.type", "anyof", "SalesOrd"],
        "AND", ["custbody_ollie_created_from.totalamount", "equalto", "0.00"],
        "AND", ["datecreated", "onorafter", "09/01/2026 12:00 am"],
        "AND", ["custbody_ollie_created_from.custbody_celigo_etail_order_total_var", "notequalto", "0"],
        "AND", ["custbody_ollie_created_from.status","noneof","SalesOrd:A","SalesOrd:C","SalesOrd:G","SalesOrd:H"],
        "AND", ["status","noneof","SalesOrd:A","SalesOrd:C","SalesOrd:G","SalesOrd:H"]
      ],
      columns: [
        "internalid",
        "tranid",
        "custbody_celigo_etail_order_id",
        "custbody_ollie_created_from",
        search.createColumn({
          name: "internalid",
          join: "CUSTBODY_OLLIE_CREATED_FROM"
        })
      ]
    });
  }

  // === 2. Group split orders under their .0 order ===
  function map(context) {
    var result = JSON.parse(context.value);
    var parentId = result.values.custbody_ollie_created_from &&
      (result.values.custbody_ollie_created_from.value || result.values.custbody_ollie_created_from);

    if (!parentId) return;

    log.debug('Parent SO Found', {
      parentId: parentId,
      childId: result.id
    });

    context.write({ key: String(parentId), value: String(result.id) });
  }

  // === 3. Check and fix each group ===
  function reduce(context) {
    var parentId = context.key;

    var ids = [parentId];
    context.values.forEach(function (v) {
      if (ids.indexOf(v) === -1) ids.push(v);
    });

    // Fetch all child orders linked to this parent
    var childSearch = search.create({
      type: 'salesorder',
      filters: [
        ["mainline", "is", "T"],
        "AND", ["custbody_ollie_created_from", "anyof", parentId]
      ],
      columns: ["internalid"]
    });

    childSearch.run().each(function (result) {
      var childId = String(result.id);
      if (ids.indexOf(childId) === -1) ids.push(childId);
      return true;
    });

    log.audit('Processing Parent SO', {
      parentId: parentId,
      orderIds: ids
    });

    // Load the group
    var orders = ids.map(function (id) {
      var rec = record.load({ type: 'salesorder', id: id });
      var rateStr = String(rec.getValue('discountrate') || '');
      return {
        id: id,
        isParent: id === parentId,
        tranid: rec.getValue('tranid'),
        statusRef: rec.getValue('statusRef'),
        subtotal: round2(rec.getValue('subtotal')),
        total: round2(rec.getValue('total')),
        discountItem: rec.getValue('discountitem'),
        rateStr: rateStr,
        rate: round2(Math.abs(parseFloat(rateStr) || 0))
      };
    });

    var anyZero = orders.some(function (o) { return o.total === 0; });
    if (!anyZero) return; // healthy group, nothing to do

    var summary = orders.map(function (o) {
      return o.tranid + ' sub=' + o.subtotal + ' rate=' + o.rateStr + ' total=' + o.total + ' status=' + o.statusRef;
    });

    // --- Pattern checks ---
    var reasons = [];
    var D = orders[0].rate;
    var combinedSubtotal = round2(orders.reduce(function (s, o) { return s + o.subtotal; }, 0));

    if (orders.length < 2) reasons.push('only one order found in group');
    if (orders.some(function (o) { return !o.discountItem; })) reasons.push('an order has no discount item');
    if (orders.some(function (o) { return o.rateStr.indexOf('%') !== -1; })) reasons.push('percentage discount');
    if (orders.some(function (o) { return Math.abs(o.rate - D) > RATE_TOLERANCE; })) reasons.push('discount rates differ between orders');
    if (!(D > 0)) reasons.push('no discount amount');
    if (D > combinedSubtotal) reasons.push('discount ' + D + ' is more than combined subtotal ' + combinedSubtotal);
    if (orders.some(function (o) { return BLOCKED_STATUSES.indexOf(o.statusRef) !== -1; })) reasons.push('an order is billed/closed/cancelled');

    if (reasons.length) {
      log.audit('NEEDS REVIEW - parent SO ' + parentId, { reasons: reasons, orders: summary });
      return;
    }

    // --- Re-split D by subtotal: split orders first, .0 order takes the remainder ---
    var allocated = 0;
    var children = orders.filter(function (o) { return !o.isParent; });
    var parent = orders.filter(function (o) { return o.isParent; })[0];

    children.forEach(function (o) {
      o.newDiscount = Math.min(round2(D * o.subtotal / combinedSubtotal), o.subtotal);
      allocated = round2(allocated + o.newDiscount);
    });
    parent.newDiscount = Math.max(0, Math.min(round2(D - allocated), parent.subtotal));

    var plan = orders.map(function (o) {
      return o.tranid + ': (' + o.rate + ') -> (' + o.newDiscount + '), new total ' + round2(o.subtotal - o.newDiscount);
    });

    log.audit('Discount Correction Plan - Parent SO ' + parentId, {
      orderDiscount: D,
      plan: plan
    });

    // --- Save ---
    orders.forEach(function (o) {
      var rec = record.load({ type: 'salesorder', id: o.id, isDynamic: true });
      if (o.newDiscount > 0) {
        rec.setValue({ fieldId: 'discountrate', value: -o.newDiscount });
      } else {
        rec.setValue({ fieldId: 'discountrate', value: '' });
        rec.setValue({ fieldId: 'discountitem', value: '' });
      }
      rec.save({ ignoreMandatoryFields: true });
    });

    log.audit('FIXED parent SO ' + parentId, { orderDiscount: D, plan: plan });
    context.write({ key: 'fixed', value: parentId });
  }

  // === 4. Summary ===
  function summarize(summary) {
    var fixed = 0;
    summary.output.iterator().each(function () { fixed++; return true; });

    if (summary.inputSummary.error) {
      log.error('Input Error', summary.inputSummary.error);
    }

    summary.mapSummary.errors.iterator().each(function (key, err) {
      log.error('Map error ' + key, err);
      return true;
    });
    summary.reduceSummary.errors.iterator().each(function (key, err) {
      log.error('Reduce error for parent SO ' + key, err);
      return true;
    });

    log.audit('Run complete', {
      groupsFixed: fixed,
      seconds: summary.seconds
    });
  }

  return {
    getInputData: getInputData,
    map: map,
    reduce: reduce,
    summarize: summarize
  };
});