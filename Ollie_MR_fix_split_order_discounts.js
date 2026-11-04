/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * Ollie | Fix Split Order Discounts
 *
 * TEST MODE:
 * Hardcoded Parent SO: 35855686
 * Document Number: 1797070.0
 *
 * Dry Run = TRUE
 *
 * No script parameters required.
 * No Sales Orders will be updated.
 *
 * Original discount validations and
 * proportional calculations are retained.
 */

define(['N/search', 'N/record', 'N/log'],
function (search, record, log) {

    var TEST_PARENT_SO_ID = '35855686';

    var DRY_RUN = true;

    var RATE_TOLERANCE = 0.01;

    var BLOCKED_STATUSES = [
        'fullyBilled',
        'closed',
        'cancelled'
    ];

    function round2(n) {
        return Math.round((Number(n) || 0) * 100) / 100;
    }

    function getParams() {
        return {
            dryRun: DRY_RUN,
            daysBack: 60
        };
    }

    // ============================================
    // 1. GET INPUT DATA - HARDCODED PARENT SO
    // ============================================

    function getInputData() {

        log.error('STEP 1 - SCRIPT STARTED', {
            parentSO: TEST_PARENT_SO_ID,
            dryRun: DRY_RUN
        });

        // No Saved Search needed for input.
        return [TEST_PARENT_SO_ID];
    }

    // ============================================
    // 2. MAP - PASS PARENT ID TO REDUCE
    // ============================================

    function map(context) {

        try {

            var parentId = String(context.value);

            log.error('STEP 2 - MAP STARTED', {
                parentId: parentId
            });

            context.write({
                key: parentId,
                value: parentId
            });

            log.error('STEP 2 - MAP COMPLETED', {
                parentId: parentId
            });

        } catch (e) {

            log.error('MAP ERROR', {
                name: e.name,
                message: e.message,
                stack: e.stack
            });

            throw e;
        }
    }

    // ============================================
    // 3. REDUCE - FIND CHILDREN AND FIX DISCOUNTS
    // ============================================

    function reduce(context) {

        var params = getParams();
        var parentId = String(context.key);

        log.error('STEP 3 - REDUCE STARTED', {
            parentId: parentId,
            dryRun: params.dryRun
        });

        try {

            // ====================================
            // A. FIND ALL CHILD SALES ORDERS
            // ====================================

            var ids = [parentId];

            log.error('SEARCHING CHILD SALES ORDERS', {
                parentId: parentId,
                field: 'custbody_ollie_created_from'
            });

            var childSearch = search.create({
                type: search.Type.SALES_ORDER,
                filters: [
                    ['mainline', 'is', 'T'],
                    'AND',
                    [
                        'custbody_ollie_created_from',
                        'anyof',
                        parentId
                    ]
                ],
                columns: [
                    'internalid',
                    'tranid',
                    'custbody_ollie_created_from'
                ]
            });

            var childCount = 0;

            childSearch.run().each(function (result) {

                var childId = String(result.id);

                if (ids.indexOf(childId) === -1) {
                    ids.push(childId);
                    childCount++;
                }

                log.error('CHILD SALES ORDER FOUND', {
                    childId: childId,
                    tranid: result.getValue('tranid'),
                    parentId: parentId
                });

                return true;
            });

            log.error('STEP 4 - GROUP IDENTIFIED', {
                parentId: parentId,
                childCount: childCount,
                totalOrders: ids.length,
                orderIds: ids
            });

            if (childCount === 0) {

                log.error('NO CHILD ORDERS FOUND', {
                    parentId: parentId,
                    message:
                        'No child Sales Orders linked to this parent. Check custbody_ollie_created_from.'
                });

                return;
            }

            // ====================================
            // B. LOAD ALL SALES ORDERS
            // ====================================

            var orders = ids.map(function (id) {

                var rec = record.load({
                    type: record.Type.SALES_ORDER,
                    id: id,
                    isDynamic: false
                });

                var rateStr = String(
                    rec.getValue('discountrate') || ''
                );

                var orderData = {
                    id: id,
                    isParent: String(id) === String(parentId),
                    tranid: rec.getValue('tranid'),
                    statusRef: rec.getValue('statusRef'),
                    orderStatus: rec.getValue('orderstatus'),
                    subtotal: round2(
                        rec.getValue('subtotal')
                    ),
                    total: round2(
                        rec.getValue('total')
                    ),
                    discountItem: rec.getValue('discountitem'),
                    rateStr: rateStr,
                    rate: round2(
                        Math.abs(parseFloat(rateStr) || 0)
                    )
                };

                log.error('STEP 5 - SALES ORDER DETAILS', orderData);

                return orderData;
            });

            // ====================================
            // C. CHECK FOR ZERO TOTAL
            // ====================================

            var anyZero = orders.some(function (o) {
                return o.total === 0;
            });

            log.error('STEP 6 - ZERO TOTAL CHECK', {
                parentId: parentId,
                anyZero: anyZero
            });

            if (!anyZero) {

                log.error('SKIPPED - NO ZERO TOTAL', {
                    parentId: parentId,
                    message: 'No Sales Order has a zero total.'
                });

                return;
            }

            // ====================================
            // D. ORIGINAL PATTERN VALIDATIONS
            // ====================================

            var summary = orders.map(function (o) {

                return o.tranid +
                    ' sub=' + o.subtotal +
                    ' rate=' + o.rateStr +
                    ' total=' + o.total +
                    ' status=' + o.statusRef;

            });

            var reasons = [];

            var D = orders[0].rate;

            var combinedSubtotal = round2(
                orders.reduce(function (s, o) {
                    return s + o.subtotal;
                }, 0)
            );

            if (orders.length < 2) {
                reasons.push('only one order found in group');
            }

            if (orders.some(function (o) {
                return !o.discountItem;
            })) {
                reasons.push('an order has no discount item');
            }

            if (orders.some(function (o) {
                return o.rateStr.indexOf('%') !== -1;
            })) {
                reasons.push('percentage discount');
            }

            if (orders.some(function (o) {
                return Math.abs(o.rate - D) > RATE_TOLERANCE;
            })) {
                reasons.push(
                    'discount rates differ between orders'
                );
            }

            if (!(D > 0)) {
                reasons.push('no discount amount');
            }

            if (D > combinedSubtotal) {
                reasons.push(
                    'discount ' + D +
                    ' is more than combined subtotal ' +
                    combinedSubtotal
                );
            }

            if (orders.some(function (o) {
                return BLOCKED_STATUSES.indexOf(
                    o.statusRef
                ) !== -1;
            })) {
                reasons.push(
                    'an order is billed/closed/cancelled'
                );
            }

            log.error('STEP 7 - VALIDATION RESULTS', {
                parentId: parentId,
                orderDiscount: D,
                combinedSubtotal: combinedSubtotal,
                reasons: reasons,
                orders: summary
            });

            if (reasons.length > 0) {

                log.error('NEEDS REVIEW - parent SO ' + parentId, {
                    reasons: reasons,
                    orders: summary
                });

                return;
            }

            // ====================================
            // E. ORIGINAL PROPORTIONAL CALCULATION
            // ====================================

            var allocated = 0;

            var children = orders.filter(function (o) {
                return !o.isParent;
            });

            var parent = orders.filter(function (o) {
                return o.isParent;
            })[0];

            children.forEach(function (o) {

                o.newDiscount = Math.min(
                    round2(
                        D * o.subtotal / combinedSubtotal
                    ),
                    o.subtotal
                );

                allocated = round2(
                    allocated + o.newDiscount
                );

            });

            parent.newDiscount = Math.max(
                0,
                Math.min(
                    round2(D - allocated),
                    parent.subtotal
                )
            );

            // ====================================
            // F. LOG OLD VS NEW DISCOUNT
            // ====================================

            var plan = [];

            var totalNewDiscount = 0;

            orders.forEach(function (o) {

                var expectedTotal = round2(
                    o.subtotal - o.newDiscount
                );

                totalNewDiscount = round2(
                    totalNewDiscount + o.newDiscount
                );

                var detail = {
                    tranid: o.tranid,
                    internalId: o.id,
                    isParent: o.isParent,
                    subtotal: o.subtotal,
                    oldDiscount: o.rate,
                    newDiscount: o.newDiscount,
                    oldTotal: o.total,
                    expectedNewTotal: expectedTotal
                };

                plan.push(detail);

                log.error('STEP 8 - DISCOUNT COMPARISON', detail);

            });

            log.error('STEP 9 - GROUP SUMMARY', {
                parentId: parentId,
                originalDiscount: D,
                combinedSubtotal: combinedSubtotal,
                totalAllocatedDiscount: totalNewDiscount,
                discountDifference: round2(
                    D - totalNewDiscount
                ),
                expectedCombinedTotal: round2(
                    combinedSubtotal - totalNewDiscount
                ),
                plan: plan
            });

            // ====================================
            // G. DRY RUN - NO UPDATES
            // ====================================

            if (params.dryRun) {

                log.error(
                    'DRY RUN - WOULD FIX PARENT SO ' + parentId,
                    {
                        orderDiscount: D,
                        plan: plan
                    }
                );

                return;
            }

            // ====================================
            // H. ORIGINAL SAVE LOGIC
            // ====================================

            orders.forEach(function (o) {

                var rec = record.load({
                    type: record.Type.SALES_ORDER,
                    id: o.id,
                    isDynamic: true
                });

                if (o.newDiscount > 0) {

                    rec.setValue({
                        fieldId: 'discountrate',
                        value: -o.newDiscount
                    });

                } else {

                    rec.setValue({
                        fieldId: 'discountrate',
                        value: ''
                    });

                    rec.setValue({
                        fieldId: 'discountitem',
                        value: ''
                    });
                }

                var savedId = rec.save({
                    ignoreMandatoryFields: true
                });

                log.error('SALES ORDER UPDATED', {
                    id: savedId,
                    tranid: o.tranid,
                    newDiscount: o.newDiscount
                });

            });

            log.audit('FIXED parent SO ' + parentId, {
                orderDiscount: D,
                plan: plan
            });

            context.write({
                key: 'fixed',
                value: parentId
            });

        } catch (e) {

            log.error('REDUCE ERROR - PARENT ' + parentId, {
                name: e.name,
                message: e.message,
                stack: e.stack
            });

            throw e;
        }
    }

    // ============================================
    // 4. SUMMARIZE
    // ============================================

    function summarize(summary) {

        log.error('STEP 10 - SUMMARIZE STARTED', {
            seconds: summary.seconds,
            dryRun: DRY_RUN,
            parentSO: TEST_PARENT_SO_ID
        });

        if (summary.inputSummary.error) {

            log.error(
                'INPUT ERROR',
                summary.inputSummary.error
            );
        }

        summary.mapSummary.errors.iterator().each(
            function (key, err) {

                log.error('MAP ERROR ' + key, err);

                return true;
            }
        );

        summary.reduceSummary.errors.iterator().each(
            function (key, err) {

                log.error('REDUCE ERROR ' + key, err);

                return true;
            }
        );

        var fixed = 0;

        summary.output.iterator().each(function () {
            fixed++;
            return true;
        });

        log.error('RUN COMPLETE', {
            parentSO: TEST_PARENT_SO_ID,
            groupsFixed: fixed,
            dryRun: DRY_RUN,
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
