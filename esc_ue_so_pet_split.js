/**
 * @NApiVersion 2.1
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 *
 * esc_ue_so_pet_split.js — Ollie | SO Pet ID Split v2
 *
 * Splits a multi-pet Shopify sales order into one SO per pet, carrying each
 * pet's line-level discount lines with it, and cleans the parent down to the
 * kept pet. Idempotent: a pet that already has ANY other open SO for the same
 * Shopify order (matched on custbody_celigo_etail_order_id, not on
 * custbody_ollie_created_from) is never re-created — its lines are only
 * removed from the parent. That covers re-runs on EDIT, parents that were
 * deleted and re-imported, and children whose parent link was lost.
 *
 * Run scope lives at the top of CONFIG: ALLOWED_SO_IDS (NetSuite internal
 * ids) and ALLOWED_ETAIL_ORDER_IDS (Shopify order ids) are optional
 * allowlists — [] means no gate — and TRIGGER_TYPES picks the events. The
 * file as shipped is set for a one-order cleanup run (edit only); the
 * production values are in the comment beside them.
 *
 * Replaces: Ollie_create new sales order for petid.js (v1,
 * customscript_ollie_so_creation_for_petid). v1 summed `discountamount` across
 * line-level discount items, applied the sum as a header `discountrate` on top
 * of the discount lines still on the record, and swallowed the resulting
 * "total can not be negative" save error — leaving the parent un-cleaned and
 * duplicate pets shipping.
 *
 * Deploy: deployment on Sales Order, Event Type = (all), Execute As Role =
 * Administrator. DEACTIVATE the v1 deployment first — both fire on CREATE,
 * and if v1 runs first its children are created without discount lines.
 *
 * @version 2.0.4
 */
define(['N/record', 'N/search', 'N/log', 'N/error'], (record, search, log, error) => {

  // ───────────────────────────────────────────────────────────── constants ──

  const CONFIG = Object.freeze({
    VERSION: '2.0.4',

    // ── Run scope ─────────────────────────────────────────────────────────
    // RE-IMPORT RUN (current settings): three Shopify orders, create + edit.
    // v1 (script 3483) must be OFF while these import — both fire on CREATE.
    // PRODUCTION: ALLOWED_SO_IDS [], ALLOWED_ETAIL_ORDER_IDS [],
    //             TRIGGER_TYPES ['create', 'edit'].
    /** NetSuite internal IDs of SOs this build may touch. [] = no gate. */
    ALLOWED_SO_IDS: [],
    /** Shopify order IDs (custbody_celigo_etail_order_id, the long number) this build may touch. [] = no gate. */
    ALLOWED_ETAIL_ORDER_IDS: ['18913864744994', '18916315070498', '18916624400418', '18916951654434'],
    /** context.UserEventType values that trigger processing. */
    TRIGGER_TYPES: ['create', 'edit'],
    /** orderstatus codes the parent must be in: A = Pending Approval, B = Pending Fulfillment. */
    ALLOWED_ORDER_STATUSES: ['A', 'B'],
    // ──────────────────────────────────────────────────────────────────────

    SUBLIST: 'item',
    /** Sales-order statuses that do not count as "this pet already has an order". */
    DEAD_STATUSES: ['SalesOrd:C', 'SalesOrd:H'],
    WAREHOUSE_ORDER_VALUE: 1,
    DISCOUNT_LINE_PREFIX: 'Celigo_Discount_Line_',
    ITEM_TYPE_DISCOUNT: 'Discount'
  });

  const FIELD = Object.freeze({
    BODY: {
      TRANID: 'tranid',
      ORDER_STATUS: 'orderstatus',
      ETAIL_ORDER_ID: 'custbody_celigo_etail_order_id',
      CREATED_FROM: 'custbody_ollie_created_from',
      WAREHOUSE_ORDER: 'custbody_ds_warehouse_order',
      SENT_TO_PACCURATE: 'custbody_ue_sent_to_paccurate'
    },
    LINE: {
      LINE_ID: 'line',
      ITEM: 'item',
      ITEM_TYPE: 'itemtype',
      AMOUNT: 'amount',
      PET_ID: 'custcol_ollie_pet_id',
      ORDER_LINE_ID: 'custcol_celigo_etail_order_line_id',
      ITEMTYPE_ID: 'custcol_celigo_etail_itemtype_id',
      DISC_LINE: 'discline'
    }
  });

  const LOG = 'PetSplit v' + CONFIG.VERSION;

  // ───────────────────────────────────────────────────────────── entry ──────

  /**
   * @param {Object} context
   * @param {Record} context.newRecord
   * @param {string} context.type
   */
  const afterSubmit = (context) => {
    const soId = String(context.newRecord.id);
    const gate = evaluateGate(context, soId);
    if (!gate.ok) {
      log.debug(LOG + ' | Skip ' + soId, gate.reason);
      return;
    }

    log.audit(LOG + ' | Start', { soId, eventType: context.type });

    const so = record.load({ type: record.Type.SALES_ORDER, id: soId, isDynamic: true });
    const inventory = buildLineInventory(so);
    assignLineOwners(inventory);
    log.debug(LOG + ' | Inventory', inventory);

    const petIds = uniqueOwners(inventory);
    if (petIds.length <= 1) {
      log.audit(LOG + ' | No split needed', { soId, petIds });
      return;
    }

    const existingChildren = findExistingChildren(soId, str(so.getValue(FIELD.BODY.ETAIL_ORDER_ID)));
    const plan = buildPlan(soId, petIds, existingChildren, inventory, so);
    log.audit(LOG + ' | Plan', summarizePlan(plan));

    validatePlan(plan);

    const createdThisRun = [];
    try {
      plan.toCreate.forEach((petId) => {
        const childId = createChildSO(soId, petId, inventory, plan, so);
        createdThisRun.push({ petId, childId });
      });
      cleanupParent(so, inventory, plan);
    } catch (e) {
      rollbackChildren(createdThisRun);
      throw e;
    }

    log.audit(LOG + ' | Complete', {
      soId,
      keepPetId: plan.keepPetId,
      createdThisRun,
      skippedExistingChildren: plan.toSkip,
      parentLinesRemoved: plan.parentRemoveIndexes.length
    });
  };

  // ───────────────────────────────────────────────────────────── gate ───────

  /**
   * Decides whether this event should be processed.
   * @param {Object} context
   * @param {string} soId
   * @returns {{ok:boolean, reason?:string}}
   */
  const evaluateGate = (context, soId) => {
    if (!CONFIG.TRIGGER_TYPES.includes(context.type)) {
      return { ok: false, reason: 'event type ' + context.type };
    }
    if (CONFIG.ALLOWED_SO_IDS.length && !CONFIG.ALLOWED_SO_IDS.includes(soId)) {
      return { ok: false, reason: 'not in ALLOWED_SO_IDS' };
    }
    const rec = context.newRecord;
    const etailId = str(rec.getValue(FIELD.BODY.ETAIL_ORDER_ID));
    if (!etailId) {
      return { ok: false, reason: 'no Shopify order id' };
    }
    if (CONFIG.ALLOWED_ETAIL_ORDER_IDS.length && !CONFIG.ALLOWED_ETAIL_ORDER_IDS.includes(etailId)) {
      return { ok: false, reason: 'Shopify order ' + etailId + ' not in ALLOWED_ETAIL_ORDER_IDS' };
    }
    if (rec.getValue(FIELD.BODY.CREATED_FROM)) {
      return { ok: false, reason: 'record is itself a split child' };
    }
    const status = str(rec.getValue(FIELD.BODY.ORDER_STATUS));
    if (!CONFIG.ALLOWED_ORDER_STATUSES.includes(status)) {
      return { ok: false, reason: 'orderstatus ' + status + ' not in ' + CONFIG.ALLOWED_ORDER_STATUSES.join('/') };
    }
    return { ok: true };
  };

  // ───────────────────────────────────────────────────────────── inventory ──

  /**
   * @typedef {Object} LineInfo
   * @property {number} index        sublist index at load time
   * @property {string} lineId       NetSuite line id (the `line` field)
   * @property {string} itemId
   * @property {string} itemType
   * @property {number} amount
   * @property {string} petId        custcol_ollie_pet_id (may be '')
   * @property {string} orderLineId  Shopify line id (may be '')
   * @property {string} itemTypeId   Celigo itemtype tag (may be '')
   * @property {string} discLine     line id this discount applies to (may be '')
   * @property {string|null} owner   resolved pet id; null = stays on parent
   */

  /**
   * Reads every item line into a plain array so all decisions are made
   * against a snapshot, not a mutating sublist.
   * @param {Record} so dynamic-mode record
   * @returns {LineInfo[]}
   */
  const buildLineInventory = (so) => {
    const count = so.getLineCount({ sublistId: CONFIG.SUBLIST });
    const lines = [];
    for (let i = 0; i < count; i++) {
      lines.push({
        index: i,
        lineId: str(getLine(so, i, FIELD.LINE.LINE_ID)),
        itemId: str(getLine(so, i, FIELD.LINE.ITEM)),
        itemType: str(getLine(so, i, FIELD.LINE.ITEM_TYPE)),
        amount: num(getLine(so, i, FIELD.LINE.AMOUNT)),
        petId: str(getLine(so, i, FIELD.LINE.PET_ID)),
        orderLineId: str(getLine(so, i, FIELD.LINE.ORDER_LINE_ID)),
        itemTypeId: str(getLine(so, i, FIELD.LINE.ITEMTYPE_ID)),
        discLine: str(getLine(so, i, FIELD.LINE.DISC_LINE)),
        owner: null
      });
    }
    return lines;
  };

  /**
   * Resolves which pet each line belongs to.
   *  - Pet-tagged lines own themselves.
   *  - Discount lines resolve to the pet of their parent line, first via the
   *    Celigo tag `Celigo_Discount_Line_<shopifyLineId>`, then via `discline`.
   *  - Everything else (tax substitute, dry ice, packaging) stays on the parent.
   * @param {LineInfo[]} lines mutated in place
   */
  const assignLineOwners = (lines) => {
    const byOrderLineId = {};
    const byLineId = {};
    lines.forEach((l) => {
      if (l.petId) l.owner = l.petId;
      if (l.orderLineId) byOrderLineId[l.orderLineId] = l;
      if (l.lineId) byLineId[l.lineId] = l;
    });

    lines.forEach((l) => {
      if (l.owner || l.itemType !== CONFIG.ITEM_TYPE_DISCOUNT) return;

      let parent = null;
      if (l.itemTypeId.indexOf(CONFIG.DISCOUNT_LINE_PREFIX) === 0) {
        parent = byOrderLineId[l.itemTypeId.substring(CONFIG.DISCOUNT_LINE_PREFIX.length)] || null;
      }
      if (!parent && l.discLine) {
        parent = byLineId[l.discLine] || null;
      }
      if (parent && parent.owner) {
        l.owner = parent.owner;
      } else {
        log.error(LOG + ' | Unresolved discount line', l);
      }
    });
  };

  /**
   * @param {LineInfo[]} lines
   * @returns {string[]} distinct pet ids in first-seen line order
   */
  const uniqueOwners = (lines) => {
    const seen = [];
    lines.forEach((l) => {
      if (l.owner && !seen.includes(l.owner)) seen.push(l.owner);
    });
    return seen;
  };

  // ───────────────────────────────────────────────────────────── children ───

  /**
   * Finds every OTHER sales order for the same Shopify order and the pet ids
   * it carries. Keyed on the Shopify order id, not custbody_ollie_created_from,
   * so it still sees children whose parent was deleted and re-imported, and
   * any earlier parent that was never cleaned. Cancelled and closed orders are
   * ignored — a pet whose only other SO is dead still needs a child.
   * @param {string} soId      this parent's internal id (excluded)
   * @param {string} etailId   custbody_celigo_etail_order_id
   * @returns {Object<string,{childId:string, tranId:string}>} keyed by pet id
   */
  const findExistingChildren = (soId, etailId) => {
    const result = {};
    if (!etailId) {
      log.error(LOG + ' | Existing children', 'No Shopify order id on SO ' + soId + '; cannot dedupe.');
      return result;
    }
    search.create({
      type: search.Type.SALES_ORDER,
      filters: [
        [FIELD.BODY.ETAIL_ORDER_ID, search.Operator.IS, etailId], 'AND',
        ['internalid', search.Operator.NONEOF, soId], 'AND',
        ['status', search.Operator.NONEOF, CONFIG.DEAD_STATUSES], 'AND',
        ['mainline', search.Operator.IS, 'F'], 'AND',
        [FIELD.LINE.PET_ID, search.Operator.ISNOTEMPTY, '']
      ],
      columns: ['internalid', FIELD.BODY.TRANID, FIELD.LINE.PET_ID, FIELD.BODY.CREATED_FROM]
    }).run().each((r) => {
      const petId = str(r.getValue(FIELD.LINE.PET_ID));
      if (petId && !result[petId]) {
        result[petId] = {
          childId: str(r.getValue('internalid')),
          tranId: str(r.getValue(FIELD.BODY.TRANID)),
          createdFrom: str(r.getValue(FIELD.BODY.CREATED_FROM))
        };
      }
      return true;
    });
    log.audit(LOG + ' | Existing SOs for Shopify order ' + etailId, result);
    return result;
  };

  /**
   * @typedef {Object} Plan
   * @property {string} keepPetId
   * @property {string[]} toCreate      pets needing a new child SO
   * @property {string[]} toSkip        pets whose child already exists
   * @property {string[]} removePetIds  toCreate + toSkip
   * @property {number[]} parentRemoveIndexes
   * @property {number} parentRemainingTotal
   * @property {string} baseTranId
   * @property {number} nextSuffix
   * @property {Object} existingChildren
   */

  /**
   * @param {string} soId
   * @param {string[]} petIds
   * @param {Object} existingChildren
   * @param {LineInfo[]} inventory
   * @param {Record} so
   * @returns {Plan}
   */
  const buildPlan = (soId, petIds, existingChildren, inventory, so) => {
    const withoutChild = petIds.filter((p) => !existingChildren[p]);
    if (!withoutChild.length) {
      throw error.create({
        name: 'PET_SPLIT_ALL_PETS_HAVE_CHILDREN',
        message: 'Every pet on SO ' + soId + ' already has a child SO; refusing to empty the parent.'
      });
    }
    const keepPetId = withoutChild[0];
    const removePetIds = petIds.filter((p) => p !== keepPetId);
    const toSkip = removePetIds.filter((p) => !!existingChildren[p]);
    const toCreate = removePetIds.filter((p) => !existingChildren[p]);

    const parentRemoveIndexes = inventory
      .filter((l) => l.owner && removePetIds.includes(l.owner))
      .map((l) => l.index);
    const parentRemainingTotal = round2(inventory
      .filter((l) => !parentRemoveIndexes.includes(l.index))
      .reduce((sum, l) => sum + l.amount, 0));

    const baseTranId = str(so.getValue(FIELD.BODY.TRANID)).replace(/\.\d+$/, '');
    const usedSuffixes = Object.keys(existingChildren)
      .map((p) => num((existingChildren[p].tranId.match(/\.(\d+)$/) || [])[1]));
    const nextSuffix = Math.max(0, ...usedSuffixes) + 1;

    return {
      keepPetId, toCreate, toSkip, removePetIds,
      parentRemoveIndexes, parentRemainingTotal,
      baseTranId, nextSuffix, existingChildren
    };
  };

  /** @param {Plan} plan */
  const validatePlan = (plan) => {
    if (plan.parentRemainingTotal < 0) {
      throw error.create({
        name: 'PET_SPLIT_PARENT_NEGATIVE',
        message: 'Parent would total ' + plan.parentRemainingTotal + ' after cleanup; aborting before any change.'
      });
    }
    if (!plan.parentRemoveIndexes.length) {
      throw error.create({
        name: 'PET_SPLIT_NOTHING_TO_REMOVE',
        message: 'Multiple pets detected but no parent lines resolved for removal.'
      });
    }
  };

  /** @param {Plan} plan */
  const summarizePlan = (plan) => ({
    keepPetId: plan.keepPetId,
    toCreate: plan.toCreate,
    toSkip: plan.toSkip,
    parentRemoveIndexes: plan.parentRemoveIndexes,
    parentRemainingTotal: plan.parentRemainingTotal,
    baseTranId: plan.baseTranId,
    nextSuffix: plan.nextSuffix
  });

  /**
   * Copies the parent, keeps only lines owned by petId (kit, subscription,
   * and that pet's discount lines), and saves. Header discount is never set.
   * @param {string} soId
   * @param {string} petId
   * @param {LineInfo[]} inventory
   * @param {Plan} plan
   * @param {Record} parent
   * @returns {string} new SO internal id
   */
  const createChildSO = (soId, petId, inventory, plan, parent) => {
    const childTotal = round2(inventory
      .filter((l) => l.owner === petId)
      .reduce((sum, l) => sum + l.amount, 0));
    if (childTotal < 0) {
      throw error.create({
        name: 'PET_SPLIT_CHILD_NEGATIVE',
        message: 'Child for pet ' + petId + ' would total ' + childTotal + '.'
      });
    }

    const child = record.copy({ type: record.Type.SALES_ORDER, id: soId, isDynamic: true });

    // Never trust the parent's line positions on the copy. Inventory the copy
    // itself and remove by ITS indexes — record.copy is not guaranteed to
    // return lines in the parent's order.
    const copyInv = buildLineInventory(child);
    assignLineOwners(copyInv);
    log.debug(LOG + ' | Copy inventory for pet ' + petId, copyInv);

    const keep = copyInv.filter((l) => l.owner === petId);
    if (!keep.length) {
      throw error.create({
        name: 'PET_SPLIT_CHILD_EMPTY',
        message: 'Copy of SO ' + soId + ' has no lines owned by pet ' + petId + '.'
      });
    }
    const copyTotal = round2(keep.reduce((sum, l) => sum + l.amount, 0));
    if (copyTotal !== childTotal) {
      throw error.create({
        name: 'PET_SPLIT_COPY_MISMATCH',
        message: 'Copy of SO ' + soId + ' resolves pet ' + petId + ' to ' + copyTotal +
          ' but the parent resolves it to ' + childTotal + '. Refusing to build the child.'
      });
    }

    for (let i = copyInv.length - 1; i >= 0; i--) {
      if (copyInv[i].owner !== petId) {
        child.removeLine({ sublistId: CONFIG.SUBLIST, line: i });
      }
    }
    assertSingleOwner(child, petId, 'child (pre-save) for pet ' + petId);

    const tranId = plan.baseTranId + '.' + plan.nextSuffix++;
    child.setValue({ fieldId: FIELD.BODY.CREATED_FROM, value: soId });
    child.setValue({ fieldId: FIELD.BODY.WAREHOUSE_ORDER, value: CONFIG.WAREHOUSE_ORDER_VALUE });
    child.setValue({ fieldId: FIELD.BODY.SENT_TO_PACCURATE, value: true });
    const status = parent.getValue(FIELD.BODY.ORDER_STATUS);
    if (status) child.setValue({ fieldId: FIELD.BODY.ORDER_STATUS, value: status });
    trySetTranId(child, tranId);

    const childId = String(child.save({ ignoreMandatoryFields: true }));
    enforceTranId(childId, tranId);

    // Verify what NetSuite actually stored, not what we think we sent.
    try {
      verifySavedOwner(childId, petId, 'child ' + childId);
    } catch (e) {
      rollbackChildren([{ petId, childId }]);
      throw e;
    }

    log.audit(LOG + ' | Child created', { petId, childId, tranId, childTotal, lines: keep.length });
    return childId;
  };

  /**
   * Throws unless every owned line on the in-memory record belongs to petId.
   * @param {Record} rec dynamic record
   * @param {string} petId
   * @param {string} label for the error message
   */
  const assertSingleOwner = (rec, petId, label) => {
    const inv = buildLineInventory(rec);
    assignLineOwners(inv);
    const owners = uniqueOwners(inv);
    if (owners.length !== 1 || owners[0] !== petId) {
      throw error.create({
        name: 'PET_SPLIT_OWNER_CHECK_FAILED',
        message: label + ' resolves to pets [' + owners.join(', ') + '], expected [' + petId + '].'
      });
    }
  };

  /**
   * Reloads a saved SO and asserts it carries exactly one pet.
   * @param {string} id
   * @param {string} petId
   * @param {string} label
   */
  const verifySavedOwner = (id, petId, label) => {
    const saved = record.load({ type: record.Type.SALES_ORDER, id, isDynamic: true });
    assertSingleOwner(saved, petId, label + ' (post-save)');
  };

  /**
   * Removes the split pets' lines from the parent and saves. Header discount
   * fields are left untouched — discounts live on lines.
   * @param {Record} so dynamic-mode parent, loaded before any child was created
   * @param {LineInfo[]} inventory
   * @param {Plan} plan
   */
  const cleanupParent = (so, inventory, plan) => {
    const sorted = plan.parentRemoveIndexes.slice().sort((a, b) => b - a);
    sorted.forEach((i) => so.removeLine({ sublistId: CONFIG.SUBLIST, line: i }));

    so.setValue({ fieldId: FIELD.BODY.WAREHOUSE_ORDER, value: CONFIG.WAREHOUSE_ORDER_VALUE });
    so.setValue({ fieldId: FIELD.BODY.SENT_TO_PACCURATE, value: true });

    assertSingleOwner(so, plan.keepPetId, 'parent (pre-save)');

    const updatedId = String(so.save({ ignoreMandatoryFields: true }));
    verifySavedOwner(updatedId, plan.keepPetId, 'parent ' + updatedId);

    log.audit(LOG + ' | Parent cleaned', {
      soId: updatedId,
      removedIndexes: sorted,
      expectedTotal: plan.parentRemainingTotal
    });
  };

  /**
   * Deletes children created in this run so a failed parent save never leaves
   * a half-split order. Pre-existing children are never touched.
   * @param {{petId:string, childId:string}[]} created
   */
  const rollbackChildren = (created) => {
    created.forEach((c) => {
      try {
        record.delete({ type: record.Type.SALES_ORDER, id: c.childId });
        log.error(LOG + ' | Rolled back child', c);
      } catch (e) {
        log.error(LOG + ' | Rollback FAILED — manual delete required', { child: c, error: e });
      }
    });
  };

  // ───────────────────────────────────────────────────────────── helpers ────

  /** @param {Record} rec @param {string} tranId */
  const trySetTranId = (rec, tranId) => {
    try {
      rec.setValue({ fieldId: FIELD.BODY.TRANID, value: tranId });
    } catch (e) {
      log.debug(LOG + ' | Pre-save tranid skipped', e.message);
    }
  };

  /** @param {string} id @param {string} tranId */
  const enforceTranId = (id, tranId) => {
    try {
      record.submitFields({
        type: record.Type.SALES_ORDER,
        id,
        values: { [FIELD.BODY.TRANID]: tranId },
        options: { enableSourcing: false, ignoreMandatoryFields: true }
      });
    } catch (e) {
      log.debug(LOG + ' | Post-save tranid override skipped', e.message);
    }
  };

  /** @param {Record} so @param {number} i @param {string} fieldId */
  const getLine = (so, i, fieldId) => {
    try {
      return so.getSublistValue({ sublistId: CONFIG.SUBLIST, fieldId, line: i });
    } catch (e) {
      return '';
    }
  };

  const str = (v) => (v === null || v === undefined) ? '' : String(v);
  const num = (v) => { const n = parseFloat(v); return isNaN(n) ? 0 : n; };
  const round2 = (n) => Math.round(n * 100) / 100;

  return { afterSubmit };
});