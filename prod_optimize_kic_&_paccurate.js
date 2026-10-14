/**
* @NApiVersion 2.1
* @NScriptType UserEventScript
*/
define(['N/https', 'N/log', 'N/record', 'N/search', 'N/runtime'], (https, log, record, search, runtime) => {

    const KEEP_IT_COOL_BASE_URL = 'https://api.keepit.cool/v1/shipments/';
    const KEEP_IT_COOL_AUTH = 'Api-Key AAjYZcBE.FJ9sbVmt0948Kpqb0QhHnhDvkZmFURIf';
    const DRY_ICE_ITEM_ID = 798;
    const WELCOM_KIT_ITEM = 484;
    const SB_SAMPLEBOX_KIT_ITEM = 833;

    const DISTRIBUTION_CENTER_ID = '8fdc588d-e3f1-40e1-ab62-265bb7f78453';
    const PA_DISTRIBUTION_CENTER_ID = '9bc6845f-0b4f-433f-8da0-b989fe9e68d0';
    const NV_DISTRIBUTION_CENTER_ID = '1351c3a0-6fb9-4999-b206-831b84d23df1';
    const FL_FC_JACKSONVILLE_DISTRIBUTION_CENTER_ID = 'b95a183b-c649-4b17-8ca9-5b7d54baafb6';
    const IL_DISTRIBUTION_CENTER_ID = '36540d56-82ec-43be-8825-dfae09de57b8';

    const NEXT_DAY_SHIPPING_METHOD = 'af6d8e72-f744-4c37-b2c3-4ad1e62ed1fe';
    const UPS_SHIPPING_METHOD = 'bf3dba05-a01b-4a19-9f28-76decc7914d2';
    const UPS_NEXT_DAY_SHIPPING_METHOD = '9f64b845-23fa-46ec-bee4-9ef07e686744';
    const JITSU_SHIPPING_METHOD = 'af6d8e72-f744-4c37-b2c3-4ad1e62ed1fe';

    const OKC_FC_LOCATION = 4;
    const PA_LOCATION = 2;
    const NV_LOCATION = 3;
    const FL_FC_LOCATION = 6;
    const IL_FC_LOCATION = 5;

    const PACCURATE_API_URL = 'https://api.paccurate.io';
    const PACCURATE_API_KEY = 'apikey RFLynVP2zTRFtZh1Rmw24GMuDHYPYFVL5_sJgOQHhqtpRpq5ductxLj-DbZoOYHx';
    const PETCO_CUSTOMER_ID = 322186;

    const PA_NEXT_DAY_SHIPPING_METHOD = 'ecfb3890-12f9-48b7-9a66-f139118328a3';
    const PA_UPS_SHIPPING_METHOD = '5d7cf127-341c-4ec6-a764-acc36cb2fac2';
    const PA_UPS_NEXT_DAY_SHIPPING_METHOD = '545f07cf-fc58-4280-b2e3-96d885d53bce';
    const PA_ONTRAC_GROUND_SHIPPING_METHOD = '716cada4-745d-4b12-bc4e-bd76e2843298';

    const NV_UPS_SHIPPING_METHOD = '58e9e6f3-56fb-4afe-a62d-170df3ca7d42'; //NV → UPS Ground
    const NV_NEXT_DAY_SHIPPING_METHOD = '22e62afb-daeb-4316-9b34-8595195296c7'; //NV → Jitsu Next Day
    const NV_UPS_NEXT_DAY_SHIPPING_METHOD = '379bfdb0-f3ce-4146-b093-b4024ec44dca'; //NV → UPS Next Day Air
    const NV_ONTRACK_GROUND_SHIPPING_METHOD = '9329ea8c-d088-4beb-8606-4ea67794b7ef'; //NV → OnTrac Ground
    const NV_FEDEX_HOME_DELIVERY_SHIPPING_METHOD = '8b951686-455f-4345-a601-5426d0340e4b'; //NV → FedEx Home Delivery
    const NV_FEDEX_TODAY_SHIPPING_METHOD = 'bd00ece5-3e13-49c3-a064-e37548f04490'; //NV → FedEx 2Day
    const NV_FEDEX_OVERNIGHT_SHIPPING_METHOD = '8a334569-aabf-4d87-9ab8-ed4a02d26ae0'; //NV → FedEx Standard Overnight

    const FL_FC_UPS_SHIPPING_METHOD = '5ef04e40-fbc8-47f5-a645-3943ca98c1df';
    const FL_FC_UPS_NEXT_DAY_SHIPPING_METHOD = 'd5a3a115-7d4d-47a6-9b36-3c355ecdb614';

    const IL_FC_UDS_SHIPPING_METHOD = '8ff9c532-dfc8-4977-8f73-148433c559ac';
    const IL_FC_UPS_SHIPPING_METHOD = '07408d01-8d8c-40d2-b18f-63eead351b89';
    const IL_FC_UPS_NEXT_DAY_SHIPPING_METHOD = 'b79ecca7-e877-4759-ba45-8397bd8e81d0';

    const LINER_DIMENSIONS = {
        'liner_0': { x: 6.6, y: 12, z: 15.5, refId: 0 },
        'liner_1': { x: 9.8, y: 12, z: 15.5, refId: 1 },
        'liner_2': { x: 13.4, y: 12, z: 15.5, refId: 2 },
        'liner_3': { x: 16.4, y: 12, z: 15.5, refId: 3 },
        'liner_4': { x: 19.4, y: 12, z: 15.5, refId: 4 }
    };

    const afterSubmit = (context) => {
        try {
            if (context.type === context.UserEventType.DELETE || context.type === context.UserEventType.XEDIT) return;

            const soId = context.newRecord.id;
            const soRec = record.load({ type: record.Type.SALES_ORDER, id: soId, isDynamic: true });
            const customerId = soRec.getValue({ fieldId: 'entity' });
            const tranid = soRec.getValue('tranid') || '';
            const logPrefix = `[SO: ${tranid || soId}] `;
            let isModified = false;

            const initialMhiPackType = soRec.getValue('custbody_mhi_op_order_pack_type') || '';
            const initialKicPackageType = soRec.getValue('custbody_kic_package_type') || '';

            if (initialMhiPackType) {
                log.debug(logPrefix + 'Skipping Script', 'MHI Order Pack Type is already populated: ' + initialMhiPackType);
                const sendToApi = context.newRecord.getValue('custbody_ue_sent_to_paccurate');
                if (sendToApi) {
                    soRec.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: false });
                    logFieldChanges(soRec, initialMhiPackType, initialKicPackageType);
                    soRec.save();
                }
                return;
            }

            if (customerId == PETCO_CUSTOMER_ID) {
                log.debug(logPrefix + 'Skipping Script', 'Petco customer detected');
                return;
            }
            var DryIceQtyFromKIC = 0;
            // If OKC location and no package_type, call KeepItCool to populate package type and potentially add dry ice
            let packageType = soRec.getValue('custbody_kic_package_type');
            const pacuratePackExisting = soRec.getValue('custbody_mhi_op_order_pack_type');
            var isKICError = false;

            // If paccuratePack and packageType exist, attempt update
            if (pacuratePackExisting && packageType) {
                var shipperNameIs = updatePaccuratePackage(pacuratePackExisting, packageType, soId);

                if (shipperNameIs) {
                    soRec.setValue({ fieldId: 'custbody_mhi_op_order_pack_type', value: shipperNameIs });
                    isModified = true;
                }
            }

            // If location is OKC FC and package_type missing, call KeepItCool (process function will save)
            const location = soRec.getValue('location');
            var coolantName = '';
            if ((location == OKC_FC_LOCATION || location == PA_LOCATION || location == NV_LOCATION || location == FL_FC_LOCATION || location == IL_FC_LOCATION) && !packageType) {
                try {
                    var KICObj = processKeepItCoolIntegration(context, location, logPrefix);

                    if (KICObj) {
                        if (KICObj.coolantName) {
                            coolantName = KICObj.coolantName;
                            packageType = KICObj.coolantName;

                            soRec.setValue({
                                fieldId: 'custbody_kic_package_type',
                                value: KICObj.coolantName
                            });
                            isModified = true;
                        }
                        if (KICObj.totalDryIceQty) {
                            DryIceQtyFromKIC = KICObj.totalDryIceQty;
                            upsertDryIceLine(soRec, KICObj.totalDryIceQty);
                            isModified = true;
                        }
                    }
                    else {
                        isKICError = true;
                    }
                } catch (err) {
                    log.error(logPrefix + 'KeepItCool error', err);
                    isKICError = true;
                }
            }

            // Read script param rules (no longer used for Paccurate request)
            // const script = runtime.getCurrentScript();
            // const paramValue = script.getParameter({ name: 'custscript_package_rules_v2' }) || '[]';
            // let rules;
            // try {
            //     rules = JSON.parse(paramValue);
            // } catch (err) {
            //     rules = [];
            //     log.error('Invalid script param custscript_package_rules', err);
            // }

            const isCreate = context.type === context.UserEventType.CREATE;
            const classValue = (isCreate && soRec.getValue('class') === 101);

            const sendToApi = context.newRecord.getValue('custbody_ue_sent_to_paccurate');

            if (!sendToApi) {
                if (isModified) {
                    logFieldChanges(soRec, initialMhiPackType, initialKicPackageType);
                    soRec.save();
                }
                return;
            }
            const { itemSets, hasFrozenItems } = buildItemSets(soRec);

            // Add liner as item if present
            const formattedLiner = packageType ? formatLiner(packageType) : '';
            if (formattedLiner && LINER_DIMENSIONS[formattedLiner]) {
                const linerDim = LINER_DIMENSIONS[formattedLiner];
                itemSets.push({
                    name: formattedLiner,
                    refId: linerDim.refId,
                    dimensions: { x: linerDim.x, y: linerDim.y, z: linerDim.z },
                    quantity: 1
                });
            }

            // Add OUTER FORCE object (hardcoded) if no frozen items exist
            if (!hasFrozenItems) {
                itemSets.push({ refId: 10001, quantity: 1, name: '__OUTER_FORCE__' });
            }

            // Box types from custom record
            const boxTypes = fetchBoxTypes(packageType);

            // Prepare payload
            const payload = {
                orderId: tranid,
                boxTypeChoiceGoal: 'most-items',
                itemOrientationSearchDepth: 4,
                itemInitialOrientationPreferred: false,
                itemSets,
                boxTypes,
                rules: []
            };

            log.debug(logPrefix + 'Paccurate Payload', JSON.stringify(payload));

            // Call Paccurate
            const pacRes = https.post({
                url: PACCURATE_API_URL,
                headers: { Authorization: PACCURATE_API_KEY, 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            let pacBody = {};
            try {
                pacBody = pacRes && pacRes.body ? JSON.parse(pacRes.body) : {};
            } catch (err) {
                log.error(logPrefix + 'Failed to parse paccurate response', err);
                return;
            }

            log.debug(logPrefix + 'Paccurate Response', JSON.stringify(pacBody));

            if (pacRes.code === 200 && pacBody.boxes && pacBody.boxes.length) {
                // Extract box names and normalize them
                const boxDetails = pacBody.boxes.map(b => (b.box && b.box.name) ? b.box.name.replace('_', ' ') : '').filter(Boolean);

                // Sort like original: Shipper first then numeric order
                boxDetails.sort((a, b) => {
                    const aIsShipper = a.indexOf('Shipper') !== -1;
                    const bIsShipper = b.indexOf('Shipper') !== -1;
                    const aNum = parseInt(a.replace(/\D/g, ''), 10) || 0;
                    const bNum = parseInt(b.replace(/\D/g, ''), 10) || 0;
                    if (aIsShipper && !bIsShipper) return -1;
                    if (!aIsShipper && bIsShipper) return 1;
                    return aNum - bNum;
                });

                // Update sales order fields
                try {
                    // Uncheck UE sent to paccurate
                    soRec.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: false });
                    // record.submitFields({ type: record.Type.SALES_ORDER, id: soId, values: { custbody_ue_sent_to_paccurate: false } });

                    // Compose paccurate package field value
                    // let pacValue = packageType ? (boxDetails[0] + ' & ' + packageType) : boxDetails.join(' & ');
                    let hasLinerInBox0 = /liner\s*\d+/i.test(boxDetails[0]);

                    let pacValue = packageType
                        ? (hasLinerInBox0 ? boxDetails[0] : (boxDetails[0] + ' & ' + packageType))
                        : boxDetails.join(' & ');

                    if (coolantName) {
                        pacValue = pacValue.replace(/(&\s*)(liner\s*\d+)/gi, `$1${coolantName}`);
                        log.debug(logPrefix + 'New Paccurate Package Name', pacValue);
                    }

                    // if (!pacValue) pacValue = pacuratePackExisting || '';
                    if (pacValue) {
                        soRec.setValue({ fieldId: 'custbody_mhi_op_order_pack_type', value: pacValue });
                    }


                    if (pacValue && packageType) {
                        const updatedName = updatePaccuratePackage(pacValue, packageType, soId);
                        if (updatedName) {
                            soRec.setValue({ fieldId: 'custbody_mhi_op_order_pack_type', value: updatedName });
                        }
                    }

                    logFieldChanges(soRec, initialMhiPackType, initialKicPackageType);
                    soRec.save();
                } catch (err) {
                    log.error(logPrefix + 'Failed to update SO after paccurate response', err);
                }
            } else {
                soRec.setValue({ fieldId: 'custbody_ue_sent_to_paccurate', value: false });
                logFieldChanges(soRec, initialMhiPackType, initialKicPackageType);
                soRec.save();
                log.audit(logPrefix + 'Paccurate API Response with empty box', `Code: ${pacRes.code}, Body: ${pacRes.body}`);
            }
        } catch (e) {
            log.error(logPrefix + 'Error in afterSubmit', e);
            log.audit(logPrefix + 'Failed to process sales order', context.newRecord.id)
        }
    };

    // ---------- Helper: Build item sets from salesorder lines ----------
    function buildItemSets(soRec) {
        const colors = ['red', 'blue', 'green', 'yellow', 'purple', 'orange'];
        const itemCount = soRec.getLineCount({ sublistId: 'item' });
        const itemSets = [];
        let addRuleItem = false;
        let lastRef = -1;
        let dryIceQty = 0;
        let dryIceAdded = false;

        const dryLinesToAdd = [];
        var welcomeKitFound = false;
        let welcomeKitRefId = itemCount + 1;
        let refCounter = 300;
        let hasFrozenItems = false;

        for (let i = 0; i < itemCount; i++) {
            const itemId = soRec.getSublistValue({ sublistId: 'item', fieldId: 'item', line: i });
            const itemName = soRec.getSublistText({ sublistId: 'item', fieldId: 'item', line: i });

            const itemType = soRec.getSublistValue({ sublistId: 'item', fieldId: 'itemtype', line: i });

            if (itemId == 798) {
                hasFrozenItems = true;
            }

            let newRefId;
            if (itemName && itemName.indexOf('WelKit') === 0) {
                welcomeKitFound = true;
                newRefId = Math.floor(10000 + Math.random() * 90000)
            } else {
                refCounter++;
                newRefId = refCounter
            }

            // Only process inventory items and kits (as original)
            if ((itemType === 'InvtPart' || itemType === 'Kit' || welcomeKitFound == true) && itemId != 798) {
                const itemName = soRec.getSublistText({ sublistId: 'item', fieldId: 'item', line: i });
                const quantity = soRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: i }) || 0;
                const weight = soRec.getSublistValue({ sublistId: 'item', fieldId: 'custcol_item_weight', line: i }) || 0;

                const itemRecord = loadItemRecord(itemId, itemType);
                let dimensions = { x: 0, y: 0, z: 0 };
                let seq = '';
                let itemCode = null;

                // if (itemId == 484) {
                //   log.debug('itemRecord', itemRecord);
                //   log.debug('itemRecord', itemRecord.getValue({ fieldId: 'custitem_ds_frozen_item' }));
                // }

                if (itemRecord) {
                    // Skip frozen items as per new instructions
                    if (itemRecord.getValue({ fieldId: 'custitem_ollie_frozen_item' })) {
                        hasFrozenItems = true;
                        continue;
                    }

                    dimensions.x = parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_height' })) || 0;
                    dimensions.y = parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_width' })) || 0;
                    dimensions.z = parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_length' })) || 0;

                    const itemidText = (itemRecord.getValue({ fieldId: 'itemid' }) || '').toString();
                    if (itemidText.indexOf('-') !== -1) {
                        const parts = itemidText.split('-');
                        itemCode = parts[2] || null;
                    }
                }

                itemSets.push({
                    refId: newRefId,
                    color: colors[i % colors.length],
                    name: itemName,
                    weight,
                    dimensions,
                    dimensionSets: get_dimensionSets(itemCode, dimensions),
                    quantity,
                    sequence: seq
                });

                lastRef = i;
            }
        }

        // if (!welcomeKitFound) {
        //   const itemRecord = loadItemRecord(WELCOM_KIT_ITEM, 'Assembly');

        //   let dimensions = {
        //       x: parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_height' })) || 0,
        //       y: parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_width' })) || 0,
        //       z: parseFloat(itemRecord.getValue({ fieldId: 'custitem_ollie_pacc_length' })) || 0
        //   };

        //   let itemCode = null;
        //   const itemidText = (itemRecord.getValue({ fieldId: 'itemid' }) || '').toString();
        //   if (itemidText.indexOf('-') !== -1) {
        //       const parts = itemidText.split('-');
        //       itemCode = parts[2] || null;
        //   }

        //   itemSets.push({
        //       refId: Math.floor(10000 + Math.random() * 90000),
        //       color: colors[itemSets.length % colors.length],
        //       name: itemRecord.getText({ fieldId: 'itemid' }),
        //       weight: parseFloat(itemRecord.getValue({ fieldId: 'custitem_item_weight' })) || 0,
        //       dimensions: dimensions,
        //       dimensionSets: get_dimensionSets(itemCode, dimensions),
        //       quantity: 1,
        //       sequence: 'FROZEN_ITEM'
        //   });

        //   log.debug('WELCOME KIT FORCE-ADDED', WELCOM_KIT_ITEM);
        // }

        // salesOrder.save();

        return { itemSets, hasFrozenItems };
    }

    // ---------- Helper: Load item record by type ----------
    function loadItemRecord(itemId, itemType) {
        let recordType = null;
        switch (itemType) {
            case 'InvtPart': recordType = 'inventoryitem'; break;
            case 'Assembly': recordType = 'assemblyitem'; break;
            case 'Kit': recordType = 'kititem'; break;
            case 'Service': recordType = 'serviceitem'; break;
            case 'NonInvtPart': recordType = 'noninventoryitem'; break;
            case 'Group': recordType = 'itemgroup'; break;
            case 'Discount': recordType = 'discountitem'; break;
            default: recordType = null;
        }
        if (!recordType) return null;
        try {
            return record.load({ type: recordType, id: itemId });
        } catch (err) {
            log.error('loadItemRecord error', `id:${itemId} type:${itemType} -> ${err}`);
            return null;
        }
    }

    // ---------- Helper: get dimension sets ----------
    function get_dimensionSets(itemcode, dimensions) {
        if (!itemcode) return null;
        const code = String(itemcode);
        const defaultObj = [
            { quantityDivisor: 1, unitOfMeasure: 'each', weight: 0, dimensions }
        ];
        const map = {
            '0308': [{ quantityDivisor: 1, unitOfMeasure: 'each', weight: 0, dimensions },
            { quantityDivisor: 8, unitOfMeasure: 'layer', weight: 0, dimensions: { x: 1.3, y: 10, z: 13.5 } }],
            '0427': [{ quantityDivisor: 1, unitOfMeasure: 'each', weight: 0, dimensions },
            { quantityDivisor: 8, unitOfMeasure: 'layer', weight: 0, dimensions: { x: 1.8, y: 10, z: 13.5 } }],
            '0707': [{ quantityDivisor: 1, unitOfMeasure: 'each', weight: 0, dimensions },
            { quantityDivisor: 5, unitOfMeasure: 'layer', weight: 0, dimensions: { x: 1.4, y: 10, z: 13.5 } }],
            '1043': [{ quantityDivisor: 1, unitOfMeasure: 'each', weight: 0, dimensions },
            { quantityDivisor: 5, unitOfMeasure: 'layer', weight: 0, dimensions: { x: 2, y: 10, z: 13.5 } }]
        };
        return map[code] || defaultObj;
    }

    // ---------- Helper: lookup dry ice qty via custom record ----------
    function lookupDryIceQty(locationId, daysInTransit) {
        try {
            const srch = search.create({
                type: 'customrecord_ds_dry_ice_ref_table',
                filters: [
                    ['custrecord_ds_location', 'anyof', locationId],
                    'AND',
                    ['custrecord_ds_das_in_transit', 'equalto', daysInTransit]
                ],
                columns: [{ name: 'custrecord_ds_dry_ice_qty' }]
            });
            let qty = 0;
            srch.run().each(r => {
                qty = parseInt(r.getValue({ name: 'custrecord_ds_dry_ice_qty' }) || 0, 10) || 0;
                return true;
            });
            return qty;
        } catch (err) {
            log.error('lookupDryIceQty error', err);
            return 0;
        }
    }

    // ---------- Helper: build dry ice item object for itemSets ----------
    function buildDryIceItem(refId, qty) {
        return {
            refId,
            quantity: qty,
            dimensions: { x: 2, y: 10, z: 5 },
            dimensionSets: [
                { dimensions: { x: 2, y: 10, z: 13.5 }, weight: 4, quantityDivisor: 4, unitOfMeasure: 'block' },
                { dimensions: { x: 2, y: 10, z: 13.5 }, weight: 8, quantityDivisor: 8, unitOfMeasure: 'blockPair' }
            ],
            weight: 1,
            name: 'Dry Ice Block',
            color: '#690a34',
            sequence: 'FROZEN_ITEM'
        };
    }

    function buildDryIceRule(refId) {
        return {
            itemRefId: refId,
            operation: 'compact-pattern',
            options: {
                alternating: true,
                compactibleAxes: [0],
                compactProperties: [{ targetRefId: null, compactibleAxisDimensionChange: [{ x: -0.6 }] }]
            }
        };
    }


    function fetchBoxTypes(packageType) {
        var LinerValue = (packageType && formatLiner(packageType)) || '';
        const boxTypes = [];

        // ---- helpers ----
        function parseIds(val) {
            if (!val) return [];
            if (Array.isArray(val)) return val.map(function (v) { return parseInt(v, 10); }).filter(function (n) { return !isNaN(n); });
            return String(val).split(/[\u0005,;|\s]+/)
                .map(function (v) { return parseInt(v, 10); })
                .filter(function (n) { return !isNaN(n); });
        }

        function pushBox(result) {
            boxTypes.push({
                weightMax: parseFloat(result.getValue('custrecord_paccurate_weight')) || 0,
                name: result.getValue('name'),
                dimensions: {
                    x: parseFloat(result.getValue('custrecord_internal_height_inch')) || 0,
                    y: parseFloat(result.getValue('custrecord_internal_width_inch')) || 0,
                    z: parseFloat(result.getValue('custrecord_internal_length_inch')) || 0
                },
                outer: { dimensionchange: { x: 2, y: 2, z: 2 } },
                refId: parseInt(result.getValue('custrecord_ds_ref_id'), 10) || 0,
                price: parseFloat(result.getValue('custrecord_ds_price')) || 0
            });
        }

        function getAllBoxTypes() {
            const all = [];
            search.create({
                type: 'customrecord_paccurate_box_type',
                filters: [['isinactive', 'is', 'F']],
                columns: [
                    search.createColumn({ name: 'name', sort: search.Sort.ASC }),
                    'custrecord_internal_height_inch',
                    'custrecord_internal_length_inch',
                    'custrecord_internal_width_inch',
                    'custrecord_paccurate_weight',
                    'custrecord_ds_ref_id',
                    'custrecord_ds_price',
                    'custrecord_related_shippers',
                    'internalid'
                ]
            }).run().each(function (res) {
                const name = res.getValue('name');
                if (name && name.toLowerCase().indexOf('liner') === 0) {
                    return true; // Skip liners in boxTypes
                }
                all.push({
                    weightMax: parseFloat(res.getValue('custrecord_paccurate_weight')) || 0,
                    name: name,
                    dimensions: {
                        x: parseFloat(res.getValue('custrecord_internal_height_inch')) || 0,
                        y: parseFloat(res.getValue('custrecord_internal_width_inch')) || 0,
                        z: parseFloat(res.getValue('custrecord_internal_length_inch')) || 0
                    },
                    outer: { dimensionchange: { x: 2, y: 2, z: 2 } },
                    refId: parseInt(res.getValue('custrecord_ds_ref_id'), 10) || 0,
                    price: parseFloat(res.getValue('custrecord_ds_price')) || 0
                });
                return true;
            });
            return all;
        }

        try {
            // If packageType empty => return ALL
            if (!packageType || !LinerValue) {
                return getAllBoxTypes();
            }

            // ---- PASS 1: fetch the exact liner row ----
            var linerResult = null;
            search.create({
                type: 'customrecord_paccurate_box_type',
                filters: [
                    ['name', 'is', LinerValue], 'AND',
                    ['name', 'startswith', 'Liner'], 'AND',
                    ['isinactive', 'is', 'F']
                ],
                columns: [
                    'custrecord_internal_height_inch',
                    'custrecord_internal_length_inch',
                    'custrecord_internal_width_inch',
                    'custrecord_paccurate_weight',
                    'name',
                    'custrecord_ds_ref_id',
                    'custrecord_ds_price',
                    'custrecord_related_shippers',
                    'internalid'
                ]
            }).run().each(function (res) {
                linerResult = res;
                return false; // only one
            });

            if (!linerResult) {
                // boxtype result empty => return ALL
                return getAllBoxTypes();
            }

            // Add the liner first
            // pushBox(linerResult); // Removed as liner is now treated as an item

            // ---- PASS 2: fetch only related shippers (same record type) ----
            var relatedIds = parseIds(linerResult.getValue('custrecord_related_shippers'));
            if (linerResult) {
                search.create({
                    type: 'customrecord_paccurate_box_type',
                    filters: [
                        ["name", "startswith", "Shipper"], 'AND',
                        ['isinactive', 'is', 'F']
                    ],
                    columns: [
                        search.createColumn({ name: 'name', sort: search.Sort.ASC }),
                        'custrecord_internal_height_inch',
                        'custrecord_internal_length_inch',
                        'custrecord_internal_width_inch',
                        'custrecord_paccurate_weight',
                        'custrecord_ds_ref_id',
                        'custrecord_ds_price',
                        'custrecord_related_shippers',
                        'internalid'
                    ]
                }).run().each(function (res) {
                    pushBox(res);
                    return true;
                });
            }

            // If still nothing somehow, fallback to ALL
            if (!boxTypes.length) {
                return getAllBoxTypes();
            }

        } catch (err) {
            log.error('fetchBoxTypes error', err);
            // safety fallback
            return getAllBoxTypes();
        }

        return boxTypes;
    }

    function upsertDryIceLine(soRec, quantity) {
        const lineCount = soRec.getLineCount({ sublistId: 'item' });
        let found = false;

        for (let i = 0; i < lineCount; i++) {
            const item = soRec.getSublistValue({
                sublistId: 'item',
                fieldId: 'item',
                line: i
            });

            if (item == DRY_ICE_ITEM_ID) {
                // Update existing dry ice line
                soRec.selectLine({ sublistId: 'item', line: i });
                soRec.setCurrentSublistValue({
                    sublistId: 'item',
                    fieldId: 'quantity',
                    value: quantity
                });
                soRec.setCurrentSublistValue({
                    sublistId: 'item',
                    fieldId: 'rate',
                    value: 0
                });
                soRec.commitLine({ sublistId: 'item' });

                found = true;
                break;
            }
        }

        // If no dry ice line found → add new
        if (!found) {
            soRec.selectNewLine({ sublistId: 'item' });
            soRec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: DRY_ICE_ITEM_ID });
            soRec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: quantity });
            soRec.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: 0 });
            soRec.commitLine({ sublistId: 'item' });
        }
    }

    // ---------- KeepItCool integration (uses saved search to build shipments) ----------
    function processKeepItCoolIntegration(context, location, logPrefix) {
        try {
            if (context.type !== context.UserEventType.CREATE &&
                context.type !== context.UserEventType.EDIT) {
                return;
            }

            const soId = context.newRecord.id;

            const headerShipMethod = context.newRecord.getValue({ fieldId: 'shipmethod' });

            // ---- Build payload using saved search ----
            const salesorderSearchObj = search.create({
                type: "salesorder",
                settings: [{ name: "consolidationtype", value: "ACCTTYPE" }],
                filters: [
                    ["type", "anyof", "SalesOrd"],
                    "AND",
                    ["internalid", "anyof", soId],
                    "AND",
                    ["mainline", "is", "F"],
                    "AND",
                    ["taxline", "is", "F"],
                    "AND",
                    ["shipping", "is", "F"],
                    "AND",
                    [
                        [
                            ["item.type", "anyof", "InvtPart"],
                            "AND",
                            ["item.custitem_ollie_frozen_item", "is", "T"]
                        ],
                        "OR",
                        ["item", "anyof", SB_SAMPLEBOX_KIT_ITEM]
                    ]
                    // "AND",
                    // ["item.type", "anyof", "InvtPart"],
                    // "AND",
                    // ["item.custitem_ds_frozen_item", "is", "T"]
                ],
                columns: [
                    search.createColumn({ name: "tranid" }),
                    search.createColumn({ name: "shipzip" }),
                    search.createColumn({ name: "shipdate" }),
                    search.createColumn({ name: "quantity" }),
                    search.createColumn({ name: "shipmethod" }),
                    search.createColumn({ name: "custbody_mhi_op_planshipdate" }),
                    search.createColumn({ name: "custbody_mhi_op_daysintransit" }),
                    search.createColumn({
                        name: "formulatext",
                        formula: "LTRIM(SUBSTR({item}, INSTR({item}, ':') + 1))"
                    })
                ]
            });
            var searchResultCount = salesorderSearchObj.runPaged().count;

            let shipments = [];

            salesorderSearchObj.run().each(result => {
                let orderNum = result.getValue("tranid") || "";
                // if (orderNum) orderNum = String(orderNum).split('.')[0];

                const shipZip = result.getValue("shipzip") || "";
                const shipDateValue = result.getValue("custbody_mhi_op_planshipdate");

                const planShipDate = result.getValue("custbody_mhi_op_planshipdate");
                const daysInTransit = result.getValue("custbody_mhi_op_daysintransit") || 0;

                let shipDate = "";
                if (shipDateValue) {
                    const dateObj = new Date(shipDateValue);
                    const year = dateObj.getFullYear();
                    const month = String(dateObj.getMonth() + 1).padStart(2, '0');
                    const day = String(dateObj.getDate()).padStart(2, '0');
                    shipDate = `${year}-${month}-${day}`;
                }

                var expectedShipDate = '';

                if (planShipDate) {
                    var planDateObj = new Date(planShipDate);
                    const daysToAdd = parseInt(daysInTransit, 10) || 0;

                    planDateObj.setDate(planDateObj.getDate() + daysToAdd);

                    const planYear = planDateObj.getFullYear();
                    const planMonth = String(planDateObj.getMonth() + 1).padStart(2, '0');
                    const planDay = String(planDateObj.getDate()).padStart(2, '0');

                    expectedShipDate = `${planYear}-${planMonth}-${planDay}`;
                }

                var shipMethodRes = headerShipMethod ? headerShipMethod : result.getValue("shipmethod");

                var shippingMethodId = '';

                let DISTRIBUTION_CENTER;

                if (location == OKC_FC_LOCATION) {

                    DISTRIBUTION_CENTER = DISTRIBUTION_CENTER_ID;

                    if (shipMethodRes == 730) {
                        shippingMethodId = NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 4) {
                        shippingMethodId = UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 17) {
                        shippingMethodId = UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 734) {
                        shippingMethodId = UPS_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 12) {
                        // shippingMethodId = JITSU_SHIPPING_METHOD;
                        shippingMethodId = NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 19) {
                        shippingMethodId = NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 453) {
                        shippingMethodId = UPS_NEXT_DAY_SHIPPING_METHOD;
                    }
                    else {
                        return;
                    }
                }

                if (location == PA_LOCATION) {

                    DISTRIBUTION_CENTER = PA_DISTRIBUTION_CENTER_ID;

                    if (shipMethodRes == 730) {
                        shippingMethodId = PA_NEXT_DAY_SHIPPING_METHOD; //NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 4) {
                        shippingMethodId = PA_UPS_SHIPPING_METHOD; //UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 734) {
                        shippingMethodId = PA_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 12) {
                        // shippingMethodId = JITSU_SHIPPING_METHOD;
                        shippingMethodId = PA_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 19) {
                        shippingMethodId = PA_NEXT_DAY_SHIPPING_METHOD; //NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 453) {
                        shippingMethodId = PA_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    }
                    else if (shipMethodRes == 18) { //ONTRAC SHIPPING METHOD
                        shippingMethodId = PA_ONTRAC_GROUND_SHIPPING_METHOD;
                    }
                    else {
                        return;
                    }
                }

                if (location == NV_LOCATION) {

                    DISTRIBUTION_CENTER = NV_DISTRIBUTION_CENTER_ID;

                    if (shipMethodRes == 730) {
                        shippingMethodId = NV_NEXT_DAY_SHIPPING_METHOD; //NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 4) {
                        shippingMethodId = NV_UPS_SHIPPING_METHOD; //UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 734) {
                        shippingMethodId = NV_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 18) {
                        shippingMethodId = NV_ONTRACK_GROUND_SHIPPING_METHOD; //ONTRACK GROUND
                    } else if (shipMethodRes == 19) {
                        shippingMethodId = NV_NEXT_DAY_SHIPPING_METHOD; //NEXT_DAY_SHIPPING_METHOD;
                    }
                    else {
                        return;
                    }
                }

                if (location == FL_FC_LOCATION) {

                    DISTRIBUTION_CENTER = FL_FC_JACKSONVILLE_DISTRIBUTION_CENTER_ID;

                    if (shipMethodRes == 4) {
                        shippingMethodId = FL_FC_UPS_SHIPPING_METHOD; //UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 734) {
                        shippingMethodId = FL_FC_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    }
                    else {
                        return;
                    }
                }

                if (location == IL_FC_LOCATION) {

                    DISTRIBUTION_CENTER = IL_DISTRIBUTION_CENTER_ID;

                    if (shipMethodRes == 4) {
                        shippingMethodId = IL_FC_UPS_SHIPPING_METHOD; //UPS_SHIPPING_METHOD;
                    } else if (shipMethodRes == 734) {
                        shippingMethodId = IL_FC_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 453) {
                        shippingMethodId = IL_FC_UPS_NEXT_DAY_SHIPPING_METHOD; //UPS_NEXT_DAY_SHIPPING_METHOD;
                    } else if (shipMethodRes == 19) {
                        shippingMethodId = IL_FC_UDS_SHIPPING_METHOD; //UDS_SHIPPING_METHOD;
                    }
                    else {
                        return;
                    }
                }

                const sku = result.getValue({ name: "formulatext" }) || "";
                if (!sku) return true;

                // const qty = result.getValue("quantity") || 0;
                const qty = Math.abs(Number(result.getValue("quantity")) || 0);
                let existingShipment = shipments.find(s => s.order_number === orderNum);

                if (existingShipment) {
                    existingShipment.items.push({ sku, quantity: qty });
                } else {
                    if (expectedShipDate) {
                        shipments.push({
                            external_shipment_id: orderNum,
                            order_number: orderNum,
                            to_location: { zip_code: shipZip },
                            distribution_center: { distribution_center_id: DISTRIBUTION_CENTER },
                            shipping_method: { shipping_method_id: shippingMethodId },
                            items: [{ sku, quantity: qty }],
                            expected_ship_date: shipDate,
                            expected_delivery_date: expectedShipDate
                        });
                    } else {
                        shipments.push({
                            external_shipment_id: orderNum,
                            order_number: orderNum,
                            to_location: { zip_code: shipZip },
                            distribution_center: { distribution_center_id: DISTRIBUTION_CENTER },
                            shipping_method: { shipping_method_id: shippingMethodId },
                            items: [{ sku, quantity: qty }],
                            expected_ship_date: shipDate
                        });
                    }
                }

                return true;
            });

            const payload = {
                shipments,
                features: {
                    ship_date_recommendation: false,
                    coolant_recommendation: true,
                    distribution_center_recommendation: false,
                    shipping_method_recommendation: false,
                    packaging_recommendation: true
                }
            };

            log.debug(logPrefix + 'KIC Payload', JSON.stringify(payload));

            // ---- POST to KeepItCool API ----
            if (shipments.length === 0) return;

            const response = https.post({
                url: KEEP_IT_COOL_BASE_URL,
                body: JSON.stringify(payload),
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Authorization': KEEP_IT_COOL_AUTH
                }
            });

            log.debug(logPrefix + "KIC Response", response.body);

            const respBody = JSON.parse(response.body);
            if (!respBody || !respBody.results || !respBody.results.length) return;

            const responseResult = respBody.results[0];
            if (responseResult.status !== 'success' || !responseResult.shipment_data) return;

            const shipment = responseResult.shipment_data;
            const coolant = shipment.coolant;
            if (!coolant) return;

            var coolantName = coolant.packaging_config?.name || coolant.name || '';

            if (coolantName) {
                var splitCoolantName = coolantName.split('&');

                if (splitCoolantName.length) {
                    coolantName = splitCoolantName[1].trim();
                }
            }

            let totalDryIceQty = 0;
            if (coolant.coolant_blocks && coolant.coolant_blocks.length > 0) {
                coolant.coolant_blocks.forEach(block => {
                    const qty = (block.coolant_block_count || 0) * (block.coolant_block_weight_lbs || 0);
                    totalDryIceQty += qty;
                });
            }

            var KICDetails = {}

            if (totalDryIceQty > 0) {

                KICDetails.totalDryIceQty = totalDryIceQty;
                KICDetails.coolantName = coolantName;

                return KICDetails;
            }

            return false;

        } catch (err) {
            log.error(logPrefix + 'Error in KeepItCool integration logic', err);
        }
    }

    // ---------- updatePaccuratePackage: replace liner or append ----------
    function updatePaccuratePackage(paccuratePackageType, KICPackageType, soId) {
        try {
            if (!paccuratePackageType || !KICPackageType) return paccuratePackageType || '';
            const newLiner = KICPackageType.trim();

            if (/liner\s*\d+/i.test(paccuratePackageType)) {
                const replaced = paccuratePackageType.replace(/liner\s*\d+/i, newLiner);
                // record.submitFields({ type: record.Type.SALES_ORDER, id: soId, values: { custbody_mhi_op_order_pack_type: replaced } });
                return replaced;
            } else {
                // const appended = paccuratePackageType.trim() + ' & ' + newLiner;
                // record.submitFields({ type: record.Type.SALES_ORDER, id: soId, values: { custbody_mhi_op_order_pack_type: appended } });
                // return appended;
            }
        } catch (err) {
            log.error('updatePaccuratePackage error', err);
            return paccuratePackageType || '';
        }
    }

    // ---------- logFieldChanges: log MHI & KIC package field updates to custbody_package_update ----------
    function logFieldChanges(soRec, initialMhiPack, initialKicPack) {
        try {
            const currentMhiPack = soRec.getValue('custbody_mhi_op_order_pack_type') || '';
            const currentKicPack = soRec.getValue('custbody_kic_package_type') || '';

            const mhiChanged = (currentMhiPack !== initialMhiPack);
            const kicChanged = (currentKicPack !== initialKicPack);

            if (mhiChanged || kicChanged) {
                let existingLog = soRec.getValue('custbody_package_update') || '';
                let entry = `\n--- Update at ${getFormattedDate()} ---\n`;
                if (mhiChanged) {
                    entry += `MHI Order Pack Type: "${initialMhiPack}" -> "${currentMhiPack}"\n`;
                }
                if (kicChanged) {
                    entry += `KIC Package Type: "${initialKicPack}" -> "${currentKicPack}"\n`;
                }

                const updatedLog = (existingLog ? existingLog + '\n' : '') + entry.trim();
                soRec.setValue({
                    fieldId: 'custbody_package_update',
                    value: updatedLog
                });
                const soId = soRec.id;
                const tranid = soRec.getValue('tranid') || '';
                log.debug(`[SO: ${tranid || soId}] Logging field changes`, entry);
            }
        } catch (err) {
            const soId = soRec ? soRec.id : '';
            const tranid = soRec ? (soRec.getValue('tranid') || '') : '';
            log.error(`[SO: ${tranid || soId}] Error in logFieldChanges`, err);
        }
    }

    function getFormattedDate() {
        const d = new Date();
        const pad = (n) => n.toString().padStart(2, '0');
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    }

    // ---------- formatLiner ----------
    function formatLiner(str) {
        if (!str) return '';
        return str.toLowerCase().replace(/\s+/g, '_').trim();
    }

    return { afterSubmit };
});
