/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/file', 'N/record', 'N/https', 'N/log'], function (search, file, record, https, log) {

    var CELIGO_TOKEN = 'c5925a41e3f04375961bd526d1125d54';
    var FLOW_ID = '6862d38aa2817b33330ea03f';
    var STEP_ID = '6862d383b5e58e8daab962dc';

    var PENDING_FOLDER = 3318;
    var PROCESSED_FOLDER = 3319;
    var ERROR_FOLDER = 3320;

    function getInputData() {
        var files = [];

        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', PENDING_FOLDER]
            ],
            columns: [
                search.createColumn({ name: 'internalid' }),
                search.createColumn({ name: 'name' })
            ]
        });

        fileSearch.run().each(function (result) {
            files.push({
                fileId: result.getValue({ name: 'internalid' }),
                fileName: result.getValue({ name: 'name' })
            });
            return true;
        });

        log.audit('Pending Files Found', files.length);
        return files;
    }

    function map(context) {
        var data = JSON.parse(context.value);
        var fileId = data.fileId;
        var fileName = data.fileName;
        var jsonData = null;

        try {
            var loadedFile = file.load({ id: fileId });
            var contents = loadedFile.getContents();
            jsonData = JSON.parse(contents);

            var traceKey = jsonData.traceKey;
            var errorId = jsonData.errorId;

            if (!traceKey) {
                throw new Error('traceKey missing in file ' + fileName);
            }

            if (!errorId) {
                throw new Error('errorId missing in file ' + fileName);
            }

            log.audit('Processing File', 'File: ' + fileName + ' | traceKey: ' + traceKey + ' | errorId: ' + errorId);

            var soIds = getSalesOrders(traceKey);

            if (!soIds.length) {
                throw new Error('No Sales Orders found for eTail Order ID: ' + traceKey);
            }

            var i;
            for (i = 0; i < soIds.length; i++) {
                closeSalesOrderLines(soIds[i]);
            }

            resolveCeligoError(errorId);
            moveFile(fileId, PROCESSED_FOLDER);

            log.audit('Processed Successfully', fileName);

        } catch (e) {
            log.error('Processing Failed: ' + fileName, e);

            try {
                createErrorFile(fileName, jsonData, e);
                moveFile(fileId, ERROR_FOLDER);
            } catch (innerErr) {
                log.error('Error Handling Failed: ' + fileName, innerErr);
            }
        }
    }

    function getSalesOrders(traceKey) {
        var soIds = [];

        var soSearch = search.create({
            type: 'salesorder',
            settings: [{ name: 'consolidationtype', value: 'ACCTTYPE' }],
            filters: [
                ['type', 'anyof', 'SalesOrd'],
                'and',
                ['mainline', 'is', 'T'],
                'and',
                ['custbody_celigo_etail_order_id', 'is', String(traceKey)]
            ],
            columns: [
                search.createColumn({ name: 'internalid' })
            ]
        });

        soSearch.run().each(function (result) {
            soIds.push(result.getValue({ name: 'internalid' }));
            return true;
        });

        log.audit('Sales Orders Found', traceKey + ' => ' + soIds.join(','));
        return soIds;
    }

    function closeSalesOrderLines(soId) {
        var soRec = record.load({
            type: record.Type.SALES_ORDER,
            id: soId,
            isDynamic: false
        });

        var lineCount = soRec.getLineCount({ sublistId: 'item' });
        var changed = false;
        var i;

        for (i = 0; i < lineCount; i++) {
            var isClosed = soRec.getSublistValue({
                sublistId: 'item',
                fieldId: 'isclosed',
                line: i
            });

            if (!isClosed) {
                soRec.setSublistValue({
                    sublistId: 'item',
                    fieldId: 'isclosed',
                    line: i,
                    value: true
                });
                changed = true;
            }
        }

        if (changed) {
            soRec.save({
                enableSourcing: false,
                ignoreMandatoryFields: true
            });
            log.audit('Sales Order Closed', soId);
        } else {
            log.audit('Sales Order Already Closed', soId);
        }
    }

    function resolveCeligoError(errorId) {
        var url = 'https://api.integrator.io/v1/flows/' + FLOW_ID + '/' + STEP_ID + '/resolved';

        var response = https.put({
            url: url,
            headers: {
                'Authorization': 'Bearer ' + CELIGO_TOKEN,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                errors: [String(errorId)]
            })
        });

        log.audit('Resolve Response Code', response.code);
        log.debug('Resolve Response Body', response.body);

        if (String(response.code) !== '200') {
            throw new Error('Failed to resolve Celigo error. Code: ' + response.code + ' Body: ' + response.body);
        }
    }

    function moveFile(fileId, targetFolder) {
        var f = file.load({ id: fileId });
        f.folder = targetFolder;
        f.save();
        log.audit('File Moved', 'File ID: ' + fileId + ' moved to folder ' + targetFolder);
    }

    function createErrorFile(originalFileName, jsonData, err) {
        var errorObj = {
            originalFileName: originalFileName,
            processedAt: new Date().toISOString(),
            errorMessage: err && err.message ? err.message : String(err),
            sourceData: jsonData || {}
        };

        var errorFile = file.create({
            name: originalFileName.replace('.json', '') + '_ERROR.json',
            fileType: file.Type.PLAINTEXT,
            contents: JSON.stringify(errorObj, null, 2),
            folder: ERROR_FOLDER,
            isOnline: false
        });

        var errorFileId = errorFile.save();
        log.audit('Error File Created', errorFileId);
    }

    function summarize(summary) {
        if (summary.inputSummary.error) {
            log.error('Input Error', summary.inputSummary.error);
        }

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('Map Error for key ' + key, error);
            return true;
        });

        log.audit('Usage', summary.usage);
        log.audit('Concurrency', summary.concurrency);
        log.audit('Yields', summary.yields);
        log.audit('Summary', 'Map/Reduce completed');
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});
