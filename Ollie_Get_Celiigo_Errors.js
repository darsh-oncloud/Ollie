/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/https', 'N/file', 'N/log', 'N/search'], function (https, file, log, search) {

    var CELIGO_TOKEN = 'c5925a41e3f04375961bd526d1125d54';
    var FLOW_ID = '6862d38aa2817b33330ea03f';
    var STEP_ID = '6862d383b5e58e8daab962dc';
    var FOLDER_ID = 3318;

    function safe(val) {
        return (val || 'blank').toString().replace(/[^a-zA-Z0-9_\-]/g, '_');
    }

    function getLast30MinUTC() {
        var dt = new Date();
        dt.setMinutes(dt.getMinutes() - 30);
        return dt.toISOString();
    }

    function fileExists(fileName) {
        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['name', 'is', fileName],
                'and',
                ['folder', 'anyof', FOLDER_ID]
            ],
            columns: ['internalid']
        });

        var results = fileSearch.run().getRange({
            start: 0,
            end: 1
        });

        return results && results.length > 0;
    }

    function getInputData() {
        try {
            var fromDate = getLast30MinUTC();
            var toDate = new Date().toISOString();

            var url = 'https://api.integrator.io/v1/flows/' + FLOW_ID + '/' + STEP_ID + '/errors'
                + '?occurredAt_gte=' + encodeURIComponent(fromDate)
                + '&occurredAt_lte=' + encodeURIComponent(toDate);

            log.audit('Celigo URL', url);
            log.audit('From Date', fromDate);
            log.audit('To Date', toDate);

            var response = https.get({
                url: url,
                headers: {
                    'Authorization': 'Bearer ' + CELIGO_TOKEN,
                    'Content-Type': 'application/json'
                }
            });

            log.audit('Response Code', response.code);
            log.debug('Response Body', response.body);

            if (response.code !== 200) {
                log.error('API Error', response.body);
                return [];
            }

            var data = JSON.parse(response.body);
            return (data && data.errors) ? data.errors : [];

        } catch (e) {
            log.error('getInputData Error', e);
            return [];
        }
    }

    function map(context) {
        try {
            var errorObj = JSON.parse(context.value);
            var fileName = safe(errorObj.traceKey) + '.json';

            if (fileExists(fileName)) {
                log.audit('File Already Exists', fileName);
                return;
            }

            var jsonFile = file.create({
                name: fileName,
                fileType: file.Type.PLAINTEXT,
                contents: JSON.stringify(errorObj, null, 2),
                folder: FOLDER_ID,
                isOnline: false
            });

            var fileId = jsonFile.save();
            log.audit('File Created', 'File ID: ' + fileId + ' | Name: ' + fileName);

        } catch (e) {
            log.error('Map Error', e);
        }
    }

    function summarize(summary) {
        if (summary.inputSummary.error) {
            log.error('Input Error', summary.inputSummary.error);
        }

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('Map Error ' + key, error);
            return true;
        });

        log.audit('Summary', 'Map/Reduce completed');
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});