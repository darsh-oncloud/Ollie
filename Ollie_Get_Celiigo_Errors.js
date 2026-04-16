/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/https', 'N/file', 'N/log'], function (https, file, log) {

    var CELIGO_TOKEN = 'c5925a41e3f04375961bd526d1125d54';
    var FLOW_ID = '6862d38aa2817b33330ea03f';
    var STEP_ID = '6862d383b5e58e8daab962dc';
    var FOLDER_ID = 3317;

    function safe(val) {
        return (val || 'blank').toString().replace(/[^a-zA-Z0-9_\-]/g, '_');
    }

    function getInputData() {
        try {
            var url = 'https://api.integrator.io/v1/flows/' + FLOW_ID + '/' + STEP_ID + '/errors'
                + '?occurredAt_gte=' + encodeURIComponent('2026-04-14T00:00:00.000Z')
                + '&occurredAt_lte=' + encodeURIComponent(new Date().toISOString());

            log.audit('Celigo URL', url);

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