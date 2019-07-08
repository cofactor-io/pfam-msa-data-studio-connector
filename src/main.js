// Copyright (c) 2019 Cofactor
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/**
 * @fileoverview Community Connector for Pfam MSA data.
 *
 */

var cc = DataStudioApp.createCommunityConnector();
var DEFAULT_ACCESSION = 'PF01352';

// [START get_config]
// https://developers.google.com/datastudio/connector/reference#getconfig
function getConfig() {
    var config = cc.getConfig();

    config
        .newInfo()
        .setId('instructions')
        .setText(
            'Enter the protein family accession number to fetch the alignments.'
        );

    config
        .newTextInput()
        .setId('accession')
        .setName(
            'Accession number'
        )
        .setHelpText('e.g. PF01352')
        .setPlaceholder(DEFAULT_ACCESSION)
        .setAllowOverride(true);

    return config.build();
}
// [END get_config]

// [START get_schema]
function getFields() {
    var fields = cc.getFields();
    var types = cc.FieldType;
    var aggregations = cc.AggregationType;

    fields
        .newDimension()
        .setId('position')
        .setName('Position')
        .setType(types.NUMBER);

    fields
        .newDimension()
        .setId('residue')
        .setName('Residue')
        .setType(types.TEXT);

    fields
        .newMetric()
        .setId('count')
        .setName('Count')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);

    return fields;
}

// https://developers.google.com/datastudio/connector/reference#getschema
function getSchema(request) {
    return { schema: getFields().build() };
}
// [END get_schema]

// [START get_data]
// https://developers.google.com/datastudio/connector/reference#getdata
function getData(request) {
    request.configParams = validateConfig(request.configParams);

    var requestedFields = getFields().forIds(
        request.fields.map(function (field) {
            return field.name;
        })
    );

    try {
        var responseText = fetchDataFromApi(request);
        var data = getFormattedData(responseText, requestedFields);
    } catch (e) {
        cc.newUserError()
            .setDebugText('Error fetching data. Exception details: ' + e)
            .setText(
                'The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists.'
            )
            .throwException();
    }

    return {
        schema: requestedFields.build(),
        rows: data
    };
}

/**
 * Gets response for UrlFetchApp.
 *
 * @param {Object} request Data request parameters.
 * @returns {string} Response text for UrlFetchApp.
 */
function fetchDataFromApi(request) {
    var url = [
        'https://pfam.xfam.org/family/',
        request.configParams.accession,
        '/alignment/seed/format?format=fasta&alnType=seed&order=t&case=u&gaps=dashes&download=0'
    ].join('');
    var response = UrlFetchApp.fetch(url);
    return response.getContentText();
}

/**
 * Formats the parsed response from external data source into correct tabular
 * format and returns only the requestedFields
 *
 * @param {string} responseText The response string from external data source
 *     parsed into an object in a standard format.
 * @param {Array} requestedFields The fields requested in the getData request.
 * @returns {Array} Array containing rows of data in key-value pairs for each
 *     field.
 */
function getFormattedData(responseText, requestedFields) {
    var data = [];
    var lines = responseText.split('\n');

    var sequences = [];
    var currentSequence = '';

    lines.forEach(function (line) {
        if (line.charAt(0) === '>') {
            if (currentSequence) {
                sequences.push(currentSequence);
            }
            currentSequence = '';
        } else {
            currentSequence += line;
        }
    });
    if (currentSequence) {
        sequences.push(currentSequence);
    }

    var length = 0; // Alignment length
    sequences.forEach(function (sequence) {
        length = Math.max(length, sequence.length);
    });

    for (var i = 0; i < length; ++i) {
        var residueCounts = {};
        sequences.forEach(function (sequence) {
            var residue = sequence.charAt(i) || '-';
            if (residueCounts.hasOwnProperty(residue)) {
                residueCounts[residue] += 1;
            } else {
                residueCounts[residue] = 1;
            }
        });

        Object.keys(residueCounts).forEach(function(residue) {
            var row = requestedFields.asArray().map(function (requestedField) {
                switch (requestedField.getId()) {
                    case 'position':
                        return i + 1;
                    case 'residue':
                        return residue;
                    case 'count':
                        return residueCounts[residue];
                    default:
                        return '';
                }
            });
            data.push({ values: row });
        });
    }

    return data;
}
// [END get_data]

// https://developers.google.com/datastudio/connector/reference#isadminuser
function isAdminUser() {
    return false;
}

/**
 * Validates config parameters and provides missing values.
 *
 * @param {Object} configParams Config parameters from `request`.
 * @returns {Object} Updated Config parameters.
 */
function validateConfig(configParams) {
    configParams = configParams || {};
    configParams.accession = configParams.accession || DEFAULT_ACCESSION;

    return configParams;
}
