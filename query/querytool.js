function jsonRpc(url, method, params) {
    if (url == null) {
        return Promise.reject(new Error(JSON.stringify({
            description: 'Must connect to server before executing queries',
            stacktrace: ''
        })));
    }

    return new Promise(function(resolve, reject) {
        $.ajax({
            data: JSON.stringify({
                id: 1,
                jsonrpc: '2.0',
                method: method,
                params: params
            }),
            method: 'POST',
            url: url,
            contentType: 'application/json'
        }).then(function(response) {
            if ('error' in response) {
                reject(new Error(JSON.stringify({
                    description: response.error.message,
                    stacktrace: response.error.data.stacktrace
                })));
            }

            resolve(response.result);
        }).fail(function(xhr) {
            var response = JSON.parse(xhr.responseText);
            reject(new Error(JSON.stringify({
                description: response.error.message,
                stacktrace: response.error.data.stacktrace
            })));
        });
    });
}

var Controller = {
    server: null,
    queryEnabled: false,

    connect: function(server) {
        var that = this;
        return new Promise(function(resolve) {
            if (that.server != null) {
                throw new Error(JSON.stringify({
                    description: "You must disconnect before connecting to a different server",
                    stacktrace: null
                }));
            }

            that.server = server;
            resolve(true);
        });
    },

    disconnect: function() {
        var base;
        if (this.queryEnabled) {
            base = this.finishQuery();
        } else {
            base = Promise.resolve(true);
        }

        var that = this;
        return base.then(function() {
            that.server = null;
        });
    },

    executeQuery: function(sql) {
        var base;
        if (this.queryEnabled) {
            base = this.finishQuery();
        } else {
            base = Promise.resolve(true);
        }

        var that = this;
        return base.then(function() {
            return jsonRpc(that.server, 'execute', [sql]).then(function(result) {
                that.queryEnabled = true;
            });
        });
    },

    queryTables: function() {
        return jsonRpc(this.server, 'tables', []);
    },

    queryViews: function() {
        return jsonRpc(this.server, 'views', []);
    },

    queryColumns: function(catalog, schema, table) {
        return jsonRpc(this.server, 'columns', [catalog, schema, table]);
    },

    queryMetadata: function() {
        return jsonRpc(this.server, 'metadata', []);
    },

    queryResultCount: function() {
        return jsonRpc(this.server, 'count', []);
    },

    queryNextPage: function() {
        return jsonRpc(this.server, 'page', []);
    },

    finishQuery: function() {
        var that = this;
        return jsonRpc(this.server, 'finish', []).then(function(response) {
            that.queryEnabled = false;
        });
    }
};

///// UI /////

function clearError() {
    displayStatus('OK');
    $('#stacktrace').text('');
}

function clearResults() {
    $('#grid-table').empty();
    $('#count-container').text('');
}

function hasNextPage(nextPage) {
    var button = $('#next-page');
    if (nextPage) {
        button.css({display: 'block'});
    } else {
        button.css({display: 'none'});
    }
}

function displayStatus(message) {
    var status = $('#status');
    status.removeClass('error');
    status.text(message);
}

function displayCount(resultCount) {
    clearResults();
    clearError();

    var count = $('#count-container');
    count.text('Records affected: ' + resultCount);
}

function displayPage(metadata, rows) {
    clearResults();
    clearError();

    var table = $('#grid-table');
    var headerRow = $('<tr></tr>');
    for (var i = 0; i < metadata.columnnames.length; i++) {
        var data = $('<td></td>');
        data.addClass('grid-col-name');
        data.text(metadata.columnnames[i]);
        headerRow.append(data);
    }
    table.append(headerRow);

    var typeRow = $('<tr></tr>');
    for (i = 0; i < metadata.columntypes.length; i++) {
        data = $('<td></td>');
        data.addClass('grid-col-type');
        data.text(metadata.columntypes[i]);
        typeRow.append(data);
    }
    table.append(typeRow);

    if (rows.length == 0) {
        Controller.finishQuery().then(function() {
            displayStatus('All rows have been processed');
            hasNextPage(false);
        });
    } else {
        displayStatus('OK');
    }

    for (i = 0; i < rows.length; i++) {
        var dataRow = $('<tr></tr>');
        for (var j = 0; j < metadata.columnnames.length; j++) {
            data = $('<td></td>');
            var colname = metadata.columnnames[j];
            data.text(rows[i][colname]);
            dataRow.append(data);
        }

        table.append(dataRow);
    }
}

function displayError(error) {
    clearResults();
    var details = JSON.parse(error.message);
    var full_details = '';

    if (details.description != null) {
        var status = $('#status');
        status.addClass('error');
        status.text(details.description);

        full_details += details.description + '\n';
    }

    if (details.stacktrace != null) {
        var stacktrace = $('#stacktrace');
        full_details += details.stacktrace;
    }

    if (full_details !== '') {
        stacktrace.text(full_details);
    }

    Controller.finishQuery();
}

function tableMetadataFullName(meta) {
    if (meta.catalog == null) meta.catalog = '';
    if (meta.schema == null) meta.schema = '';

    return '"' +
        meta.catalog.replace(/"/g, '""') + '"."' +
        meta.schema.replace(/"/g, '""') + '"."' +
        meta.table.replace(/"/g, '""') + '"';
}

function refreshSchema() {
    var tableUI = $('#table-list');
    var viewUI = $('#view-list');

    tableUI.empty();
    viewUI.empty();

    function make_column_loader(element, schema, catalog, table) {
        return function(event) {
            element.empty();
            Controller.queryColumns(schema, catalog, table).then(function(columns) {
                for (var i = 0; i < columns.length; i++) {
                    var columnEntry = $('<li></li>');

                    var columnType = $('<span></span>');
                    columnType.addClass('schema-column-type');
                    columnType.text(columns[i].datatype);

                    var columnName = $('<span></span>');
                    columnName.addClass('schema-column-name');
                    columnName.text(columns[i].column);

                    columnEntry.append(columnType);
                    columnEntry.append(columnName);
                    element.append(columnEntry);
                }
            }).catch(displayError);
        };
    }

    Promise.all([Controller.queryTables(), Controller.queryViews()])
        .then(function(results) {
            var tables = results[0];
            var views = results[1];

            for (var i = 0; i < tables.length; i++) {
                var table = tables[i];
                var fullName = tableMetadataFullName(table);

                var tableEntry = $('<li></li>');
                var tableName = $('<span></span>');
                tableName.addClass('schema-table');
                tableName.text(fullName);

                var columnListing = $('<ul></ul>');

                var tableButton = $('<button></button>');
                tableButton.addClass('schema-table-reload');
                tableButton.text('+');
                tableButton.click(make_column_loader(columnListing, table.catalog, table.schema, table.table));

                tableEntry.append(tableButton);
                tableEntry.append(tableName);
                tableUI.append(tableEntry);
                tableUI.append(columnListing);
            }

            for (i = 0; i < views.length; i++) {
                var view = views[i];
                fullName = tableMetadataFullName(view);

                var viewEntry = $('<li></li>');
                var viewName = $('<span></span>');
                viewName.addClass('schema-view');
                viewName.text(fullName);

                columnListing = $('<ul></ul>');

                var viewButton = $('<button></button>');
                viewButton.addClass('schema-table-reload');
                viewButton.text('+');
                viewButton.click(make_column_loader(columnListing, view.catalog, view.schema, view.view));

                viewEntry.append(viewButton);
                viewEntry.append(viewName);
                viewUI.append(viewEntry);
                viewUI.append(columnListing);
            }
        }).catch(displayError);
}

$('#connect').click(function() {
    var server = $('#address').val();
    Controller.connect(server).then(function() {
        displayStatus('Connected to ' + server);
        refreshSchema();
    }).catch(displayError);
});

$('#disconnect').click(function() {
    Controller.disconnect().then(function() {
        displayStatus('Disconnected');
    }).catch(displayError);
});

$('#refresh-schema').click(function() {
    refreshSchema();
});

function pageDisplayHelper() {
    return Controller.queryMetadata().then(function(metadata) {
        if (metadata.columnnames.length == 0) {
            hasNextPage(false);
            Controller.queryResultCount().then(displayCount);
        } else {
            Controller.queryNextPage().then(function(page) {
                displayPage(metadata, page);
                hasNextPage(true);
            });
        }
    });
}

$('#query-input').keypress(function(event) {
    if (event.key == 'Enter' && event.shiftKey) {
        event.preventDefault();
        Controller.executeQuery($('#query-input').val())
            .then(pageDisplayHelper)
            .catch(displayError);
    }
});

$('#next-page').click(function() {
    pageDisplayHelper()
        .catch(displayError);
});

function clearTabStates() {
    $('#display-tab-bar .tab-button').removeClass('enabled');
    $('#display-tab-containers .tab-page').removeClass('enabled');
}

function bindTabActions(tabId, containerId) {
    $(tabId).click(function() {
        clearTabStates();
        $(tabId).addClass('enabled');
        $(containerId).addClass('enabled');
    });
}

bindTabActions('#grid-tab-button', '#grid-container');
bindTabActions('#result-count-button', '#count-container');
bindTabActions('#error-button', '#error-container');
