/************************************************************
 * Google Sheets Archiver v4.3
 * ----------------------------------------------------------
 * v4.3 — НАДЁЖНЫЙ ПОДХОД:
 * - Preview хранится в PropertiesService (не CacheService,
 *   не скрытые листы) — строго по чанкам ROW_CHUNK строк
 * - Каждая функция логирует шаги через log_()
 * - Фронт отображает лог в терминале операций
 * - Нет двойного определения clearPreview_
 ************************************************************/

const CFG = {
  LOG_SHEET:       'Лог Архива',
  ARCHIVE_SUFFIX:  ' - Архив',
  HEADER_ROW:      1,
  PAGE_SIZE_DEF:   50,
  PAGE_SIZE_MAX:   200,
  TYPE_SAMPLE:     200,
  UNIQUE_LIMIT:    300,
  WRITE_CHUNK:     2000,
  SCAN_CHUNK:      5000,
  PREVIEW_KEY:     'ARCH_PREVIEW_V42',
  ROW_CHUNK:       800,   // строк на один Property-ключ (~7KB при 4 цифрах на строку)
};

/* ── лог текущего вызова ── */
const _opLog = [];
function log_(msg) {
  const ts = new Date().toISOString().substr(11, 12);
  const line = `[${ts}] ${msg}`;
  _opLog.push(line);
  Logger.log(line);
}
function getLog_() { return _opLog.join('\n'); }

/* =========================================================
   UI ENTRY
========================================================= */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Архивация')
    .addItem('Открыть архиватор v4.3', 'showArchiveDialog')
    .addToUi();
}

function showArchiveDialog() {
  const html = HtmlService.createHtmlOutputFromFile('ArchiveDialog')
    .setWidth(1600).setHeight(980);
  SpreadsheetApp.getUi().showModalDialog(html, 'Архивация данных v4.3');
}

/* =========================================================
   PUBLIC API
========================================================= */

function getAvailableSheets() {
  log_('getAvailableSheets: start');
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets()
    .map(sh => sh.getName())
    .filter(n => n !== CFG.LOG_SHEET && !n.startsWith('__ARCHIVE'))
    .map(n => ({ name: n }));
  log_('getAvailableSheets: found ' + sheets.length);
  return { sheets: sheets, log: getLog_() };
}

function getSheetMetadataV31(sheetName) {
  log_('getSheetMetadata: start "' + sheetName + '"');
  try {
    const sheet   = sheetOrThrow_(sheetName);
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    log_('getSheetMetadata: lastRow=' + lastRow + ' lastCol=' + lastCol);

    if (lastCol === 0) throw new Error('Лист "' + sheetName + '" пуст.');

    const headers   = sheet.getRange(CFG.HEADER_ROW, 1, 1, lastCol).getDisplayValues()[0];
    const totalRows = Math.max(0, lastRow - CFG.HEADER_ROW);
    log_('getSheetMetadata: headers=' + headers.length + ' totalRows=' + totalRows);

    let columnTypes = new Array(lastCol).fill('text');
    if (totalRows > 0) {
      const sRows  = Math.min(totalRows, CFG.TYPE_SAMPLE);
      const sample = sheet.getRange(CFG.HEADER_ROW + 1, 1, sRows, lastCol).getValues();
      columnTypes  = inferTypes_(headers, sample);
      log_('getSheetMetadata: types inferred');
    }

    log_('getSheetMetadata: DONE');
    return { sheetName: sheetName, headers: headers, columnTypes: columnTypes, totalRows: totalRows, totalColumns: lastCol, log: getLog_() };
  } catch(e) {
    log_('getSheetMetadata: ERROR ' + e.message);
    throw new Error('[getSheetMetadata] ' + e.message + '\n\nЛог:\n' + getLog_());
  }
}

function getColumnUniqueValuesV31(payload) {
  log_('getColumnUnique: col=' + (payload && payload.columnIndex));
  try {
    if (!payload || !payload.sheetName || payload.columnIndex === undefined) throw new Error('Не переданы параметры.');
    const sheet   = sheetOrThrow_(payload.sheetName);
    const lastRow = sheet.getLastRow();
    if (lastRow <= CFG.HEADER_ROW) return { values: [], log: getLog_() };

    const col = Number(payload.columnIndex);
    const raw = sheet.getRange(CFG.HEADER_ROW + 1, col + 1, lastRow - CFG.HEADER_ROW, 1).getDisplayValues();
    const set = new Set();
    for (let i = 0; i < raw.length; i++) {
      const v = (raw[i][0] || '').toString().trim();
      if (v) set.add(v);
    }
    let values = Array.from(set).sort(function(a,b){ return a.localeCompare(b,'ru'); });
    const q = norm_(payload.searchTerm || '');
    if (q) values = values.filter(function(v){ return norm_(v).includes(q); });
    log_('getColumnUnique: found ' + values.length);
    return { values: values.slice(0, CFG.UNIQUE_LIMIT), log: getLog_() };
  } catch(e) {
    log_('getColumnUnique: ERROR ' + e.message);
    throw new Error('[getColumnUnique] ' + e.message + '\n\nЛог:\n' + getLog_());
  }
}

/* =========================================================
   PREVIEW  v4.3 STATELESS — никакого хранилища rowNumbers.
   previewArchiveV31  — полный скан, counts + стр.1
   getPreviewPageV31  — повторный скан с пропуском offset
   performArchiveV31  — scanRows_ (только rowNumbers)
========================================================= */

function previewArchiveV31(payload) {
  log_('previewArchive: start');
  try {
    validatePayload_(payload, true);
    log_('previewArchive: sheet="' + payload.sheetName + '" filters=' + payload.filters.length + ' mode=' + payload.archiveMode);

    var sheet    = sheetOrThrow_(payload.sheetName);
    var headers  = getHeaders_(sheet);
    var lastCol  = sheet.getLastColumn();
    var pageSize = clampPageSize_(payload.pageSize);
    log_('previewArchive: headers=' + headers.length + ' pageSize=' + pageSize);

    // Два скана параллельно невозможны в GAS — делаем один скан, который собирает обе страницы сразу
    var both = scanForBothPages_(sheet, payload, lastCol, pageSize);
    log_('previewArchive: archiveCount=' + both.archiveCount + ' remainCount=' + both.remainCount);

    log_('previewArchive: DONE');
    return {
      headers:      headers,
      pageSize:     pageSize,
      archiveCount: both.archiveCount,
      remainCount:  both.remainCount,
      matchedCount: both.matchedCount,
      archivePage:  1,
      remainPage:   1,
      archiveRows:  both.archiveRows,
      remainRows:   both.remainRows,
      log:          getLog_()
    };
  } catch(e) {
    log_('previewArchive: ERROR ' + e.message);
    throw new Error('[previewArchive] ' + e.message + '\n\nЛог:\n' + getLog_());
  }
}

function getPreviewPageV31(payload) {
  log_('getPreviewPage: target=' + (payload && payload.target) + ' page=' + (payload && payload.page));
  try {
    validatePayload_(payload, true);
    var target   = payload.target === 'remain' ? 'remain' : 'archive';
    var page     = Math.max(1, Number(payload.page || 1));
    var pageSize = clampPageSize_(payload.pageSize);

    var sheet   = sheetOrThrow_(payload.sheetName);
    var lastCol = sheet.getLastColumn();

    var result = scanForPage_(sheet, payload, lastCol, target, page, pageSize);
    var total  = target === 'archive' ? result.archiveCount : result.remainCount;
    log_('getPreviewPage: done rows=' + result.rows.length + ' total=' + total);
    return { target: target, page: page, pageSize: pageSize, totalCount: total, rows: result.rows, log: getLog_() };
  } catch(e) {
    log_('getPreviewPage: ERROR ' + e.message);
    throw new Error('[getPreviewPage] ' + e.message + '\n\nЛог:\n' + getLog_());
  }
}

/**
 * Один скан — собирает первую страницу и для archive, и для remain.
 * Используется только в previewArchiveV31 чтобы не делать два прохода.
 */
function scanForBothPages_(sheet, payload, lastCol, pageSize) {
  var filters     = payload.filters;
  var combineMode = payload.combineMode;
  var archiveMode = payload.archiveMode;

  var colSet = {};
  for (var fi = 0; fi < filters.length; fi++) colSet[Number(filters[fi].columnIndex)] = true;
  var colIndexes = Object.keys(colSet).map(Number).sort(function(a,b){return a-b;});
  var groups = buildColGroups_(colIndexes);

  var lastRow   = sheet.getLastRow();
  var totalData = Math.max(0, lastRow - CFG.HEADER_ROW);

  var archiveCount = 0, remainCount = 0, matchedCount = 0;
  var archiveRowNums = [], remainRowNums = [];

  var offset = 0, chunkN = 0;
  while (offset < totalData) {
    var size     = Math.min(CFG.SCAN_CHUNK, totalData - offset);
    var startRow = CFG.HEADER_ROW + 1 + offset;
    chunkN++;
    log_('scanBoth: chunk#' + chunkN + ' rows ' + startRow + '..' + (startRow+size-1));

    var groupData = [];
    for (var gi = 0; gi < groups.length; gi++) {
      groupData.push(sheet.getRange(startRow, groups[gi].startCol + 1, size, groups[gi].len).getValues());
    }

    for (var i = 0; i < size; i++) {
      var rowMap = {};
      for (var g = 0; g < groups.length; g++) {
        for (var jj = 0; jj < groups[g].len; jj++) {
          rowMap[groups[g].startCol + jj] = groupData[g][i][jj];
        }
      }

      var matched   = evalRow_(rowMap, filters, combineMode);
      if (matched) matchedCount++;
      var toArchive = archiveMode === 'MATCH' ? matched : !matched;

      if (toArchive) {
        archiveCount++;
        if (archiveRowNums.length < pageSize) archiveRowNums.push(startRow + i);
      } else {
        remainCount++;
        if (remainRowNums.length < pageSize) remainRowNums.push(startRow + i);
      }
    }

    offset += size;
  }

  log_('scanBoth: done archive=' + archiveCount + ' remain=' + remainCount);

  return {
    archiveCount: archiveCount,
    remainCount:  remainCount,
    matchedCount: matchedCount,
    archiveRows:  fetchByRowNums_(sheet, archiveRowNums, lastCol),
    remainRows:   fetchByRowNums_(sheet, remainRowNums,  lastCol)
  };
}

/**
 * Скан для одной страницы конкретного target.
 * Считает полные counts + собирает нужный slice строк.
 */
function scanForPage_(sheet, payload, lastCol, target, page, pageSize) {
  var filters     = payload.filters;
  var combineMode = payload.combineMode;
  var archiveMode = payload.archiveMode;

  var colSet = {};
  for (var fi = 0; fi < filters.length; fi++) colSet[Number(filters[fi].columnIndex)] = true;
  var colIndexes = Object.keys(colSet).map(Number).sort(function(a,b){return a-b;});
  var groups = buildColGroups_(colIndexes);

  var lastRow   = sheet.getLastRow();
  var totalData = Math.max(0, lastRow - CFG.HEADER_ROW);

  var archiveCount = 0, remainCount = 0, matchedCount = 0;
  var pageOffset   = (page - 1) * pageSize;
  var collected    = 0;
  var pageRowNums  = [];

  var offset = 0, chunkN = 0;
  while (offset < totalData) {
    var size     = Math.min(CFG.SCAN_CHUNK, totalData - offset);
    var startRow = CFG.HEADER_ROW + 1 + offset;
    chunkN++;
    log_('scanPage: chunk#' + chunkN + ' rows ' + startRow + '..' + (startRow+size-1));

    var groupData = [];
    for (var gi = 0; gi < groups.length; gi++) {
      groupData.push(sheet.getRange(startRow, groups[gi].startCol + 1, size, groups[gi].len).getValues());
    }

    for (var i = 0; i < size; i++) {
      var rowMap = {};
      for (var g = 0; g < groups.length; g++) {
        for (var jj = 0; jj < groups[g].len; jj++) {
          rowMap[groups[g].startCol + jj] = groupData[g][i][jj];
        }
      }

      var matched   = evalRow_(rowMap, filters, combineMode);
      if (matched) matchedCount++;
      var toArchive = archiveMode === 'MATCH' ? matched : !matched;

      if (toArchive) archiveCount++;
      else           remainCount++;

      var inTarget = (target === 'archive') ? toArchive : !toArchive;
      if (inTarget) {
        if (collected < pageOffset) {
          collected++;
        } else if (pageRowNums.length < pageSize) {
          pageRowNums.push(startRow + i);
        }
      }
    }

    offset += size;
  }

  log_('scanPage: done archiveCount=' + archiveCount + ' remainCount=' + remainCount + ' pageRows=' + pageRowNums.length);

  return {
    archiveCount: archiveCount,
    remainCount:  remainCount,
    matchedCount: matchedCount,
    rows:         fetchByRowNums_(sheet, pageRowNums, lastCol)
  };
}

/* =========================================================
   ARCHIVE OPERATION
========================================================= */

function performArchiveV31(payload) {
  log_('performArchive: start');
  try {
    validatePayload_(payload, false);
    var lock = LockService.getDocumentLock();
    lock.waitLock(30000);
    log_('performArchive: lock acquired');

    var opId    = 'ARCH-' + Utilities.getUuid().split('-')[0].toUpperCase() + '-' + Date.now();
    var srcName  = payload.sheetName;
    var archName = srcName + CFG.ARCHIVE_SUFFIX;

    try {
      var ss      = SpreadsheetApp.getActiveSpreadsheet();
      var src     = sheetOrThrow_(srcName);
      var headers = getHeaders_(src);
      var lastRow = src.getLastRow();
      var lastCol = src.getLastColumn();
      log_('performArchive: lastRow=' + lastRow + ' lastCol=' + lastCol);

      if (lastRow <= CFG.HEADER_ROW) {
        writeLog_(opId, srcName, archName, [], payload, 'NO_DATA', 'Нет данных на листе.');
        return { success: true, operationId: opId, archivedCount: 0, archiveSheetName: archName, message: 'На листе нет данных.', log: getLog_() };
      }

      log_('performArchive: scanning rows...');
      var archiveRowNumbers = scanRows_(src, payload).archiveRowNumbers;
      log_('performArchive: scan done rows=' + archiveRowNumbers.length);

      if (!archiveRowNumbers.length) {
        writeLog_(opId, srcName, archName, [], payload, 'NO_MATCH', 'Нет строк по условиям.');
        return { success: true, operationId: opId, archivedCount: 0, archiveSheetName: archName, message: 'Нет строк для архивации.', log: getLog_() };
      }

      log_('performArchive: reading source data...');
      var dataRows   = lastRow - CFG.HEADER_ROW;
      var allValues  = src.getRange(CFG.HEADER_ROW + 1, 1, dataRows, lastCol).getValues();
      var allFormulas= src.getRange(CFG.HEADER_ROW + 1, 1, dataRows, lastCol).getFormulasR1C1();
      log_('performArchive: data read OK');

      var archSet  = new Set(archiveRowNumbers);
      var archVals = [], remVals = [], remForm = [];
      for (var i = 0; i < allValues.length; i++) {
        if (archSet.has(CFG.HEADER_ROW + 1 + i)) {
          archVals.push(allValues[i]);
        } else {
          remVals.push(allValues[i]);
          remForm.push(allFormulas[i]);
        }
      }
      log_('performArchive: split done archive=' + archVals.length + ' remain=' + remVals.length);

      var archSheet = getOrCreateSheet_(ss, archName, headers);
      ensureHeader_(archSheet, headers);
      appendChunked_(archSheet, archVals);
      log_('performArchive: archive written');

      rebuildSheet_(src, headers, remVals, remForm);
      log_('performArchive: source rebuilt');

      clearPreview_(srcName);

      var msg = 'Успешно архивировано ' + archiveRowNumbers.length + ' строк.';
      writeLog_(opId, srcName, archName, archiveRowNumbers, payload, 'SUCCESS', msg);
      log_('performArchive: DONE');
      return { success: true, operationId: opId, archivedCount: archiveRowNumbers.length, archiveSheetName: archName, message: msg, log: getLog_() };

    } finally {
      lock.releaseLock();
    }
  } catch(e) {
    log_('performArchive: ERROR ' + e.message);
    throw new Error('[performArchive] ' + e.message + '\n\nЛог:\n' + getLog_());
  }
}

/* =========================================================
   SAVED FILTERS & TEMPLATES
========================================================= */

function getSavedFilterSetsV31() {
  var map = userJson_('ARCHIVER_FILTER_SETS');
  var items = Object.keys(map).sort(function(a,b){return a.localeCompare(b,'ru');}).map(function(n){return Object.assign({name:n},map[n]);});
  return { items: items, log: getLog_() };
}
function saveFilterSetV31(payload) {
  if (!payload || !payload.name) throw new Error('Укажи название набора.');
  var map = userJson_('ARCHIVER_FILTER_SETS');
  map[payload.name] = { config: payload.config, updatedAt: new Date().toISOString() };
  setUserJson_('ARCHIVER_FILTER_SETS', map);
  return { success: true, log: getLog_() };
}
function deleteFilterSetV31(name) {
  var map = userJson_('ARCHIVER_FILTER_SETS');
  delete map[name];
  setUserJson_('ARCHIVER_FILTER_SETS', map);
  return { success: true, log: getLog_() };
}
function getArchiveTemplatesV31() {
  var map = docJson_('ARCHIVER_TEMPLATES');
  var items = Object.keys(map).sort(function(a,b){return a.localeCompare(b,'ru');}).map(function(n){return Object.assign({name:n},map[n]);});
  return { items: items, log: getLog_() };
}
function saveArchiveTemplateV31(payload) {
  if (!payload || !payload.name) throw new Error('Укажи название шаблона.');
  var map = docJson_('ARCHIVER_TEMPLATES');
  map[payload.name] = { config: payload.config, updatedAt: new Date().toISOString() };
  setDocJson_('ARCHIVER_TEMPLATES', map);
  return { success: true, log: getLog_() };
}
function deleteArchiveTemplateV31(name) {
  var map = docJson_('ARCHIVER_TEMPLATES');
  delete map[name];
  setDocJson_('ARCHIVER_TEMPLATES', map);
  return { success: true, log: getLog_() };
}

/* =========================================================
   PREVIEW STORAGE (PropertiesService)
========================================================= */

function savePreview_(sheetName, archRowNums, remRowNums) {
  log_('savePreview: archive=' + archRowNums.length + ' remain=' + remRowNums.length);
  var props = PropertiesService.getDocumentProperties();
  var base  = CFG.PREVIEW_KEY + ':' + sheetName + ':';
  var chunk = CFG.ROW_CHUNK;

  var aChunks = Math.ceil(archRowNums.length / chunk);
  var rChunks = Math.ceil(remRowNums.length  / chunk);

  var meta = {
    archiveCount: archRowNums.length,
    remainCount:  remRowNums.length,
    aChunks:      aChunks,
    rChunks:      rChunks,
    savedAt:      Date.now()
  };
  props.setProperty(base + 'meta', JSON.stringify(meta));
  log_('savePreview: meta saved aChunks=' + aChunks + ' rChunks=' + rChunks);

  for (var i = 0; i < aChunks; i++) {
    props.setProperty(base + 'a' + i, JSON.stringify(archRowNums.slice(i * chunk, (i+1) * chunk)));
  }
  for (var j = 0; j < rChunks; j++) {
    props.setProperty(base + 'r' + j, JSON.stringify(remRowNums.slice(j * chunk, (j+1) * chunk)));
  }
  log_('savePreview: done');
}

function loadPreview_(sheetName) {
  var props    = PropertiesService.getDocumentProperties();
  var base     = CFG.PREVIEW_KEY + ':' + sheetName + ':';
  var metaRaw  = props.getProperty(base + 'meta');
  if (!metaRaw) { log_('loadPreview: no meta'); return null; }

  var meta;
  try { meta = JSON.parse(metaRaw); } catch(e) { log_('loadPreview: meta parse error'); return null; }

  var archRowNums = [];
  for (var i = 0; i < meta.aChunks; i++) {
    var raw = props.getProperty(base + 'a' + i);
    if (!raw) { log_('loadPreview: missing archive chunk ' + i); return null; }
    var chunk = JSON.parse(raw);
    for (var j = 0; j < chunk.length; j++) archRowNums.push(chunk[j]);
  }

  var remRowNums = [];
  for (var k = 0; k < meta.rChunks; k++) {
    var rawR = props.getProperty(base + 'r' + k);
    if (!rawR) { log_('loadPreview: missing remain chunk ' + k); return null; }
    var chunkR = JSON.parse(rawR);
    for (var m = 0; m < chunkR.length; m++) remRowNums.push(chunkR[m]);
  }

  log_('loadPreview: loaded archive=' + archRowNums.length + ' remain=' + remRowNums.length);
  return { archiveRowNumbers: archRowNums, remainRowNumbers: remRowNums };
}

function clearPreview_(sheetName) {
  var props   = PropertiesService.getDocumentProperties();
  var base    = CFG.PREVIEW_KEY + ':' + sheetName + ':';
  var metaRaw = props.getProperty(base + 'meta');
  var aC = 0, rC = 0;
  if (metaRaw) {
    try { var m = JSON.parse(metaRaw); aC = m.aChunks || 0; rC = m.rChunks || 0; } catch(e) { aC = 50; rC = 50; }
  }
  try { props.deleteProperty(base + 'meta'); } catch(e) {}
  for (var i = 0; i < Math.max(aC, 1); i++) { try { props.deleteProperty(base + 'a' + i); } catch(e) {} }
  for (var j = 0; j < Math.max(rC, 1); j++) { try { props.deleteProperty(base + 'r' + j); } catch(e) {} }
  log_('clearPreview: done');
}

/* =========================================================
   ROW SCANNING
========================================================= */

function scanRows_(sheet, payload) {
  log_('scanRows: start');
  var filters     = payload.filters;
  var combineMode = payload.combineMode;
  var archiveMode = payload.archiveMode;

  var colSet = {};
  for (var fi = 0; fi < filters.length; fi++) colSet[Number(filters[fi].columnIndex)] = true;
  var colIndexes = Object.keys(colSet).map(Number).sort(function(a,b){return a-b;});
  log_('scanRows: filter columns=' + JSON.stringify(colIndexes));

  var lastRow   = sheet.getLastRow();
  var totalData = Math.max(0, lastRow - CFG.HEADER_ROW);
  log_('scanRows: totalData=' + totalData);

  var archiveRowNumbers = [];
  var remainRowNumbers  = [];
  var matchedCount = 0;

  if (totalData === 0) {
    log_('scanRows: no data');
    return { archiveRowNumbers: archiveRowNumbers, remainRowNumbers: remainRowNumbers, matchedCount: 0 };
  }

  var groups = buildColGroups_(colIndexes);
  log_('scanRows: groups=' + groups.length);

  var offset = 0;
  var chunkN = 0;
  while (offset < totalData) {
    var size     = Math.min(CFG.SCAN_CHUNK, totalData - offset);
    var startRow = CFG.HEADER_ROW + 1 + offset;
    chunkN++;
    log_('scanRows: chunk#' + chunkN + ' rows ' + startRow + '..' + (startRow+size-1));

    var groupData = [];
    for (var gi = 0; gi < groups.length; gi++) {
      groupData.push(sheet.getRange(startRow, groups[gi].startCol + 1, size, groups[gi].len).getValues());
    }

    for (var i = 0; i < size; i++) {
      var rowMap = {};
      for (var g = 0; g < groups.length; g++) {
        for (var jj = 0; jj < groups[g].len; jj++) {
          rowMap[groups[g].startCol + jj] = groupData[g][i][jj];
        }
      }
      var matched = evalRow_(rowMap, filters, combineMode);
      if (matched) matchedCount++;
      if ((archiveMode === 'MATCH') ? matched : !matched) archiveRowNumbers.push(startRow + i);
      else remainRowNumbers.push(startRow + i);
    }
    offset += size;
  }

  log_('scanRows: done matched=' + matchedCount + ' archive=' + archiveRowNumbers.length + ' remain=' + remainRowNumbers.length);
  return { archiveRowNumbers: archiveRowNumbers, remainRowNumbers: remainRowNumbers, matchedCount: matchedCount };
}

/* =========================================================
   PAGE FETCH
========================================================= */

function fetchPageRows_(sheet, rowNumbers, page, pageSize, lastCol) {
  var total  = rowNumbers.length;
  var offset = (page - 1) * pageSize;
  if (offset >= total) return [];
  var slice = rowNumbers.slice(offset, offset + pageSize);
  return fetchByRowNums_(sheet, slice, lastCol);
}

function fetchByRowNums_(sheet, rowNums, lastCol) {
  if (!rowNums || !rowNums.length) return [];
  var groups = [];
  var s = rowNums[0], e = rowNums[0];
  for (var i = 1; i < rowNums.length; i++) {
    if (rowNums[i] === e + 1) { e = rowNums[i]; }
    else { groups.push({s:s, c:e-s+1}); s = e = rowNums[i]; }
  }
  groups.push({s:s, c:e-s+1});

  var result = [];
  for (var gi = 0; gi < groups.length; gi++) {
    var g = groups[gi];
    var vals = sheet.getRange(g.s, 1, g.c, lastCol).getDisplayValues();
    for (var r = 0; r < vals.length; r++) {
      result.push({ rowNumber: g.s + r, cells: vals[r] });
    }
  }
  return result;
}

/* =========================================================
   FILTER ENGINE
========================================================= */

function evalRow_(rowMap, filters, combineMode) {
  if (!filters || !filters.length) return false;
  var results = [];
  for (var i = 0; i < filters.length; i++) results.push(evalCond_(rowMap[filters[i].columnIndex], filters[i]));
  return combineMode === 'OR' ? results.some(Boolean) : results.every(Boolean);
}

function evalCond_(cell, f) {
  var op = f.operator || 'equals';
  var sv = Array.isArray(f.selectedValues) ? f.selectedValues : [];
  if (f.excludeBlanks && isEmpty_(cell)) return false;
  if (op === 'is_empty')     return isEmpty_(cell);
  if (op === 'is_not_empty') return !isEmpty_(cell);
  if (op === 'in_list' || op === 'not_in_list') {
    var cn = norm_(cell);
    var has = sv.map(norm_).indexOf(cn) !== -1;
    return op === 'in_list' ? has : !has;
  }
  switch (f.type || 'text') {
    case 'number':  return cmpNum_(cell, op, f.value1, f.value2);
    case 'date':    return cmpDate_(cell, op, f.value1, f.value2);
    case 'boolean': return cmpBool_(cell, op, f.value1);
    default:        return cmpText_(cell, op, f.value1);
  }
}

function cmpText_(cell, op, v1) {
  var c = norm_(cell), v = norm_(v1);
  switch(op) {
    case 'equals':       return c === v;
    case 'not_equals':   return c !== v;
    case 'contains':     return c.indexOf(v) !== -1;
    case 'not_contains': return c.indexOf(v) === -1;
    case 'starts_with':  return c.indexOf(v) === 0;
    case 'ends_with':    return c.lastIndexOf(v) === c.length - v.length;
    default: return false;
  }
}

function cmpNum_(cell, op, v1, v2) {
  var c = parseNum_(cell), n1 = parseNum_(v1), n2 = parseNum_(v2);
  if (c === null) return false;
  switch(op) {
    case 'equals':     return c === n1;
    case 'not_equals': return c !== n1;
    case 'gt':  return c > n1;
    case 'gte': return c >= n1;
    case 'lt':  return c < n1;
    case 'lte': return c <= n1;
    case 'between': return n1!==null && n2!==null && c >= Math.min(n1,n2) && c <= Math.max(n1,n2);
    default: return false;
  }
}

function cmpDate_(cell, op, v1, v2) {
  var d = parseDate_(cell), d1 = parseDate_(v1), d2 = parseDate_(v2);
  if (!d) return false;
  var t = d.getTime();
  switch(op) {
    case 'on':
    case 'equals':     return d1 ? t === d1.getTime() : false;
    case 'not_equals': return d1 ? t !== d1.getTime() : false;
    case 'before':     return d1 ? t < d1.getTime() : false;
    case 'after':      return d1 ? t > d1.getTime() : false;
    case 'between':    return d1 && d2 ? t >= Math.min(d1.getTime(),d2.getTime()) && t <= Math.max(d1.getTime(),d2.getTime()) : false;
    default: return false;
  }
}

function cmpBool_(cell, op, v1) {
  var c = parseBool_(cell), b = parseBool_(v1);
  switch(op) {
    case 'equals':     return c === b;
    case 'not_equals': return c !== b;
    default: return false;
  }
}

/* =========================================================
   ARCHIVE / REBUILD
========================================================= */

function appendChunked_(sheet, rows) {
  if (!rows || !rows.length) return;
  var startRow = sheet.getLastRow() + 1;
  var cols = rows[0].length;
  for (var i = 0; i < rows.length; i += CFG.WRITE_CHUNK) {
    var chunk = rows.slice(i, i + CFG.WRITE_CHUNK);
    sheet.getRange(startRow, 1, chunk.length, cols).setValues(chunk);
    startRow += chunk.length;
  }
}

function rebuildSheet_(sheet, headers, remVals, remForm) {
  var cols    = headers.length;
  var oldData = Math.max(0, sheet.getLastRow() - CFG.HEADER_ROW);
  var newData = remVals.length;
  if (oldData > 0) sheet.getRange(CFG.HEADER_ROW + 1, 1, oldData, cols).clearContent();
  if (newData === 0) return;

  var writeRow = CFG.HEADER_ROW + 1;
  for (var i = 0; i < newData; i += CFG.WRITE_CHUNK) {
    var vChunk = remVals.slice(i, i + CFG.WRITE_CHUNK);
    var fChunk = remForm.slice(i, i + CFG.WRITE_CHUNK);
    sheet.getRange(writeRow, 1, vChunk.length, cols).setValues(vChunk);
    for (var r = 0; r < vChunk.length; r++) {
      var rowF = fChunk[r]; var segS = -1;
      for (var c = 0; c <= cols; c++) {
        var hasF = c < cols && rowF[c] && String(rowF[c]).trim();
        if (hasF && segS === -1) segS = c;
        if (!hasF && segS !== -1) {
          sheet.getRange(writeRow+r, segS+1, 1, c-segS).setFormulasR1C1([rowF.slice(segS,c)]);
          segS = -1;
        }
      }
    }
    writeRow += vChunk.length;
  }
}

/* =========================================================
   VALIDATION
========================================================= */

function validatePayload_(payload, previewOnly) {
  if (!payload) throw new Error('Не переданы параметры.');
  if (!payload.sheetName) throw new Error('Не выбран лист.');
  if (!Array.isArray(payload.filters) || !payload.filters.length) throw new Error('Добавьте хотя бы один фильтр.');
  if (['AND','OR'].indexOf(payload.combineMode) === -1) throw new Error('Некорректная логика (AND или OR).');
  if (['MATCH','INVERT'].indexOf(payload.archiveMode) === -1) throw new Error('Некорректный режим (MATCH или INVERT).');
  if (!previewOnly) {
    if (!payload.archiveComment || !String(payload.archiveComment).trim()) throw new Error('Комментарий обязателен.');
    if (String(payload.archiveConfirm || '').trim() !== 'ARCHIVE') throw new Error('Введите ARCHIVE для подтверждения.');
  }
}

/* =========================================================
   HELPERS
========================================================= */

function sheetOrThrow_(name) {
  var s = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!s) throw new Error('Лист "' + name + '" не найден.');
  return s;
}
function getHeaders_(sheet) {
  return sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
}
function getOrCreateSheet_(ss, name, headers) {
  var s = ss.getSheetByName(name);
  if (!s) { s = ss.insertSheet(name); s.getRange(1,1,1,headers.length).setValues([headers]); s.setFrozenRows(1); }
  return s;
}
function ensureHeader_(sheet, headers) {
  if (sheet.getLastColumn() === 0) { sheet.getRange(1,1,1,headers.length).setValues([headers]); sheet.setFrozenRows(1); return; }
  var ex = sheet.getRange(1,1,1,headers.length).getDisplayValues()[0];
  if (!headers.every(function(h,i){ return String(h)===String(ex[i]); })) throw new Error('Структура архивного листа не совпадает с исходным.');
}
function clampPageSize_(v) {
  var n = Number(v || CFG.PAGE_SIZE_DEF);
  return (isNaN(n)||n<=0) ? CFG.PAGE_SIZE_DEF : Math.min(n, CFG.PAGE_SIZE_MAX);
}
function buildColGroups_(idxs) {
  if (!idxs.length) return [];
  var g=[]; var s=idxs[0], p=idxs[0];
  for (var i=1;i<idxs.length;i++) {
    if (idxs[i]===p+1) p=idxs[i];
    else { g.push({startCol:s,len:p-s+1}); s=p=idxs[i]; }
  }
  g.push({startCol:s,len:p-s+1});
  return g;
}
function userJson_(key) { var r=PropertiesService.getUserProperties().getProperty(key); if(!r)return{}; try{return JSON.parse(r);}catch(e){return{};} }
function setUserJson_(key,obj) { PropertiesService.getUserProperties().setProperty(key,JSON.stringify(obj||{})); }
function docJson_(key) { var r=PropertiesService.getDocumentProperties().getProperty(key); if(!r)return{}; try{return JSON.parse(r);}catch(e){return{};} }
function setDocJson_(key,obj) { PropertiesService.getDocumentProperties().setProperty(key,JSON.stringify(obj||{})); }
function norm_(v) { if(v===null||v===undefined)return''; return String(v).trim().toLowerCase(); }
function isEmpty_(v) { return v===''||v===null||v===undefined; }
function parseNum_(v) {
  if(typeof v==='number'&&!isNaN(v))return v;
  if(typeof v==='string'&&v.trim()){var n=Number(v.replace(/\s/g,'').replace(',','.')); if(!isNaN(n))return n;}
  return null;
}
function parseDate_(v) {
  if(v instanceof Date&&!isNaN(v))return strip_(v);
  if(typeof v==='string'&&v.trim()){
    var s=v.trim();
    if(/^\d{4}-\d{2}-\d{2}$/.test(s)){var d=new Date(s);if(!isNaN(d))return strip_(d);}
    if(/^\d{2}\.\d{2}\.\d{4}$/.test(s)){var p=s.split('.');var d2=new Date(+p[2],+p[1]-1,+p[0]);if(!isNaN(d2))return strip_(d2);}
    var d3=new Date(s);if(!isNaN(d3))return strip_(d3);
  }
  return null;
}
function strip_(d){return new Date(d.getFullYear(),d.getMonth(),d.getDate());}
function parseBool_(v){
  if(typeof v==='boolean')return v;
  if(typeof v==='number')return v!==0;
  if(typeof v==='string'){var s=v.trim().toLowerCase();if(['true','да','1'].indexOf(s)!==-1)return true;if(['false','нет','0'].indexOf(s)!==-1)return false;}
  return false;
}
function inferTypes_(headers, sample) {
  return headers.map(function(_,c){
    var col=[];
    for(var r=0;r<sample.length;r++){if(!isEmpty_(sample[r][c]))col.push(sample[r][c]);if(col.length>=50)break;}
    if(!col.length)return'text';
    var nums=0,dates=0,bools=0;
    for(var i=0;i<col.length;i++){
      if(parseDate_(col[i]))dates++;
      if(parseNum_(col[i])!==null)nums++;
      if(typeof col[i]==='boolean'||['true','false','да','нет'].indexOf(String(col[i]).toLowerCase())!==-1)bools++;
    }
    var t=col.length;
    if(dates/t>=0.9)return'date';
    if(nums/t>=0.9)return'number';
    if(bools/t>=0.9)return'boolean';
    return'text';
  });
}
function writeLog_(opId, srcName, archName, rowNums, payload, status, msg) {
  var ss=SpreadsheetApp.getActiveSpreadsheet();
  var s=ss.getSheetByName(CFG.LOG_SHEET);
  if(!s){
    s=ss.insertSheet(CFG.LOG_SHEET);
    s.getRange(1,1,1,13).setValues([['ID','Дата','Пользователь','Источник','Архив','Режим','Логика','Строк','Диапазоны','Комментарий','Фильтры','Статус','Сообщение']]);
    s.setFrozenRows(1);
  }
  var email='';try{email=Session.getActiveUser().getEmail();}catch(e){}
  var nums=[].concat(rowNums).sort(function(a,b){return a-b;});
  var ranges=[];
  if(nums.length){var st=nums[0],en=nums[0];for(var i=1;i<nums.length;i++){if(nums[i]===en+1)en=nums[i];else{ranges.push(st===en?''+st:st+'-'+en);st=en=nums[i];}}ranges.push(st===en?''+st:st+'-'+en);}
  s.appendRow([opId,new Date(),email,srcName,archName,payload.archiveMode||'',payload.combineMode||'',nums.length,ranges.join(', '),payload.archiveComment||'',JSON.stringify(payload.filters||[]),status,msg]);
}