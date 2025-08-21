/**
 * 从 GitHub Raw CSV 拉取并“追加”到当前表（按唯一键去重）
 * 唯一键：date | source | title | address
 */
function IMPORT_NOW() {
  const RAW_CSV_URL = 'https://raw.githubusercontent.com/OWNER/REPO/BRANCH/public/nyc_developers_daily.csv'; // ← 改成你的 Raw URL
  const SHEET_NAME  = 'Daily'; // 目标工作表名（不存在会自动创建）

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_NAME);

  const header = ["date","source","title","address","borough","developers","url"];

  // 读现有数据，构建去重集合
  const values = sh.getDataRange().getNumRows() ? sh.getDataRange().getValues() : [];
  let existing = {};
  if (values.length && values[0].join() === header.join()) {
    for (let i = 1; i < values.length; i++) {
      const row = values[i];
      if (!row[0]) continue;
      const key = [row[0], row[1], String(row[2]).toLowerCase(), String(row[3]).toLowerCase()].join('|');
      existing[key] = true;
    }
  } else {
    sh.clearContents();
    sh.getRange(1,1,1,header.length).setValues([header]);
  }

  // 拉取 CSV
  const resp = UrlFetchApp.fetch(RAW_CSV_URL, {muteHttpExceptions: true});
  if (resp.getResponseCode() !== 200) throw new Error('Fetch CSV failed: ' + resp.getResponseCode());
  const csv = Utilities.parseCsv(resp.getContentText());

  // 对齐列并去重后追加
  const colIndex = {};
  csv[0].forEach((name, idx) => colIndex[String(name).trim().toLowerCase()] = idx);
  const need = header.map(h => colIndex[h] ?? -1);
  const out = [];
  for (let i = 1; i < csv.length; i++) {
    const row = need.map(idx => (idx >= 0 ? csv[i][idx] : ''));
    const key = [row[0], row[1], String(row[2]).toLowerCase(), String(row[3]).toLowerCase()].join('|');
    if (!existing[key] && row[0]) {
      out.push(row);
      existing[key] = true;
    }
  }
  if (out.length) {
    sh.getRange(sh.getLastRow()+1, 1, out.length, header.length).setValues(out);
    sh.autoResizeColumns(1, header.length);
  }
}
