/**
 * issuer.js
 * =========
 * Automated formatter for payment issuer transaction reports.
 * Reads raw CSV/XLSX files from Google Drive, normalizes each issuer's
 * schema, and uploads standardized semicolon-delimited CSV files to GCS.
 *
 * Supported issuers: BTN, BCA, BNI, Mandiri, ShopeePay, Indodana,
 *                    Kredivo, LinkAja, OttoPay, OttoPay Dashboard, Nobu, BRI
 *
 * SETUP: Copy config.example.js → config.js and fill in your folder IDs,
 *        GCS bucket name, and email. Store your Service Account JSON in
 *        Script Properties under the key "GCS_KEY".
 */

// ─── Pull settings from CONFIG (defined in config.js) ────────────────────────
const OUTPUT_DELIM = CONFIG.OUTPUT_DELIMITER;
const TZ           = CONFIG.TIMEZONE;
const GCS_BUCKET   = CONFIG.GCS.BUCKET;
const GCS_SCOPE    = CONFIG.GCS.SCOPE;

// ─── GCS paths ────────────────────────────────────────────────────────────────
const GCS_PATH_BTN              = CONFIG.GCS.PATHS.BTN;
const GCS_PATH_BCA              = CONFIG.GCS.PATHS.BCA_ISSUER;
const GCS_PATH_BNI              = CONFIG.GCS.PATHS.BNI_ISSUER;
const GCS_PATH_MANDIRI          = CONFIG.GCS.PATHS.MANDIRI_ISSUER;
const GCS_PATH_SHOPEEPAY        = CONFIG.GCS.PATHS.SHOPEEPAY;
const GCS_PATH_INDODANA         = CONFIG.GCS.PATHS.INDODANA;
const GCS_PATH_KREDIVO          = CONFIG.GCS.PATHS.KREDIVO;
const GCS_PATH_LINKAJA          = CONFIG.GCS.PATHS.LINKAJA;
const GCS_PATH_OTTOPAY          = CONFIG.GCS.PATHS.OTTOPAY;
const GCS_PATH_OTTOPAY_DASHBOARD= CONFIG.GCS.PATHS.OTTOPAY_DASHBOARD;
const GCS_PATH_NOBU             = CONFIG.GCS.PATHS.NOBU;
const GCS_PATH_BRI              = CONFIG.GCS.PATHS.BRI;

// ─── Drive folder IDs ─────────────────────────────────────────────────────────
const SOURCE_FOLDER_ID_BTN       = CONFIG.FOLDERS.ISSUER.BTN.SRC;
const DEST_FOLDER_ID_BTN         = CONFIG.FOLDERS.ISSUER.BTN.DST;
const SRC_FOLDER_BCA             = CONFIG.FOLDERS.ISSUER.BCA.SRC;
const DST_FOLDER_BCA             = CONFIG.FOLDERS.ISSUER.BCA.DST;
const SOURCE_FOLDER_ID_OTTOPAY   = CONFIG.FOLDERS.ISSUER.OTTOPAY_DASHBOARD.SRC;
const DEST_FOLDER_ID_OTTOPAY     = CONFIG.FOLDERS.ISSUER.OTTOPAY_DASHBOARD.DST;
const SOURCE_FOLDER_ID_NOBU      = CONFIG.FOLDERS.ISSUER.NOBU.SRC;
const DEST_FOLDER_ID_NOBU        = CONFIG.FOLDERS.ISSUER.NOBU.DST;
const SOURCE_FOLDER_ID_BRI       = CONFIG.FOLDERS.ISSUER.BRI.SRC;
const DEST_FOLDER_ID_BRI         = CONFIG.FOLDERS.ISSUER.BRI.DST;


// =============================================================================
//  SHARED GCS UPLOAD HELPERS
// =============================================================================

/**
 * Generates a short-lived Bearer token by signing a JWT with the Service
 * Account private key stored in Script Properties under "GCS_KEY".
 * @returns {string} OAuth2 access token
 */
function getServiceAccountToken_() {
  const key = JSON.parse(PropertiesService.getScriptProperties().getProperty("GCS_KEY"));
  if (!key) throw new Error("GCS_KEY not found in Script Properties");

  const header = { alg: "RS256", typ: "JWT" };
  const now    = Math.floor(Date.now() / 1000);
  const claim  = {
    iss:   key.client_email,
    scope: GCS_SCOPE,
    aud:   "https://oauth2.googleapis.com/token",
    exp:   now + 3600,
    iat:   now
  };

  const encHeader    = Utilities.base64EncodeWebSafe(JSON.stringify(header));
  const encClaim     = Utilities.base64EncodeWebSafe(JSON.stringify(claim));
  const signature    = Utilities.computeRsaSha256Signature(encHeader + "." + encClaim, key.private_key);
  const encSignature = Utilities.base64EncodeWebSafe(signature);
  const jwt          = `${encHeader}.${encClaim}.${encSignature}`;

  const res = UrlFetchApp.fetch("https://oauth2.googleapis.com/token", {
    method: "post",
    payload: { grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: jwt }
  });
  return JSON.parse(res.getContentText()).access_token;
}

/**
 * Uploads a CSV string to Google Cloud Storage.
 * @param {string} filename  - Object name (file name part only)
 * @param {string} content   - CSV string content
 * @param {string} gcsPath   - GCS path prefix (e.g. "recon-project/issuer/btn")
 */
function uploadCsvToGcs_(filename, content, gcsPath) {
  const token = getServiceAccountToken_();
  const url   = `https://storage.googleapis.com/upload/storage/v1/b/${GCS_BUCKET}/o`
              + `?uploadType=media&name=${encodeURIComponent(gcsPath + "/" + filename)}`;
  const res = UrlFetchApp.fetch(url, {
    method:      "post",
    contentType: "text/csv",
    payload:     content,
    headers:     { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });
  L(`📤 GCS upload [${filename}]: ${res.getResponseCode()}`);
}


// =============================================================================
//  SHARED UTILITY FUNCTIONS
// =============================================================================

/** Left-pads a number/string to 2 characters with a leading zero. */
function pad2(x) { x = String(x); return x.length === 1 ? "0" + x : x; }

/** Counts occurrences of a character in a string (used for delimiter detection). */
function countChar(s, ch) {
  if (s == null) return 0;
  return (String(s).match(new RegExp("\\" + ch, "g")) || []).length;
}


// =============================================================================
//  BTN — QRIS Transaction Report
//  Input:  CSV or XLSX
//  Output: Report_Transaksi_QRIS_<date>.csv
// =============================================================================

function processBTN() {
  const src   = DriveApp.getFolderById(SOURCE_FOLDER_ID_BTN);
  const dst   = DriveApp.getFolderById(DEST_FOLDER_ID_BTN);
  const files = src.getFiles();
  while (files.hasNext()) {
    const f    = files.next();
    const name = f.getName().toLowerCase();
    try {
      if      (name.endsWith(".csv"))  processBtnCsvFile_(f, dst);
      else if (name.endsWith(".xlsx")) processBtnXlsxFile_(f, dst);
      else L("⏩ Skip: " + f.getName());
    } catch (e) { L("❌ BTN " + f.getName() + ": " + e); }
  }
}

function processBtnCsvFile_(file, dstFolder) {
  const raw       = file.getBlob().getDataAsString();
  const firstLine = (raw.split(/\r?\n/)[0] || "");
  const inDelim   = countChar(firstLine, ";") > countChar(firstLine, ",") ? ";" : ",";
  let rows = Utilities.parseCsv(raw, inDelim);
  if (!rows || rows.length < 1) return;

  rows = normalizeAndFormatBTN_(rows);
  const idxDate = rows[0].map(h => h.toString().toLowerCase()).indexOf("transaction_date");
  const range   = getDateRangeBTN_(rows, idxDate);
  const outName = range.start === range.end
    ? `Report_Transaksi_QRIS_${range.start}.csv`
    : `Report_Transaksi_QRIS_${range.start}_sampai_${range.end}.csv`;

  const csv = rows.map(r => r.map(x => x == null ? "" : String(x)).join(OUTPUT_DELIM)).join("\n");
  dstFolder.createFile(outName, csv, MimeType.CSV);
  uploadCsvToGcs_(outName, csv, GCS_PATH_BTN);
  L("✅ BTN CSV: " + outName);
}

function processBtnXlsxFile_(file, dstFolder) {
  const temp = Drive.Files.copy(
    { title: "TEMP_" + file.getName(), parents: [{ id: SOURCE_FOLDER_ID_BTN }], mimeType: MimeType.GOOGLE_SHEETS },
    file.getId()
  );
  const ss  = SpreadsheetApp.openById(temp.id);
  const sh  = ss.getSheets()[0];
  let rows  = sh.getDataRange().getValues();
  if (!rows || rows.length < 1) { DriveApp.getFileById(temp.id).setTrashed(true); return; }

  rows = normalizeAndFormatBTN_(rows);
  const idxDate = rows[0].map(h => h.toString().toLowerCase()).indexOf("transaction_date");
  const range   = getDateRangeBTN_(rows, idxDate);
  const outName = range.start === range.end
    ? `Report_Transaksi_QRIS_${range.start}.csv`
    : `Report_Transaksi_QRIS_${range.start}_sampai_${range.end}.csv`;

  const csv = rows.map(r => r.map(x => x == null ? "" : String(x)).join(OUTPUT_DELIM)).join("\n");
  dstFolder.createFile(outName, csv, MimeType.CSV);
  uploadCsvToGcs_(outName, csv, GCS_PATH_BTN);
  DriveApp.getFileById(temp.id).setTrashed(true);
  L("✅ BTN XLSX: " + outName);
}

/** Normalizes BTN headers and field values to a standard schema. */
function normalizeAndFormatBTN_(rows) {
  if (!rows || rows.length === 0) return rows;
  rows[0] = rows[0].map(h => String(h || "").trim().replace(/\s+/g, "_"));
  const hnorm  = rows[0].map(h => h.toString().toLowerCase());
  const idxDate = hnorm.indexOf("transaction_date");
  const idxTime = hnorm.indexOf("transaction_time");
  const idxRRN  = hnorm.indexOf("retrieval_reference_number");
  const idxMID  = hnorm.indexOf("merchant_id");
  const idxCust = hnorm.indexOf("customer_name");
  const idxMDR  = hnorm.indexOf("mdr");

  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (idxDate > -1) r[idxDate] = normalizeDateBTN_(r[idxDate]);
    if (idxTime > -1) r[idxTime] = normalizeTimeBTN_(r[idxTime]);

    // RRN: always kept as text, zero-padded to 12 chars
    if (idxRRN > -1) {
      let v = r[idxRRN] == null ? "" : String(r[idxRRN]).trim();
      if (v.length > 0 && v.length < 12) v = v.padStart(12, "0");
      r[idxRRN] = v;
    }
    if (idxMID  > -1 && r[idxMID]  != null) { const s = String(r[idxMID]).trim();  r[idxMID]  = /^\d+$/.test(s) ? Number(s) : s; }
    if (idxCust > -1 && r[idxCust] != null)  r[idxCust] = String(r[idxCust]).trim();
    if (idxMDR  > -1) { const s = String(r[idxMDR] == null ? "" : r[idxMDR]).trim(); r[idxMDR] = (s === "-" || s === "") ? "0" : s; }
    rows[i] = r;
  }
  return rows;
}

function normalizeDateBTN_(v) {
  if (Object.prototype.toString.call(v) === "[object Date]" && !isNaN(v))
    return Utilities.formatDate(v, TZ, "yyyy-MM-dd");
  if (v == null) return "";
  let s = String(v).trim().split(/[T ]/)[0];
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  let m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
  if (m) return `${m[1]}-${pad2(m[2])}-${pad2(m[3])}`;
  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (m) return `${m[3]}-${pad2(m[2])}-${pad2(m[1])}`;
  return s;
}

function normalizeTimeBTN_(v) {
  if (Object.prototype.toString.call(v) === "[object Date]" && !isNaN(v))
    return Utilities.formatDate(v, TZ, "HH:mm:ss");
  if (v == null) return "";
  let s = String(v).trim().replace(/\./g, ":");
  let parts = s.split(":").map(p => p.replace(/\D/g, "")).filter(p => p.length > 0);
  let h = "00", m = "00", sec = "00";
  if      (parts.length === 1) { const p = parts[0].padStart(6, "0"); h = p.substring(0,2); m = p.substring(2,4); sec = p.substring(4,6); }
  else if (parts.length === 2) { h = parts[0].padStart(2,"0"); const mm = parts[1].padStart(4,"0"); m = mm.substring(0,2); sec = mm.substring(2,4); }
  else if (parts.length >= 3) { h = parts[0].padStart(2,"0"); m = parts[1].padStart(2,"0"); sec = parts[2].padStart(2,"0"); }
  return `${pad2(Math.min(Math.max(parseInt(h)||0,0),23))}:${pad2(Math.min(Math.max(parseInt(m)||0,0),59))}:${pad2(Math.min(Math.max(parseInt(sec)||0,0),59))}`;
}

function getDateRangeBTN_(rows, idxDate) {
  let minDate = null, maxDate = null;
  for (let i = 1; i < rows.length; i++) {
    if (idxDate > -1 && rows[i][idxDate]) {
      const d = new Date(rows[i][idxDate]);
      if (!isNaN(d)) {
        if (!minDate || d < minDate) minDate = d;
        if (!maxDate || d > maxDate) maxDate = d;
      }
    }
  }
  return {
    start: minDate ? Utilities.formatDate(minDate, TZ, "yyyy-MM-dd") : "NA",
    end:   maxDate ? Utilities.formatDate(maxDate, TZ, "yyyy-MM-dd") : "NA"
  };
}


// =============================================================================
//  BCA (ISSUER) — Transaction Settlement Report
//  Input:  CSV or XLSX
//  Output: <original_name>_formatted.csv
// =============================================================================

function processBCA() {
  const src = DriveApp.getFolderById(SRC_FOLDER_BCA);
  const dst = DriveApp.getFolderById(DST_FOLDER_BCA);
  const it  = src.getFiles();
  while (it.hasNext()) {
    const file = it.next();
    const name = file.getName();
    try {
      let values;
      if      (isXlsxBCA_(file))                       values = readXlsxToValuesBCA_(file);
      else if (name.toLowerCase().endsWith(".csv"))     values = readCsvToValuesBCA_(file);
      else { L("Skip: " + name); continue; }

      if (!values || values.length === 0) { L("⚠ Empty: " + name); continue; }
      const formatted = formatDataBCA_(values);
      const csv       = toCsvWithSemicolonBCA_(formatted);
      const outName   = stripExtBCA_(name) + "_formatted.csv";
      dst.createFile(Utilities.newBlob(csv, "text/csv", outName));
      uploadCsvToGcs_(outName, csv, GCS_PATH_BCA);
      L(`✅ BCA: ${name} → ${outName}`);
    } catch (e) { L("❌ BCA " + name + ": " + e); }
  }
}

function isXlsxBCA_(file) {
  const n = file.getName().toLowerCase(), m = file.getMimeType();
  return n.endsWith(".xlsx") || m === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" || m === "application/vnd.ms-excel";
}

function readXlsxToValuesBCA_(file) {
  const copied = Drive.Files.copy({ title: "conv_" + file.getName() }, file.getId(), { mimeType: MimeType.GOOGLE_SHEETS });
  try {
    const sheets = SpreadsheetApp.openById(copied.id).getSheets();
    let best = sheets[0], bestScore = 0;
    sheets.forEach(sh => { const s = sh.getLastRow() * sh.getLastColumn(); if (s > bestScore) { bestScore = s; best = sh; } });
    return best.getDataRange().getValues();
  } finally { DriveApp.getFileById(copied.id).setTrashed(true); }
}

function readCsvToValuesBCA_(file) {
  const text      = file.getBlob().getDataAsString("UTF-8");
  const firstLine = (text.split(/\r?\n/)[0] || "");
  const delim     = (firstLine.match(/;/g)||[]).length > (firstLine.match(/,/g)||[]).length ? ";" : ",";
  return Utilities.parseCsv(text, delim);
}

function formatDataBCA_(values) {
  if (!values || values.length === 0) return values;
  values[0] = values[0].map(h => String(h).trim().replace(/\s+/g, "_"));
  const headers        = values[0];
  const paymentIdx     = headers.indexOf("PAYMENT_DATE");
  const partnerRefIdx  = headers.indexOf("PARTNER_REFERENCE_NO");

  if (paymentIdx > -1)
    for (let i = 1; i < values.length; i++) {
      const d = parseToDateBCA_(values[i][paymentIdx]);
      if (d) values[i][paymentIdx] = Utilities.formatDate(d, TZ, "yyyy/MM/dd HH:mm:ss");
    }

  if (partnerRefIdx > -1)
    for (let i = 1; i < values.length; i++) {
      const v = values[i][partnerRefIdx];
      if (v === "" || v == null) continue;
      const num = Number(String(v).replace(/,/g, ""));
      values[i][partnerRefIdx] = isNaN(num) ? "" : num;
    }
  return values;
}

function parseToDateBCA_(raw) {
  if (!raw) return null;
  if (Object.prototype.toString.call(raw) === "[object Date]" && !isNaN(raw)) return raw;
  const s = String(raw).trim();
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$/i);
  if (m) {
    let [_, mo, da, yr, hh, mm, ss, ap] = m;
    let year = yr.length === 2 ? (parseInt(yr) > 70 ? 1900 + parseInt(yr) : 2000 + parseInt(yr)) : parseInt(yr);
    let hour = parseInt(hh, 10);
    if (ap) { if (ap.toUpperCase()==="PM" && hour<12) hour+=12; if (ap.toUpperCase()==="AM" && hour===12) hour=0; }
    return new Date(year, mo-1, da, hour, mm, ss ? parseInt(ss,10) : 0);
  }
  m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (m) return new Date(m[1], m[2]-1, m[3], m[4], m[5], m[6]||0);
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

function toCsvWithSemicolonBCA_(rows) { return rows.map(r => r.map(csvEscapeBCA_).join(";")).join("\n"); }
function csvEscapeBCA_(v) { if (v==null) return ""; const s=String(v); return /[;"\n\r]/.test(s) ? '"'+s.replace(/"/g,'""')+'"' : s; }
function stripExtBCA_(name) { return name.replace(/\.[^/.]+$/, ""); }


// =============================================================================
//  BNI (ISSUER) — QRIS Transaction Report
//  Input:  XLSX
//  Output: BNI_Report_<date>.csv
// =============================================================================

function processBNI() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.BNI.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.BNI.DST;

  /**
   * Output column schema.
   * Columns not present in the source file will appear as empty strings.
   */
  const newHeader = [
    "No","Bill_Number","Aggregator_Name","Nama_Merchant","Merchant_PAN","MID","Reff_ID",
    "QR_Method","Tipe_QR","Tipe_Transaksi","Nama_Issuer","Customer_PAN","Nama_Customer",
    "Jenis_Pembayaran","Nama_Acquirer","Nominal","Amount_MDR","Net_Amount","TRX_Datetime",
    "Status","Settlement_Status","Source_Of_Fund","Additional_Data","Promo_Code","Switcher_Name"
  ];

  /** Maps output column name → source column name in the raw file. */
  const headerMap = {
    "No":"No","Bill_Number":"Bill Number","Aggregator_Name":"Aggregator Name",
    "Nama_Merchant":"Nama Merchant","Merchant_PAN":"Merchant PAN","MID":"MID","Reff_ID":"Reff ID",
    "QR_Method":"QR Method","Tipe_QR":"Tipe QR","Tipe_Transaksi":"Tipe Transaksi",
    "Nama_Issuer":"Nama Issuer","Customer_PAN":"Customer PAN","Nama_Customer":"Nama Customer",
    "Jenis_Pembayaran":"Jenis Pembayaran","Nama_Acquirer":"Nama Acquirer","Nominal":"Nominal",
    "Amount_MDR":"Amount MDR","Net_Amount":"Net Amount","TRX_Datetime":"TRX Datetime",
    "Status":"Status","Settlement_Status":"Settlement Status","Source_Of_Fund":"Source Of Fund",
    "Additional_Data":"Additional Data","Promo_Code":"Promo Code","Switcher_Name":"Switcher Name"
  };

  try {
    const sourceFolder = DriveApp.getFolderById(sourceFolderId);
    const files        = sourceFolder.getFiles();
    if (!files.hasNext()) { L("BNI: no files found"); return; }

    while (files.hasNext()) {
      const file = files.next();
      L("BNI processing: " + file.getName());

      const tempSS = SpreadsheetApp.create("Temp Import");
      const tempId = tempSS.getId();
      Drive.Files.update({ mimeType: MimeType.GOOGLE_SHEETS }, tempId, file.getBlob());
      const data         = SpreadsheetApp.openById(tempId).getSheets()[0].getDataRange().getValues();
      const excelHeaders = data[0];
      const targetIndices = newHeader.map(col => {
        const idx = excelHeaders.indexOf(headerMap[col]);
        if (idx === -1) L(`⚠ BNI: column '${headerMap[col]}' not found`);
        return idx;
      });

      const newData = [newHeader];
      for (let i = 1; i < data.length; i++) {
        newData.push(targetIndices.map(idx => (idx !== -1 && data[i][idx] !== undefined) ? data[i][idx] : ""));
      }

      // Build output filename from the first TRX_Datetime value
      let trxDate      = "0000-00-00";
      const dateColIdx = newHeader.indexOf("TRX_Datetime");
      if (newData.length > 1 && dateColIdx !== -1 && newData[1][dateColIdx]) {
        trxDate = Utilities.formatDate(new Date(newData[1][dateColIdx]), TZ, "yyyy-MM-dd");
      }

      const csvOutput  = newData.map(r => r.join(";")).join("\n");
      const newFileName = `BNI_Report_${trxDate}.csv`;
      DriveApp.getFolderById(destinationFolderId).createFile(newFileName, csvOutput, MimeType.CSV);
      uploadCsvToGcs_(newFileName, csvOutput, GCS_PATH_BNI);
      DriveApp.getFileById(tempId).setTrashed(true);
      L("✅ BNI: " + newFileName);
    }
  } catch (err) { L("❌ BNI: " + err); }
}


// =============================================================================
//  MANDIRI (ISSUER) — Merchant Settlement Report
//  Input:  CSV (metadata rows at top, semicolon-delimited)
//  Output: MSR_<merchantCode>_GID_<date>_v3.csv
// =============================================================================

function processMandiri() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.MANDIRI.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.MANDIRI.DST;

  const headerRow = [
    "NMID","MID","Merchant_Official","Trading_Name","Bank_Account","Bank_Account_Name",
    "TRXDATE","TRXTIME","Issuer_Name","TID","Refference_Number","Reff_ID/Invoice_No",
    "AMOUNT","MDR_Amount","NET_AMOUNT"
  ];

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    if (!file.getName().toLowerCase().endsWith(".csv")) continue;
    L("Mandiri processing: " + file.getName());

    let content    = file.getBlob().getDataAsString();
    const commaC   = (content.match(/,/g)||[]).length;
    const semiC    = (content.match(/;/g)||[]).length;
    const delimiter = semiC > commaC ? ";" : ",";

    function parseCSVLine(line, delim) {
      const regex = new RegExp(`(?!\\s*$)\\s*(?:'([^']*(?:''[^']*)*)'|"([^"]*(?:""[^"]*)*)"|([^'"${delim}]*))\\s*(?:${delim}|$)`, "g");
      const out = []; let m;
      while ((m = regex.exec(line))) out.push((m[1]||m[2]||m[3]||"").trim());
      return out;
    }

    // Skip the first 5 metadata/summary rows present in this issuer's format
    let rows = content.split(/\r?\n/).slice(5).filter(r => r.trim() !== "")
                      .map(r => parseCSVLine(r, delimiter));
    if (rows.length === 0) continue;

    const dateCol = headerRow.indexOf("TRXDATE");
    const timeCol = headerRow.indexOf("TRXTIME");

    rows = rows.map((r, i) => {
      if (i === 0) return r;
      // Normalize date
      const rawDate  = (r[dateCol] || "").trim();
      if (rawDate) {
        const parts = rawDate.split(/[\/\-]/);
        let d;
        if      (parts.length === 3 && parts[2].length === 4) d = new Date(parts[2], parts[1]-1, parts[0]);
        else if (parts.length === 3 && parts[0].length === 4) d = new Date(parts[0], parts[1]-1, parts[2]);
        r[dateCol] = (d && !isNaN(d.getTime())) ? Utilities.formatDate(d, TZ, "yyyy-MM-dd") : "";
      }
      // Normalize time
      const rawTime = (r[timeCol] || "").replace(/[^0-9]/g, "");
      if      (rawTime.length === 6) r[timeCol] = rawTime.replace(/(..)(..)(..)/, "$1:$2:$3");
      else if (rawTime.length === 4) r[timeCol] = rawTime.replace(/(..)(..)/, "$1:$2:00");
      return r;
    });

    rows[0] = headerRow;

    // Remove summary/total rows at the bottom
    const totalIdx = rows.findIndex(r => r[0] && r[0].toUpperCase().includes("TOTAL"));
    if (totalIdx > -1) rows = rows.slice(0, totalIdx);

    const trxDate = (rows[1] && rows[1][dateCol]) || Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd");

    const newContent = rows.map(r => r.map(c => {
      let val = (c || "").replace(/"/g, "").replace(/^'+|'+$/g, "").trim();
      if (/^\d{1,3}([.,]\d{3})+([.,]\d+)?$/.test(val)) val = val.replace(/[.,](?=\d{3}(\D|$))/g, "");
      return val;
    }).join(";")).join("\n");

    // NOTE: The merchant code segment in the filename ("40013") is an example placeholder.
    // Replace with your own merchant identifier or derive it from the file content.
    const newFileName = `MSR_MERCHANT_GID_${trxDate}_v3.csv`;
    destFolder.createFile(newFileName, newContent, MimeType.CSV);
    uploadCsvToGcs_(newFileName, newContent, GCS_PATH_MANDIRI);
    L("✅ Mandiri: " + newFileName);
  }
}


// =============================================================================
//  SHOPEEPAY — Transaction Settlement Report
//  Input:  CSV
//  Output: host_settlement_report_<date>.csv
// =============================================================================

function processShopeepay() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.SHOPEEPAY.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.SHOPEEPAY.DST;

  const outputHeaders = [
    "Merchant_Host","Partner_Merchant_ID","Merchant/Store_Name","Transaction_Type",
    "Merchant_Scope","Transaction_ID","Reference_ID","Parent_ID","External_Reference_ID",
    "Issuer_Identifier","Transaction_Amount","Fee_(MDR)","Settlement_Amount",
    "Terminal_ID","Create_Time","Update_Time","Adjustment_Reason","Entity_ID",
    "Fee_(Cofunding)","Reward_Amount","Reward_Type","Promo_Type","Payment_Method",
    "Currency_Code","Voucher_Promotion_Event_Name","Fee_(Withdrawal)","Fee_(Handling)"
  ];

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    if (!file.getName().toLowerCase().endsWith(".csv")) continue;

    let rows = Utilities.parseCsv(file.getBlob().getDataAsString());
    rows[0]  = outputHeaders;

    for (let i = 1; i < rows.length; i++) {
      if (rows[i][6])  rows[i][6]  = "'" + rows[i][6];    // Reference_ID — keep as text
      if (rows[i][8])  rows[i][8]  = "'" + rows[i][8];    // External_Reference_ID
      if (rows[i][13]) rows[i][13] = "'" + rows[i][13];   // Terminal_ID
      if (rows[i][14]) rows[i][14] = Utilities.formatDate(new Date(rows[i][14]), TZ, "yyyy-MM-dd HH:mm:ss");
      if (rows[i][15]) rows[i][15] = Utilities.formatDate(new Date(rows[i][15]), TZ, "yyyy-MM-dd HH:mm:ss");
      rows[i][25] = "";  // Fee_(Withdrawal) — not present in this version of the report
    }

    let dateStr = "0000-00-00";
    if (rows[1] && rows[1][14]) dateStr = String(rows[1][14]).substring(0, 10);
    const newFileName = `host_settlement_report_${dateStr}.csv`;

    let output = rows.map(r => r.join(";")).join("\n").replace(/"/g, "");
    destFolder.createFile(Utilities.newBlob(output, "text/csv", newFileName));
    uploadCsvToGcs_(newFileName, output, GCS_PATH_SHOPEEPAY);
    L(`✅ ShopeePay: ${file.getName()} → ${newFileName}`);
  }
}


// =============================================================================
//  INDODANA — Transaction Report
//  Input:  CSV or XLSX
//  Output: <original_name>_formatted.csv
// =============================================================================

function processIndodana() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.INDODANA.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.INDODANA.DST;

  const finalHeaders = [
    "NO","MERCHANT_NAME","TRANSACTION_DATE","TRANSIDMERCHANT","CUSTOMER_NAME",
    "AMOUNT","FEE","TAX","MERCHANT_SUPPORT","PAY_TO_MERCHANT","PAY_OUT_DATE",
    "TRANSACTION_TYPE","TENURE"
  ];

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    if (fileName.startsWith("~$")) continue;

    let data = [];

    if (fileName.toLowerCase().endsWith(".csv")) {
      let raw = file.getBlob().getDataAsString("UTF-8").replace(/"/g, "");
      data    = Utilities.parseCsv(raw, raw.split(";").length > raw.split(",").length ? ";" : ",");
    } else if (fileName.toLowerCase().endsWith(".xlsx")) {
      const tempFile = Drive.Files.insert({ title: fileName, mimeType: MimeType.GOOGLE_SHEETS }, file.getBlob());
      // getDisplayValues() prevents large integers being converted to scientific notation
      data = SpreadsheetApp.openById(tempFile.id).getSheets()[0].getDataRange().getDisplayValues();
      Drive.Files.remove(tempFile.id);
    } else { L("Skip: " + fileName); continue; }

    if (data.length === 0) { L("Empty: " + fileName); continue; }

    const headerRow = data[0].map(h => h.toString().replace(/"/g,"").replace(/\s+/g,"_").trim().toUpperCase());
    const headerMap = {};
    finalHeaders.forEach(h => { headerMap[h] = headerRow.indexOf(h); });

    const formattedData = [finalHeaders];
    for (let i = 1; i < data.length; i++) {
      const row = finalHeaders.map(h => {
        let val = headerMap[h] >= 0 ? data[i][headerMap[h]] : "";
        if (["TRANSACTION_DATE","PAY_OUT_DATE"].includes(h) && val) {
          const d = new Date(val.toString().replace(" +07:00",""));
          if (!isNaN(d.getTime())) val = Utilities.formatDate(d, TZ, "yyyy-MM-dd HH:mm:ss");
        }
        if (["AMOUNT","FEE","TAX","MERCHANT_SUPPORT","PAY_TO_MERCHANT"].includes(h))
          val = (val && !isNaN(val)) ? Number(val) : 0;
        if (h === "CUSTOMER_NAME" && typeof val === "string")
          val = val.split(";")[0].replace(/[\r\n]+/g," ").trim();
        return val;
      });
      formattedData.push(row);
    }

    const csvContent  = formattedData.map(r => r.join(";")).join("\n");
    const newFileName = fileName.replace(/\.[^/.]+$/, "") + "_formatted.csv";
    destFolder.createFile(newFileName, csvContent, MimeType.CSV);
    uploadCsvToGcs_(newFileName, csvContent, GCS_PATH_INDODANA);
    L("✅ Indodana: " + fileName + " → " + newFileName);
  }
}


// =============================================================================
//  KREDIVO — Transaction Report
//  Input:  XLSX
//  Output: <original_name>_formatted.csv
// =============================================================================

function processKredivo() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.KREDIVO.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.KREDIVO.DST;

  const finalHeaders = [
    "Name","Transaction_Date","User_ID","Settlement_Date","Cancellation_Date",
    "Order_ID","Transaction_ID","Amount","Type","Status","Source","Store_ID","Store_Name"
  ];

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    L("Kredivo: " + fileName);
    try {
      const gsFile = Drive.Files.insert(
        { title: fileName, mimeType: MimeType.GOOGLE_SHEETS, parents: [{ id: sourceFolderId }] },
        file.getBlob(), { convert: true }
      );
      const ss    = SpreadsheetApp.openById(gsFile.id);
      let data    = ss.getSheets()[0].getDataRange().getValues();

      if (data[0].length > 13) data.forEach(row => row.splice(13, 1));  // drop extra column if present
      for (let i = 1; i < data.length; i++)
        if (data[i][7] && !isNaN(data[i][7])) data[i][7] = Number(data[i][7]);
      data[0] = finalHeaders;
      data = data.map(row => row.map(c => typeof c === "string" ? c.replace(/"/g,"") : c));

      const csvContent  = data.map(r => r.join(";")).join("\n");
      const outFileName = fileName + "_formatted.csv";
      const out         = destFolder.createFile(outFileName, csvContent, MimeType.CSV);
      uploadCsvToGcs_(out.getName(), csvContent, GCS_PATH_KREDIVO);
      L("✅ Kredivo: " + out.getName());
      DriveApp.getFileById(gsFile.id).setTrashed(true);
    } catch (err) { L("❌ Kredivo " + fileName + ": " + err); }
  }
}


// =============================================================================
//  LINKAJA — Transaction Report
//  Input:  CSV or XLSX
//  Output: <original_name>.csv (removes checksum/sample rows)
// =============================================================================

function processLinkAja() {
  const sourceFolderId      = CONFIG.FOLDERS.ISSUER.LINKAJA.SRC;
  const destinationFolderId = CONFIG.FOLDERS.ISSUER.LINKAJA.DST;

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    const mimeType = file.getMimeType();
    try {
      if (mimeType === MimeType.GOOGLE_SHEETS) { L("Skip Sheets: " + fileName); continue; }

      let tempSS;
      if (mimeType === MimeType.MICROSOFT_EXCEL || fileName.toLowerCase().endsWith(".xlsx")) {
        tempSS = SpreadsheetApp.open(Drive.Files.copy({ title: "TEMP_" + fileName, mimeType: MimeType.GOOGLE_SHEETS }, file.getId()));
      } else if (fileName.toLowerCase().endsWith(".csv")) {
        const csvData = Utilities.parseCsv(file.getBlob().getDataAsString());
        tempSS = SpreadsheetApp.create("TEMP_" + fileName);
        tempSS.getActiveSheet().getRange(1, 1, csvData.length, csvData[0].length).setValues(csvData);
      } else { L("Skip: " + fileName); continue; }

      const data = tempSS.getSheets()[0].getDataRange().getValues();
      // Remove "SAMPLE CHECKSUM" footer rows that appear in some LinkAja exports
      let cutoff = data.length;
      for (let i = 0; i < data.length; i++)
        if (data[i].join(" ").toUpperCase().includes("SAMPLE CHECKSUM")) { cutoff = i; break; }

      const csv     = data.slice(0, cutoff).map(row => row.map(v => {
        if (typeof v === "string" && v.includes(",")) return `"${v.replace(/"/g,'""')}"`;
        return v;
      }).join(",")).join("\r\n");

      const outName = fileName.replace(/\.[^/.]+$/, "") + ".csv";
      destFolder.createFile(outName, csv, MimeType.CSV);
      uploadCsvToGcs_(outName, csv, GCS_PATH_LINKAJA);
      DriveApp.getFileById(tempSS.getId()).setTrashed(true);
      L("✅ LinkAja: " + fileName);
    } catch (err) { L("❌ LinkAja " + fileName + ": " + err); }
  }
}


// =============================================================================
//  OTTOPAY (EMAIL REPORT) — Transaction Report
//  Input:  CSV or XLSX
//  Output: Transaction_<ddMMyyyy>.csv
// =============================================================================

function processOttopayEmail() {
  const sourceFolderId = CONFIG.FOLDERS.ISSUER.OTTOPAY.SRC;
  const targetFolderId = CONFIG.FOLDERS.ISSUER.OTTOPAY.DST;

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const targetFolder = DriveApp.getFolderById(targetFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    const ext  = file.getName().split(".").pop().toLowerCase();

    if (ext === "csv") {
      const rawData = Utilities.parseCsv(file.getBlob().getDataAsString());
      const newCSV  = formatDataAsCSV_Otto_(rawData);
      const trxDate = getTrxDateFromData_Otto_(rawData);
      const newName = `Transaction_${trxDate}.csv`;
      targetFolder.createFile(newName, newCSV, MimeType.CSV);
      uploadCsvToGcs_(newName, newCSV, GCS_PATH_OTTOPAY);
      L("✅ OttoPay CSV: " + newName);
    } else if (ext === "xlsx") {
      const converted = Drive.Files.copy({ title: file.getName(), mimeType: MimeType.GOOGLE_SHEETS }, file.getId());
      const ss = SpreadsheetApp.openById(converted.id);
      try { ss.setSpreadsheetTimeZone(TZ); } catch (e) {}
      const data    = ss.getSheets()[0].getDataRange().getDisplayValues();
      const newCSV  = formatDataAsCSV_Otto_(data);
      const trxDate = getTrxDateFromData_Otto_(data);
      const newName = `Transaction_${trxDate}.csv`;
      targetFolder.createFile(newName, newCSV, MimeType.CSV);
      uploadCsvToGcs_(newName, newCSV, GCS_PATH_OTTOPAY);
      DriveApp.getFileById(converted.id).setTrashed(true);
      L("✅ OttoPay XLSX: " + newName);
    } else { L("⏭ Skip: " + file.getName()); }
  }
  L("🎯 OttoPay done.");
}

// ── OttoPay helpers ──────────────────────────────────────────────────────────

function getTrxDateFromData_Otto_(data) {
  if (!data || data.length < 2) return Utilities.formatDate(new Date(), TZ, "ddMMyyyy");
  let maxDate = null;
  for (let i = 1; i < data.length; i++) {
    const raw  = data[i][0]; // Column A = transaction_time
    if (!raw) continue;
    const norm = normalizeLocalDateTime_Otto_(raw);
    if (!norm || norm.indexOf(" ") === -1) continue;
    const [datePart, timePart] = norm.split(" ");
    const [y, m, d] = datePart.split("-").map(Number);
    const [hh, mi, ss] = (timePart || "00:00:00").split(":").map(Number);
    const dt = new Date(y, m-1, d, hh||0, mi||0, ss||0);
    if (!isNaN(dt.getTime()) && (!maxDate || dt > maxDate)) maxDate = dt;
  }
  return maxDate ? Utilities.formatDate(maxDate, TZ, "ddMMyyyy") : Utilities.formatDate(new Date(), TZ, "ddMMyyyy");
}

function formatDataAsCSV_Otto_(data) {
  if (!data || data.length === 0) return "";
  data[0] = data[0].map(h => String(h||"").trim().replace(/\s+/g,"_"));
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (row[0])  row[0]  = normalizeLocalDateTime_Otto_(row[0]);
    if (row[22]) row[22] = normalizeDateOnly_Otto_(row[22]);
    if (row[23]) row[23] = normalizeTimeOnly_Otto_(row[23]);
    if (row[38]) row[38] = normalizeDateOnly_Otto_(row[38]);
  }
  return data.map(escapeCsvRowSemicolon_Otto_).join("\n");
}

function normalizeLocalDateTime_Otto_(v) {
  if (v == null) return "";
  const s = String(v).trim();
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]||"00"}`;
  m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/);
  if (m) { const [,a,b,yyyy,hh,mi,ss="00"]=m; return `${yyyy}-${pad2(a)}-${pad2(b)} ${pad2(hh)}:${pad2(mi)}:${pad2(ss)}`; }
  m = s.match(/^(\d{4})[\/](\d{2})[\/](\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) { const [,yyyy,mm,dd,hh,mi,ss="00"]=m; return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`; }
  if (/^\d+(\.\d+)?$/.test(s) && Number(s) > 59) {
    const serial = Number(s), days = Math.floor(serial), secs = Math.round((serial-days)*86400);
    return `${ymdToString_Otto_(addDays_Otto_("1899-12-30", days))} ${secondsToHHMMSS_Otto_(secs)}`;
  }
  return s.replace(/\s+/g," ");
}

function normalizeDateOnly_Otto_(v) {
  if (v == null) return "";
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  let m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
  if (m) { const a1=Number(m[1]),a2=Number(m[2]),y=Number(m[3]); return a1>12 ? `${y}-${pad2(a2)}-${pad2(a1)}` : `${y}-${pad2(a1)}-${pad2(a2)}`; }
  m = s.match(/^(\d{4})[\/](\d{2})[\/](\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  if (/\s+/.test(s)) { const dt=normalizeLocalDateTime_Otto_(s); if (dt.indexOf(" ")>-1) return dt.split(" ")[0]; }
  return s;
}

function normalizeTimeOnly_Otto_(v) {
  if (v == null) return "";
  const s = String(v).trim();
  let m = s.match(/^(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) return `${m[1]}:${m[2]}:${m[3]||"00"}`;
  m = s.match(/^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/);
  if (m) return `${pad2(m[1])}:${pad2(m[2])}:${pad2(m[3]||"00")}`;
  if (/\s+/.test(s)) { const dt=normalizeLocalDateTime_Otto_(s); if (dt.indexOf(" ")>-1) return dt.split(" ")[1]; }
  return s;
}

function escapeCsvRowSemicolon_Otto_(row) {
  return row.map(v => { const s=v==null?"":String(v); return (s.includes(";")||s.includes('"')||s.includes("\n")) ? '"'+s.replace(/"/g,'""')+'"' : s; }).join(";");
}

function addDays_Otto_(yyyy_mm_dd, days) {
  const [y,m,d]=yyyy_mm_dd.split("-").map(Number), dt=new Date(Date.UTC(y,m-1,d));
  dt.setUTCDate(dt.getUTCDate()+days);
  return `${dt.getUTCFullYear()}-${pad2(dt.getUTCMonth()+1)}-${pad2(dt.getUTCDate())}`;
}
function secondsToHHMMSS_Otto_(s) { s=Math.max(0,Math.min(86399,s)); return `${pad2(Math.floor(s/3600))}:${pad2(Math.floor((s%3600)/60))}:${pad2(s%60)}`; }
function ymdToString_Otto_(s) { return s; }


// =============================================================================
//  OTTOPAY DASHBOARD — Dashboard Export Report
//  Input:  CSV or XLSX
//  Output: Dashboard_Ottopay_<ddMMyyyy>.csv  (or date range)
// =============================================================================

const FINAL_HEADERS_OTTOPAY = [
  "Order_ID","Invoice_Number","Payment_Method","Payment_Type","Transaction_Date",
  "Store_Name","Status_Transaction","Status_Invoice","Gross_Amount","Customer_Name",
  "Customer_Email","Customer_Phone","Reference_Number","Expired_Date",
  "Refund_ID","Refund_RRN","Refund_Amount"
];

function processOttopayDashboard() {
  const sourceFolder = DriveApp.getFolderById(SOURCE_FOLDER_ID_OTTOPAY);
  const destFolder   = DriveApp.getFolderById(DEST_FOLDER_ID_OTTOPAY);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    const name = file.getName();
    const ext  = (name.split(".").pop() || "").toLowerCase();
    L("OttoPay Dashboard: " + name);
    try {
      let raw = [];
      if (ext === "xlsx" || ext === "xls") {
        const tempId = SpreadsheetApp.create("TempImport").getId();
        Drive.Files.update({ mimeType: MimeType.GOOGLE_SHEETS }, tempId, file.getBlob());
        raw = SpreadsheetApp.openById(tempId).getSheets()[0].getDataRange().getValues();
        DriveApp.getFileById(tempId).setTrashed(true);
      } else if (ext === "csv") {
        raw = Utilities.parseCsv(file.getBlob().getDataAsString());
      } else { L("⏭ Skip: " + name); continue; }

      const { earliest, latest } = getDateRangeFromRaw_(raw);
      const formatted = formatOttopayData_(raw);
      const asc  = Utilities.formatDate(earliest, TZ, "ddMMyyyy");
      const desc = Utilities.formatDate(latest,   TZ, "ddMMyyyy");
      const outName = asc === desc ? `Dashboard_Ottopay_${asc}.csv` : `Dashboard_Ottopay_${asc}-${desc}.csv`;

      const csv = formatted.map(r => r.join(";")).join("\n");
      destFolder.createFile(outName, csv, MimeType.CSV);
      uploadCsvToGcs_(outName, csv, GCS_PATH_OTTOPAY_DASHBOARD);
      L("✅ OttoPay Dashboard: " + outName);
    } catch (e) { L("❌ OttoPay Dashboard: " + e); }
  }
}

function formatOttopayData_(data) {
  if (!data || data.length <= 1) return [FINAL_HEADERS_OTTOPAY];
  const headerInput      = data[0].map(h => (h??"").toString().trim().toLowerCase());
  const idxStatusInvoice = headerInput.indexOf("status_invoice");
  const out              = [FINAL_HEADERS_OTTOPAY];

  for (let i = 1; i < data.length; i++) {
    const safeRow = [...data[i]];
    // If status_invoice column is missing, insert an empty cell to prevent column shift
    if (idxStatusInvoice === -1) safeRow.splice(7, 0, "");
    const newRow = FINAL_HEADERS_OTTOPAY.map((h, j) => {
      let v = safeRow[j];
      const hl = h.toLowerCase();
      if (hl === "transaction_date" || hl === "expired_date") {
        const d = parseIdDate_(v); v = d ? Utilities.formatDate(d, TZ, "yyyy-MM-dd HH:mm:ss") : "";
      } else if (hl === "gross_amount" || hl === "refund_amount") {
        v = Number((v??"").toString().replace(/[^0-9.-]/g,"")) || 0;
      } else if (hl === "reference_number") {
        v = (v??"").toString();
      }
      return v ?? "";
    });
    out.push(newRow);
  }
  return out;
}

function getDateRangeFromRaw_(raw) {
  if (!raw || raw.length <= 1) { const now=new Date(); return {earliest:now,latest:now}; }
  const idx = raw[0].map(h=>(h?.toString?.()??).toLowerCase()).findIndex(h=>h.includes("transaction"));
  if (idx === -1) { const now=new Date(); return {earliest:now,latest:now}; }
  const ts = [];
  for (let i=1;i<raw.length;i++) { const d=parseIdDate_(raw[i][idx]); if(d) ts.push(d.getTime()); }
  if (ts.length===0) { const now=new Date(); return {earliest:now,latest:now}; }
  return { earliest: new Date(Math.min(...ts)), latest: new Date(Math.max(...ts)) };
}

function parseIdDate_(v) {
  if (v instanceof Date && !isNaN(v)) return v;
  if (typeof v==="number" && isFinite(v)) return new Date(Math.round((v-25569)*86400*1000));
  if (typeof v!=="string") return null;
  const s = v.trim(); if (!s) return null;
  let m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s+(\d{1,2})[.:](\d{1,2})(?:[.:](\d{1,2}))?$/);
  if (m) { const y=normalizeYear_(m[3]); return new Date(y,m[2]-1,m[1],m[4],m[5],m[6]?parseInt(m[6]):0); }
  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
  if (m) { const y=normalizeYear_(m[3]); return new Date(y,m[2]-1,m[1]); }
  const fb = new Date(s); return isNaN(fb) ? null : fb;
}

function normalizeYear_(y) { const n=parseInt(y,10); return n<100 ? (n>=70?1900+n:2000+n) : n; }


// =============================================================================
//  NOBU — QRIS Merchant Transaction Report
//  Input:  XLSX or CSV
//  Output: recon_issuer_nobu_report_<date>.csv
// =============================================================================

function processNobu() {
  const sourceFolder = DriveApp.getFolderById(SOURCE_FOLDER_ID_NOBU);
  const targetFolder = DriveApp.getFolderById(DEST_FOLDER_ID_NOBU);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    const mimeType = file.getMimeType();
    if (mimeType !== MimeType.MICROSOFT_EXCEL && mimeType !== MimeType.CSV && !fileName.endsWith(".xlsx")) {
      L("⏭ Skip: " + fileName); continue;
    }
    try {
      const tempFile = Drive.Files.insert(
        { title: "temp_" + fileName, mimeType: MimeType.GOOGLE_SHEETS }, file.getBlob()
      );
      const result = processNobuData_(SpreadsheetApp.openById(tempFile.id));
      if (result) {
        targetFolder.createFile(result.fileName, result.csv, MimeType.CSV);
        uploadCsvToGcs_(result.fileName, result.csv, GCS_PATH_NOBU);
        L("✅ Nobu: " + result.fileName);
      }
      DriveApp.getFileById(tempFile.id).setTrashed(true);
    } catch (e) { L("❌ Nobu " + fileName + ": " + e); }
  }
}

function processNobuData_(ss) {
  const sheet   = ss.getSheets()[0];
  const lastRow = sheet.getLastRow();
  if (lastRow < 1) return null;

  sheet.deleteColumn(1);  // Remove the "NO" index column

  const newHeader = [
    "transaction_date","transaction_time","invoice_number","ref_no","approval_code",
    "mpan","store_name","nmid","tid","qris_type","transaction_nominal","fee_amount",
    "mdr","net_amount","issuer","status","merchant_transaction_number","ext_merchant_transaction_number"
  ];
  sheet.getRange(1, 1, 1, newHeader.length).setValues([newHeader]);
  if (lastRow < 2) return null;

  const dataRows = lastRow - 1;

  // Force plain text for reference/code columns to prevent numeric conversion
  sheet.getRange(2, 3, dataRows, 8).setNumberFormat("@");
  sheet.getRange(2, 15, dataRows, 4).setNumberFormat("@");

  // Approval code (col E): zero-pad to minimum 6 characters
  const approvalRng = sheet.getRange(2, 5, dataRows, 1);
  approvalRng.setValues(approvalRng.getValues().map(row => {
    let v = (row[0]??"").toString().trim();
    return [v.length > 0 && v.length < 6 ? v.padStart(6,"0") : v];
  }));

  // MDR (col M): normalize percentage or decimal representation
  const feeRng = sheet.getRange(2, 13, dataRows, 1);
  feeRng.setNumberFormat("@");
  feeRng.setValues(feeRng.getDisplayValues().map(row => {
    const v = row[0].toString().trim();
    if (v === "") return [""];
    if (v.includes("%")) { const n=parseFloat(v.replace("%","").replace(",",".")); return [!isNaN(n) ? parseFloat((n/100).toFixed(10)).toString() : v]; }
    const n = parseFloat(v.replace(",",".")); return [!isNaN(n) ? parseFloat(n.toFixed(10)).toString() : v];
  }));

  // Transaction date (col A): normalize to yyyy-MM-dd, track max date for filename
  const dateRng = sheet.getRange(2, 1, dataRows, 1);
  let maxTime   = 0;
  dateRng.setNumberFormat("@");
  dateRng.setValues(dateRng.getDisplayValues().map(row => {
    const d = row[0].trim();
    let dateObj;
    const parts = d.split(/[ /-]/);
    if (parts.length >= 3) {
      if      (parts[2].length === 4) dateObj = new Date(parts[2], parts[1]-1, parts[0]);
      else if (parts[0].length === 4) dateObj = new Date(parts[0], parts[1]-1, parts[2]);
    }
    if (!dateObj || isNaN(dateObj.getTime())) dateObj = new Date(d);
    if (!isNaN(dateObj.getTime())) {
      if (dateObj.getTime() > maxTime) maxTime = dateObj.getTime();
      return [Utilities.formatDate(dateObj, TZ, "yyyy-MM-dd")];
    }
    return [d];
  }));

  sheet.getRange(2, 2, dataRows, 1).setNumberFormat("HH:mm:ss");

  const maxDateStr  = maxTime > 0 ? Utilities.formatDate(new Date(maxTime), TZ, "yyyy-MM-dd") : "unknown";
  const finalData   = sheet.getDataRange().getDisplayValues();
  let csvContent    = "";
  for (let i = 0; i < finalData.length; i++) csvContent += finalData[i].join(";") + "\r\n";
  return { fileName: `recon_issuer_nobu_report_${maxDateStr}.csv`, csv: csvContent };
}


// =============================================================================
//  BRI — E-Commerce Transaction Report
//  Input:  CSV or XLSX
//  Output: recon_issuer_bank_bri_report_<yyyyMMdd>.csv
// =============================================================================

function processBri() {
  const sourceFolder = DriveApp.getFolderById(SOURCE_FOLDER_ID_BRI);
  const targetFolder = DriveApp.getFolderById(DEST_FOLDER_ID_BRI);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    const mimeType = file.getMimeType();
    if (!([MimeType.MICROSOFT_EXCEL, MimeType.CSV].includes(mimeType) || fileName.endsWith(".xlsx") || fileName.endsWith(".csv"))) {
      L("⏭ Skip: " + fileName); continue;
    }
    try {
      const tempFile = Drive.Files.insert({ title: "temp_" + fileName, mimeType: MimeType.GOOGLE_SHEETS }, file.getBlob());
      const result   = processBriData_(SpreadsheetApp.openById(tempFile.id));
      if (result) {
        targetFolder.createFile(result.fileName, result.csv, MimeType.CSV);
        uploadCsvToGcs_(result.fileName, result.csv, GCS_PATH_BRI);
        L("✅ BRI: " + result.fileName);
      }
      DriveApp.getFileById(tempFile.id).setTrashed(true);
    } catch (e) { L("❌ BRI " + fileName + ": " + e); }
  }
}

/**
 * Core BRI formatter.
 * Aligns data to a 22-column BigQuery DDL schema, handling:
 *  - Auto-insertion of JAM_TRX and TGL_RK columns if absent
 *  - Multiple date formats for TGL_TRX and TGL_SETL
 *  - Fractional-day time values from Google Sheets
 *  - String columns protected from scientific notation
 *  - Integer casting for amount fields
 */
function processBriData_(ss) {
  const sheet   = ss.getSheets()[0];
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  // ── Date parser: handles yyyy-MM-dd, yyyyMMdd, dd/MM/yyyy, dd/MM/yy, Excel serial ──
  function parseToYMD(raw) {
    if (!raw && raw !== 0) return "";
    if (raw instanceof Date) return !isNaN(raw.getTime()) ? Utilities.formatDate(raw, TZ, "yyyy-MM-dd") : "";
    const d = raw.toString().trim(); if (!d) return "";
    if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
    if (/^\d{8}$/.test(d))     return `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`;
    if (/^\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{4}$/.test(d)) { const p=d.split(/[\/\-\.]/); return `${p[2]}-${p[1].padStart(2,"0")}-${p[0].padStart(2,"0")}`; }
    if (/^\d{4}[\/\.]\d{1,2}[\/\.]\d{1,2}$/.test(d)) { const p=d.split(/[\/\.]/); return `${p[0]}-${p[1].padStart(2,"0")}-${p[2].padStart(2,"0")}`; }
    if (/^\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2}$/.test(d)) { const p=d.split(/[\/\-\.]/); return `${2000+parseInt(p[2],10)}-${p[1].padStart(2,"0")}-${p[0].padStart(2,"0")}`; }
    if (/^\d{5}$/.test(d)) { const dt=new Date(new Date(1899,11,30).getTime()+parseInt(d,10)*86400000); return !isNaN(dt.getTime()) ? Utilities.formatDate(dt, TZ, "yyyy-MM-dd") : d; }
    const fb = new Date(d); return !isNaN(fb.getTime()) ? Utilities.formatDate(fb, TZ, "yyyy-MM-dd") : d;
  }

  // ── Time parser: handles fraction-of-day, HH:mm:ss, HH.mm.ss, HHMMSS ──
  function parseToHMS(raw) {
    if (!raw && raw !== 0) return "";
    if (typeof raw === "number") { const t=Math.round(raw*86400); return `${String(Math.floor(t/3600)).padStart(2,"0")}:${String(Math.floor((t%3600)/60)).padStart(2,"0")}:${String(t%60).padStart(2,"0")}`; }
    if (raw instanceof Date) return Utilities.formatDate(raw, TZ, "HH:mm:ss");
    const s = raw.toString().trim();
    if (/^\d{1,2}:\d{2}:\d{2}$/.test(s)) { const p=s.split(":"); return p[0].padStart(2,"0")+":"+p[1]+":"+p[2]; }
    if (/^\d{1,2}\.\d{2}\.\d{2}$/.test(s)) { const p=s.split("."); return p[0].padStart(2,"0")+":"+p[1]+":"+p[2]; }
    if (/^\d{6}$/.test(s)) return `${s.slice(0,2)}:${s.slice(2,4)}:${s.slice(4,6)}`;
    if (/^\d{5}$/.test(s)) return `0${s[0]}:${s.slice(1,3)}:${s.slice(3,5)}`;
    return s;
  }

  function safeString(v) { if (!v && v!==0) return ""; return typeof v==="number" ? v.toFixed(0).toString().trim() : v.toString().trim(); }

  let header = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0]
                    .map(h => h.toString().trim().toUpperCase().replace(/[﻿"]/g,""));

  // Handle CSV imported as a single column (delimiter not auto-detected)
  if (header.length === 1 && header[0].indexOf(",") > -1) {
    const allData  = sheet.getDataRange().getValues();
    const splitData = allData.map(row => row[0].toString().split(",").map(c => c.replace(/^"|"$/g,"").trim()));
    sheet.clearContents();
    sheet.getRange(1,1,splitData.length,splitData[0].length).setValues(splitData);
    header = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0].map(h => h.toString().trim().toUpperCase().replace(/[﻿"]/g,""));
  }

  const hasJamTrx = header.indexOf("JAM_TRX") > -1;
  const hasTglRk  = header.indexOf("TGL_RK")  > -1;
  if (!hasJamTrx) { sheet.insertColumnBefore(6); sheet.getRange(1,6).setValue("JAM_TRX"); sheet.getRange(2,6,lastRow-1,1).setValues(Array(lastRow-1).fill([""])); }
  if (!hasTglRk)  { const c=sheet.getLastColumn(); if(c<22) sheet.insertColumnAfter(c); sheet.getRange(1,22).setValue("TGL_RK"); sheet.getRange(2,22,lastRow-1,1).setValues(Array(lastRow-1).fill([""])); }

  const newHeader = ["no","mid","tid","nama_merchant","tgl_trx","jam_trx","tgl_setl","card_number",
    "remark_rk","amt_setl","amt_trx","rate","disc_amt","net_amt","tipe","principle","issuer",
    "aprv_code_reff_num","partner_reff_no","batch_num","amtnonfare","tgl_rk"];
  sheet.getRange(1,1,1,newHeader.length).setValues([newHeader]);

  const dataRows = lastRow - 1;

  // JAM_TRX (col 6) — normalize to HH:MM:SS
  const jamRng = sheet.getRange(2,6,dataRows,1); jamRng.setNumberFormat("@");
  jamRng.setValues(jamRng.getValues().map(r => [parseToHMS(r[0])]));

  // Text columns — prevent scientific notation
  [8,18,19,20].forEach(col => {
    const rng = sheet.getRange(2,col,dataRows,1); rng.setNumberFormat("@");
    rng.setValues(rng.getDisplayValues().map(r => { let v=safeString(r[0]); if(col===18&&v.length>0&&v.length<6) v=v.padStart(6,"0"); return [v]; }));
  });

  // TGL_TRX (col 5) — normalize to yyyy-MM-dd
  const trxRng = sheet.getRange(2,5,dataRows,1); trxRng.setNumberFormat("@");
  trxRng.setValues(trxRng.getValues().map(r => [parseToYMD(r[0])]));

  // TGL_SETL (col 7) — normalize, track max date for filename
  const setlRng = sheet.getRange(2,7,dataRows,1);
  let maxTime   = 0;
  setlRng.setNumberFormat("@");
  setlRng.setValues(setlRng.getValues().map(r => { const result=parseToYMD(r[0]); if(result&&result.length===10){const t=new Date(result).getTime(); if(!isNaN(t)&&t>maxTime) maxTime=t;} return [result]; }));

  // TGL_RK (col 22) — normalize to yyyy-MM-dd
  const rkRng = sheet.getRange(2,22,dataRows,1); rkRng.setNumberFormat("@");
  rkRng.setValues(rkRng.getValues().map(r => [parseToYMD(r[0])]));

  // Integer columns (amounts, IDs)
  [1,2,3,10,11,12,13,14,21].forEach(col => {
    const rng = sheet.getRange(2,col,dataRows,1);
    rng.setValues(rng.getValues().map(r => { const v=r[0]; if(!v&&v!==0) return [0]; const n=parseInt(v.toString().replace(/[.,\s]/g,""),10); return [isNaN(n)?0:n]; }));
  });

  const CRLF     = "\r\n";
  const data     = sheet.getDataRange().getDisplayValues();
  let csvContent = "";
  for (let i=0; i<data.length; i++) csvContent += data[i].join(";") + CRLF;

  const maxDateStr = maxTime > 0 ? Utilities.formatDate(new Date(maxTime), TZ, "yyyyMMdd") : "unknown";
  return { fileName: `recon_issuer_bank_bri_report_${maxDateStr}.csv`, csv: csvContent };
}


// =============================================================================
//  LOGGING HELPERS
// =============================================================================

/** Appends a timestamped line to the in-memory log buffer (Script Properties). */
function L(msg) {
  const ts   = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd HH:mm:ss");
  const line = `${ts} Info ${String(msg)}`;
  Logger.log(String(msg));
  _logAppend_(line);
}

function _logReset_() { PropertiesService.getScriptProperties().deleteProperty("LAST_RUN_LOG"); }

function _logAppend_(line) {
  const props  = PropertiesService.getScriptProperties();
  const maxLen = 80000;
  let buf      = (props.getProperty("LAST_RUN_LOG") || "") + String(line) + "\n";
  if (buf.length > maxLen) buf = buf.substring(buf.length - maxLen);
  props.setProperty("LAST_RUN_LOG", buf);
}


// =============================================================================
//  MASTER RUNNER
// =============================================================================

/** Runs all issuer formatters in sequence. Errors in one issuer do not stop others. */
function runAllIssuers() {
  _logReset_();
  const issuers = [
    ["BCA",               processBCA],
    ["BTN",               processBTN],
    ["BNI",               processBNI],
    ["Mandiri",           processMandiri],
    ["ShopeePay",         processShopeepay],
    ["Indodana",          processIndodana],
    ["Kredivo",           processKredivo],
    ["LinkAja",           processLinkAja],
    ["OttoPay",           processOttopayEmail],
    ["OttoPay Dashboard", processOttopayDashboard],
    ["Nobu",              processNobu],
    ["BRI",               processBri],
  ];
  issuers.forEach(([name, fn]) => {
    L(`🏦 Start ${name}`);
    try { fn(); } catch (e) { L(`❌ Error ${name}: ${e}`); }
  });
  L("✅ All done");

  // Send completion email (optional)
  const ts      = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd HH:mm:ss");
  const subject = `✅ Recon Formatter Done (${ts})`;
  const body    = `<p>All issuer formatters completed at <b>${ts}</b> (WIB).</p>
                   <p>Check GCS bucket <b>${GCS_BUCKET}</b> for output files.</p>`;
  sendCompletionEmail_(subject, body);
}

function sendCompletionEmail_(subject, body) {
  try { GmailApp.sendEmail(CONFIG.NOTIFICATION.EMAIL, subject, "", { htmlBody: body }); }
  catch (e) { L("⚠ Email not sent: " + e); }
}


// =============================================================================
//  SCHEDULER — Weekday triggers (Mon–Fri)
// =============================================================================

/** Removes existing runAllIssuers triggers and creates fresh weekday triggers. */
function createWeekdayTriggers() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === "runAllIssuers")
    .forEach(t => ScriptApp.deleteTrigger(t));

  [ScriptApp.WeekDay.MONDAY, ScriptApp.WeekDay.TUESDAY, ScriptApp.WeekDay.WEDNESDAY,
   ScriptApp.WeekDay.THURSDAY, ScriptApp.WeekDay.FRIDAY].forEach(day => {
    ScriptApp.newTrigger("runAllIssuers").timeBased().onWeekDay(day).atHour(9).nearMinute(50).create();
  });
  L("✅ Weekday triggers created (Mon–Fri ~08:50 WIB)");
}
