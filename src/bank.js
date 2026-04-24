/**
 * bank.js
 * =======
 * Automated formatter for bank mutation / account statement files.
 * Reads raw CSV/XLSX files from Google Drive, normalizes each bank's
 * column schema and date/time format, and uploads standardized
 * semicolon-delimited CSV files to GCS.
 *
 * Supported banks: BCA, BTN, BNI, Mandiri
 *
 * SETUP: Copy config.example.js → config.js and fill in your folder IDs,
 *        GCS bucket name, and email. Store your Service Account JSON in
 *        Script Properties under the key "GCS_KEY".
 */

// ─── Pull settings from CONFIG (defined in config.js) ────────────────────────
const GCS_BUCKET = CONFIG.GCS.BUCKET;
const GCS_SCOPE  = CONFIG.GCS.SCOPE;


// =============================================================================
//  SHARED GCS UPLOAD HELPER
// =============================================================================

/**
 * Generates a short-lived Bearer token from the Service Account key
 * stored in Script Properties, then uploads a CSV to GCS.
 *
 * @param {string} path     - GCS path prefix (no trailing slash)
 * @param {string} filename - Object name (filename only)
 * @param {string} content  - CSV string content
 */
function uploadCsvToGcs_(path, filename, content) {
  const raw = PropertiesService.getScriptProperties().getProperty("GCS_KEY");
  if (!raw) throw new Error("GCS_KEY not found in Script Properties");
  const key = JSON.parse(raw);

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
  const signature    = Utilities.computeRsaSha256Signature(`${encHeader}.${encClaim}`, key.private_key);
  const jwt          = `${encHeader}.${encClaim}.${Utilities.base64EncodeWebSafe(signature)}`;

  const tokenRes = UrlFetchApp.fetch("https://oauth2.googleapis.com/token", {
    method: "post",
    payload: { grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: jwt }
  });
  const token = JSON.parse(tokenRes.getContentText()).access_token;

  const url = `https://storage.googleapis.com/upload/storage/v1/b/${GCS_BUCKET}/o`
            + `?uploadType=media&name=${path}/${filename}`;
  const res = UrlFetchApp.fetch(url, {
    method:      "post",
    contentType: "text/csv",
    payload:     content,
    headers:     { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });
  Logger.log(`📤 Upload ${filename}: ${res.getResponseCode()}`);
}


// =============================================================================
//  BCA BANK — Account Statement
//  Input:  CSV  (pattern: filename contains "BCA_BankFile")
//  Output: <original_name>.csv
//  Schema: Transaction_Date | Description | Branch | Transaction_Amount | Type_Trx | Total_Amount
// =============================================================================

function prosessBankBCA() {
  const sourceFolderId      = CONFIG.FOLDERS.BANK.BCA.SRC;
  const destinationFolderId = CONFIG.FOLDERS.BANK.BCA.DST;
  const gcsPath             = CONFIG.GCS.PATHS.BCA_BANK;

  /**
   * Filename filter: adjust this pattern to match your bank statement files.
   * Example: "BCA_BankFile", "Acc_Statement_BCA", etc.
   */
  const FILE_NAME_PATTERN = "BCA_BankFile";

  /**
   * Header detection: the parser searches for this exact header row string
   * to skip any account summary rows at the top of the file.
   */
  const HEADER_SIGNATURE = "Tanggal Transaksi,Keterangan,Cabang,Jumlah,Saldo";

  /**
   * Footer detection: rows starting with these strings are treated as
   * end-of-data markers (account summary footer).
   */
  const FOOTER_MARKERS = ["Saldo Awal", "Mutasi Debet", "Mutasi Kredit", "Saldo Akhir"];

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file = files.next();
    if (!file.getName().includes(FILE_NAME_PATTERN)) continue;

    try {
      const rawData = Utilities.parseCsv(file.getBlob().getDataAsString());

      // Locate header row (skip bank account summary rows at top)
      let headerRowIndex = -1;
      for (let i = 0; i < rawData.length; i++) {
        if (rawData[i].join(",").includes(HEADER_SIGNATURE)) { headerRowIndex = i; break; }
      }
      if (headerRowIndex === -1) { Logger.log("⚠ Header not found: " + file.getName()); continue; }

      let cleanData = rawData.slice(headerRowIndex + 1);

      // Remove footer summary rows
      let footerStart = -1;
      for (let i = 0; i < cleanData.length; i++) {
        if (FOOTER_MARKERS.some(m => cleanData[i][0].toString().trim().includes(m))) { footerStart = i; break; }
      }
      if (footerStart !== -1) cleanData = cleanData.slice(0, footerStart);

      const processedData = [["Transaction_Date","Description","Branch","Transaction_Amount","Type_Trx","Total_Amount"]];
      cleanData.forEach(row => {
        if (row.length < 5) return;

        // Normalize date: dd/MM/yyyy or dd-MM-yyyy → yyyy-MM-dd
        const parts = row[0].split(/[\/-]/);
        let formattedDate = row[0];
        if (parts.length === 3) {
          const d = new Date(parts[2], parts[1]-1, parts[0]);
          if (!isNaN(d)) formattedDate = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`;
        }

        // BCA format: "Amount CR" or "Amount DB" in one cell — split into amount + type
        const amountParts = row[3].toString().trim().split(" ");
        processedData.push([formattedDate, row[1], row[2], amountParts[0], amountParts[1]||"", row[4]]);
      });

      const csvContent  = processedData.map(e => e.join(";")).join("\n");
      const newFileName = file.getName().replace(/\.[^/.]+$/, "") + ".csv";
      destFolder.createFile(newFileName, csvContent, MimeType.CSV);
      uploadCsvToGcs_(gcsPath, newFileName, csvContent);
      Logger.log("✅ BCA Bank: " + newFileName);
    } catch (e) { Logger.log("❌ BCA Bank: " + e); }
  }
}


// =============================================================================
//  BTN BANK — Single Transaction Inquiry Export
//  Input:  CSV  (pattern: filename contains "SingleTransactionInquiry")
//  Output: <original_name>.csv
//  Schema: No. | Post_Date | Post_Time | Eff_Date | Eff_Time | Description |
//          Amount_Debit | Amount_Credit | Balance | Reference_No
// =============================================================================

function processBankBTN() {
  const sourceFolderId      = CONFIG.FOLDERS.BANK.BTN.SRC;
  const destinationFolderId = CONFIG.FOLDERS.BANK.BTN.DST;
  const gcsPath             = CONFIG.GCS.PATHS.BTN_BANK;

  /**
   * Filename filter: adjust this pattern to match your BTN bank files.
   * "_temp" suffix is excluded to avoid picking up intermediate files.
   */
  const FILE_NAME_PATTERN = "SingleTransactionInquiry";

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.getFiles();

  while (files.hasNext()) {
    const file     = files.next();
    const fileName = file.getName();
    if (!fileName.includes(FILE_NAME_PATTERN) || fileName.includes("_temp")) continue;

    const processed = parseAndFormatBTNBankFile_(file.getBlob().getDataAsString());
    if (processed.length === 0) continue;

    const csvContent  = processed.map(r => r.join(";")).join("\n");
    const csvFileName = fileName.replace(/-/g, "_") + ".csv";
    destFolder.createFile(csvFileName, csvContent, MimeType.CSV);
    uploadCsvToGcs_(gcsPath, csvFileName, csvContent);
    Logger.log("✅ BTN Bank: " + csvFileName);
  }
}

/**
 * Parses a BTN bank statement CSV string into normalized rows.
 * Data rows are identified by a leading integer (row number).
 */
function parseAndFormatBTNBankFile_(fileContent) {
  const newHeader = [
    "No.","Post_Date","Post_Time","Eff_Date","Eff_Time",
    "Description","Amount_Debit","Amount_Credit","Balance","Reference_No"
  ];

  const lines    = fileContent.split(/\r?\n/).filter(l => l.trim() !== "");
  let dataRows   = [], dataFound = false;

  for (const line of lines) {
    const row = line.split(",");
    if (!dataFound && row[0] && !isNaN(parseInt(row[0].trim()))) dataFound = true;
    if (dataFound) {
      if (row[0] && isNaN(parseInt(row[0].trim()))) break;  // stop at non-numeric row (footer)
      dataRows.push(row);
    }
  }

  const formattedRows = dataRows.map(r => {
    const n = [...r];
    if (n[1]) n[1] = formatDateBTNBank_(n[1]);
    if (n[3]) n[3] = formatDateBTNBank_(n[3]);
    if (n[2]) n[2] = formatTimeBTNBank_(n[2]);
    if (n[4]) n[4] = formatTimeBTNBank_(n[4]);
    if (n[9]) n[9] = String(n[9]);  // Reference_No: keep as text
    return n;
  });

  return [newHeader, ...formattedRows];
}

function formatDateBTNBank_(v) {
  if (!v) return "";
  const p = v.split("/");
  if (p.length === 3) {
    if (p[2].length === 4) return `${p[2]}-${p[1].padStart(2,"0")}-${p[0].padStart(2,"0")}`;
    if (p[0].length === 4) return `${p[0]}-${p[1].padStart(2,"0")}-${p[2].padStart(2,"0")}`;
  }
  const d = new Date(v);
  return !isNaN(d) ? `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}` : v;
}

function formatTimeBTNBank_(v) {
  if (!v) return "";
  const d = new Date("1970/01/01 " + v);
  return !isNaN(d)
    ? `${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}:${String(d.getSeconds()).padStart(2,"0")}`
    : v;
}


// =============================================================================
//  BNI BANK — Account Statement
//  Input:  XLSX  (pattern: filename contains "BNI_BankFile")
//  Output: <original_name>.csv
//  Schema: Post_Date | Value_Date | Branch | Journal_No | Description | Debit | Credit
// =============================================================================

function processBankBNI() {
  const sourceFolderId      = CONFIG.FOLDERS.BANK.BNI.SRC;
  const destinationFolderId = CONFIG.FOLDERS.BANK.BNI.DST;
  const gcsPath             = CONFIG.GCS.PATHS.BNI_BANK;

  /** Filename filter: adjust to match your BNI bank statement files. */
  const FILE_NAME_PATTERN = "BNI_BankFile";
  const DELIMITER         = ";";

  // Column rename map: source column name → output column name
  const HEADER_RENAME = {
    "Post Date":    "Post_Date",
    "Value Date":   "Value_Date",
    "Branch":       "Branch",
    "Journal No.":  "Journal_No",
    "Description":  "Description",
    "Debit":        "Debit",
    "Credit":       "Credit"
  };

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const allFiles     = sourceFolder.getFiles();

  while (allFiles.hasNext()) {
    const f = allFiles.next();
    if (!f.getName().includes(FILE_NAME_PATTERN)) continue;

    // Convert XLSX → Google Sheets to extract values
    const tempSheet = Drive.Files.copy(
      { title: f.getName(), mimeType: MimeType.GOOGLE_SHEETS }, f.getId(), { convert: true }
    );
    const values    = SpreadsheetApp.openById(tempSheet.id).getSheets()[0].getDataRange().getValues();
    const header    = values[0];

    // Rename headers
    for (let i = 0; i < header.length; i++) {
      if (HEADER_RENAME[header[i]]) header[i] = HEADER_RENAME[header[i]];
    }

    const processed = [header];
    for (let i = 1; i < values.length; i++) {
      const r = values[i];
      if (!r[0]) continue;
      if (r[0] instanceof Date) r[0] = Utilities.formatDate(r[0], Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
      if (r[1] instanceof Date) r[1] = Utilities.formatDate(r[1], Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
      processed.push(r);
    }

    const csv         = processed.map(r => r.join(DELIMITER)).join("\n");
    const newFileName = f.getName().replace(/\.[^/.]+$/, "") + ".csv";
    destFolder.createFile(newFileName, csv, MimeType.CSV);
    uploadCsvToGcs_(gcsPath, newFileName, csv);
    DriveApp.getFileById(tempSheet.id).setTrashed(true);
    Logger.log("✅ BNI Bank: " + newFileName);
  }
}


// =============================================================================
//  MANDIRI BANK — Account Statement
//  Input:  CSV  (semicolon-delimited, pattern: filename contains "Acc_Statement")
//  Output: <original_name>.csv
//  Schema: AccountNo | Ccy | Post_Date | Remarks | Additional_Desc |
//          Credit_Amount | Debit_Amount | Close_Balance
// =============================================================================

function processBankMandiri() {
  const sourceFolderId      = CONFIG.FOLDERS.BANK.MANDIRI.SRC;
  const destinationFolderId = CONFIG.FOLDERS.BANK.MANDIRI.DST;
  const gcsPath             = CONFIG.GCS.PATHS.MANDIRI_BANK;

  const OUTPUT_HEADER = [
    "AccountNo","Ccy","Post_Date","Remarks","Additional_Desc",
    "Credit_Amount","Debit_Amount","Close_Balance"
  ];

  /** Filename filter: adjust to match your Mandiri account statement files. */
  const FILE_NAME_PATTERN = "Acc_Statement";

  const sourceFolder = DriveApp.getFolderById(sourceFolderId);
  const destFolder   = DriveApp.getFolderById(destinationFolderId);
  const files        = sourceFolder.searchFiles(`title contains '${FILE_NAME_PATTERN}'`);

  while (files.hasNext()) {
    const file = files.next();
    let data   = Utilities.parseCsv(file.getBlob().getDataAsString(), ";");

    // Remove trailing empty rows
    let last = data.length;
    while (last > 0 && data[last-1].join("").trim() === "") last--;
    data = data.slice(0, last);
    data.shift();  // Remove original header row

    let csvContent = OUTPUT_HEADER.join(";") + "\n";

    for (let i = 0; i < data.length; i++) {
      // AccountNo: strip thousands separators and cast to number
      if (typeof data[i][0] === "string")
        data[i][0] = parseFloat(data[i][0].replace(/,/g, ""));

      // Post_Date: normalize to yyyy-MM-dd HH:mm:ss
      const d = new Date(data[i][2]);
      if (!isNaN(d)) data[i][2] = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");

      csvContent += data[i].join(";") + "\n";
    }

    const newFileName = file.getName().replace(/\.[^/.]+$/, "") + ".csv";
    destFolder.createFile(newFileName, csvContent, MimeType.CSV);
    uploadCsvToGcs_(gcsPath, newFileName, csvContent);
    Logger.log("✅ Mandiri Bank: " + newFileName);
  }
}


// =============================================================================
//  MASTER RUNNER
// =============================================================================

/** Runs all bank formatters in sequence. Errors in one bank do not stop others. */
function processAllBanks() {
  const banks = [
    ["BCA Bank",     prosessBankBCA],
    ["BTN Bank",     processBankBTN],
    ["BNI Bank",     processBankBNI],
    ["Mandiri Bank", processBankMandiri],
  ];
  banks.forEach(([name, fn]) => {
    Logger.log(`🏦 Start ${name}`);
    try { fn(); } catch (e) { Logger.log(`❌ Error ${name}: ${e}`); }
  });
  Logger.log("✅ All banks done");
}
