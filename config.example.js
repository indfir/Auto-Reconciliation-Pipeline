/**
 * config.example.js
 * =================
 * Copy this file to config.js and fill in your own values.
 * NEVER commit config.js to version control (it's in .gitignore).
 *
 * In Google Apps Script, paste the CONFIG object at the top of your project
 * or store sensitive values (GCS_KEY) in Project Settings → Script Properties.
 */

const CONFIG = {

  /** ── Google Cloud Storage ──────────────────────────────── */
  GCS: {
    BUCKET: "YOUR_GCS_BUCKET_NAME",          // e.g. "my-company-bi-bucket"
    SCOPE:  "https://www.googleapis.com/auth/devstorage.full_control",

    /** GCS object path prefix per source (no leading/trailing slash) */
    PATHS: {
      // Issuers
      BTN:              "recon-project/issuer/btn",
      BCA_ISSUER:       "recon-project/issuer/bca",
      SHOPEEPAY:        "recon-project/issuer/shopeepay",
      INDODANA:         "recon-project/issuer/indodana",
      KREDIVO:          "recon-project/issuer/kredivo",
      LINKAJA:          "recon-project/issuer/linkaja",
      OTTOPAY:          "recon-project/issuer/ottopay",
      OTTOPAY_DASHBOARD:"recon-project/issuer/ottopay/dashboard",
      NOBU:             "recon-project/issuer/nobu",
      BRI:              "recon-project/issuer/bri",

      // Banks
      BNI_ISSUER:       "recon-project/bank/bni/bank_report",
      MANDIRI_ISSUER:   "recon-project/bank/mandiri/bank_report",
      BCA_BANK:         "recon-project/bank/bca/bank_file",
      BTN_BANK:         "recon-project/bank/btn/mutation_file",
      BNI_BANK:         "recon-project/bank/bni/bank_file",
      MANDIRI_BANK:     "recon-project/bank/mandiri/bank_file",
    }
  },

  /** ── Google Drive Folder IDs ───────────────────────────── */
  /** Find a folder ID in its Google Drive URL: drive.google.com/drive/folders/<ID> */
  FOLDERS: {
    ISSUER: {
      BTN:               { SRC: "YOUR_BTN_SOURCE_FOLDER_ID",       DST: "YOUR_BTN_DEST_FOLDER_ID"       },
      BCA:               { SRC: "YOUR_BCA_SOURCE_FOLDER_ID",       DST: "YOUR_BCA_DEST_FOLDER_ID"       },
      BNI:               { SRC: "YOUR_BNI_SOURCE_FOLDER_ID",       DST: "YOUR_BNI_DEST_FOLDER_ID"       },
      MANDIRI:           { SRC: "YOUR_MANDIRI_SOURCE_FOLDER_ID",   DST: "YOUR_MANDIRI_DEST_FOLDER_ID"   },
      SHOPEEPAY:         { SRC: "YOUR_SHOPEEPAY_SOURCE_FOLDER_ID", DST: "YOUR_SHOPEEPAY_DEST_FOLDER_ID" },
      INDODANA:          { SRC: "YOUR_INDODANA_SOURCE_FOLDER_ID",  DST: "YOUR_INDODANA_DEST_FOLDER_ID"  },
      KREDIVO:           { SRC: "YOUR_KREDIVO_SOURCE_FOLDER_ID",   DST: "YOUR_KREDIVO_DEST_FOLDER_ID"   },
      LINKAJA:           { SRC: "YOUR_LINKAJA_SOURCE_FOLDER_ID",   DST: "YOUR_LINKAJA_DEST_FOLDER_ID"   },
      OTTOPAY:           { SRC: "YOUR_OTTOPAY_SOURCE_FOLDER_ID",   DST: "YOUR_OTTOPAY_DEST_FOLDER_ID"   },
      OTTOPAY_DASHBOARD: { SRC: "YOUR_OTTOPAY_DASH_SOURCE_FOLDER_ID", DST: "YOUR_OTTOPAY_DASH_DEST_FOLDER_ID" },
      NOBU:              { SRC: "YOUR_NOBU_SOURCE_FOLDER_ID",      DST: "YOUR_NOBU_DEST_FOLDER_ID"      },
      BRI:               { SRC: "YOUR_BRI_SOURCE_FOLDER_ID",       DST: "YOUR_BRI_DEST_FOLDER_ID"       },
    },
    BANK: {
      BCA:     { SRC: "YOUR_BANK_BCA_SOURCE_FOLDER_ID",     DST: "YOUR_BANK_BCA_DEST_FOLDER_ID"     },
      BTN:     { SRC: "YOUR_BANK_BTN_SOURCE_FOLDER_ID",     DST: "YOUR_BANK_BTN_DEST_FOLDER_ID"     },
      BNI:     { SRC: "YOUR_BANK_BNI_SOURCE_FOLDER_ID",     DST: "YOUR_BANK_BNI_DEST_FOLDER_ID"     },
      MANDIRI: { SRC: "YOUR_BANK_MANDIRI_SOURCE_FOLDER_ID", DST: "YOUR_BANK_MANDIRI_DEST_FOLDER_ID" },
    }
  },

  /** ── Notification ──────────────────────────────────────── */
  NOTIFICATION: {
    EMAIL: "your-email@example.com",   // Receives completion alerts
  },

  /** ── Timezone ───────────────────────────────────────────── */
  TIMEZONE: "Asia/Jakarta",

  /** ── Output delimiter ──────────────────────────────────── */
  OUTPUT_DELIMITER: ";",
};
