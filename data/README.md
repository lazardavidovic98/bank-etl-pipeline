# data/

| Folder | Contents | In Git? |
|---|---|---|
| `sample/transactions_snapshot.json` | Last real response from the original API (`http://65.52.154.25`): 26,052 card transactions. Used in `file` mode and as the fallback in `auto` mode | yes |
| `raw/` | Raw file of each run, exactly as downloaded or copied | no |
| `processed/` | Parsed row-per-line CSV of each run | no |
| `rejected/` | Rows that failed validation, with a `reasons` column | no |

The pipeline keeps the last `KEEP_RUNS` (default 7) runs in `raw/`, `processed/` and `rejected/` and deletes older files.

The snapshot was committed to the original repository as `data/dataset.json`. Its format is an envelope `{"status": "200", "message": "ok", "json": "<pandas column-oriented JSON string>"}` with the columns `index, City, Date, Card Type, Exp Type, Gender, Amount`.
