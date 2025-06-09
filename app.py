from flask import Flask, request, send_file, jsonify
import pandas as pd
from io import BytesIO
import zipfile
from datetime import datetime

app = Flask(__name__)

# ─── zone → expected columns mapping ───
EXPECTED = {
    'LPL Alerts':           ['Category', 'LPL Account Number'],
    'Schwab Alerts':        ['Subject', 'Date Created', 'Account'],
    'Eclipse Accounts':     ['Account Number', 'Managed Value'],
    'Eclipse Contributions':['Account Number', 'Model', 'Managed Value'],
    'Orion Query 30842':    ['Account #', 'Account Model', 'Account Value', 'Ticker', 'Current Units'],
    'Practifi PSC Requests':['Related Process: Account Number', 'New Model Trading'],
    'Practifi Cash Requests':['Related Process: Account Number',
                              'Related Process: Gross Amount Requested',
                              'Related Process: Amount to be set aside']
}

def read_and_validate(file_storage, expected_cols):
    """Load CSV or Excel, validate columns."""
    fn = file_storage.filename.lower()
    if fn.endswith('.csv'):
        df = pd.read_csv(file_storage)
    elif fn.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_storage)
    else:
        raise ValueError("Unsupported file type, must be .csv or .xlsx")
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {file_storage.filename}: {missing}")
    return df

# ─── stub functions: paste your pandas logic here ───
def process_raise_cash(df_rc):
    # paste your "Raise Cash" logic, return out_cash_df
    # example:
    today_str = datetime.today().strftime('%m-%d-%Y')
    rows = []
    for _,r in df_rc.iterrows():
        acct = r['Related Process: Account Number']
        gross = r['Related Process: Gross Amount Requested']
        if pd.notna(gross):
            rows.append({
                'Account Number': acct,
                'Set Aside Amount': gross,
                'Description': 'Raise Cash',
                'Start Date': today_str
            })
    return pd.DataFrame(rows)

def process_model_changes(df_rc, df_liq):
    # paste your PSC + Liquidation logic, return DataFrame
    # ...
    return pd.DataFrame([])

def process_notifications(df_lpl, df_schwab, df_accounts):
    # paste your Alerts + master-account-list merge logic, return DataFrame
    # ...
    return pd.DataFrame([])

def process_rebalances(df_orion, master_df):
    # paste your OOM rebalances logic, return DataFrame
    # ...
    return pd.DataFrame([])

def process_contributions(df_contrib, master_df):
    # paste your Contributions logic, return DataFrame
    # ...
    return pd.DataFrame([])

def process_master_accounts(all_dfs):
    # take all the small DataFrames, combine + filter your master list, return DataFrame
    return pd.DataFrame([])

@app.route('/process', methods=['POST'])
def process_all():
    try:
        # 1) read & validate each uploaded file
        dfs = {}
        for zone, cols in EXPECTED.items():
            f = request.files.get(zone)
            if not f:
                return jsonify(error=f"Missing upload for '{zone}'"), 400
            dfs[zone] = read_and_validate(f, cols)

        # 2) run each piece of logic, capturing outputs in-memory
        bio = BytesIO()
        with zipfile.ZipFile(bio, 'w') as z:
            # Raise-Cash
            cash_df = process_raise_cash(dfs['Practifi Cash Requests'])
            buf = BytesIO(); cash_df.to_excel(buf, index=False)
            z.writestr('Raise_Cash_Requests.xlsx', buf.getvalue())

            # PSC + Liquidation → Model_Changes.xlsx
            mc_df = process_model_changes(dfs['Practifi PSC Requests'],
                                          dfs['Orion Query 30842'])  # or df_liq
            buf = BytesIO(); mc_df.to_excel(buf, index=False)
            z.writestr('Model_Changes.xlsx', buf.getvalue())

            # Notifications.csv
            notif_df = process_notifications(dfs['LPL Alerts'],
                                             dfs['Schwab Alerts'],
                                             dfs['Eclipse Accounts'])
            buf = BytesIO(); notif_df.to_csv(buf, index=False)
            z.writestr('Notifications.csv', buf.getvalue())

            # Rebalances.csv
            reb_df = process_rebalances(dfs['Orion Query 30842'], notif_df)
            buf = BytesIO(); reb_df.to_csv(buf, index=False)
            z.writestr('Rebalances.csv', buf.getvalue())

            # Contributions.csv
            contrib_df = process_contributions(dfs['Eclipse Contributions'], notif_df)
            buf = BytesIO(); contrib_df.to_csv(buf, index=False)
            z.writestr('Contributions.csv', buf.getvalue())

            # Master Account Numbers.csv
            master_df = process_master_accounts(dfs)
            buf = BytesIO(); master_df.to_csv(buf, index=False)
            z.writestr('Master_Account_Numbers.csv', buf.getvalue())

        bio.seek(0)
        return send_file(
            bio,
            mimetype='application/zip',
            as_attachment=True,
            download_name='results.zip'
        )
    except ValueError as e:
        return jsonify(error=str(e)), 400
    except Exception as e:
        print("ERR:", e)
        return jsonify(error="Internal server error"), 500

if __name__ == '__main__':
    app.run(debug=True)
