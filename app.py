from flask import Flask, render_template, request, jsonify, session, redirect
from datetime import timedelta
from functools import wraps
import psycopg2
import psycopg2.extras
import hashlib
import secrets
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')
import io
import base64
import pandas as pd
import numpy as np
import os

app = Flask(__name__)
app.secret_key = "salesbi_secret_key"
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

# ===== LOGIN REQUIRED DECORATOR =====
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({"success": False, "error": "Authentication required"}), 401
        return f(*args, **kwargs)
    return decorated_function

# ================= DATABASE CONNECTION =================
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:D4wxQYWmuuL6PTO7@db.duupwhaklxcvmbfzynyz.supabase.co:5432/postgres')

def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise Exception(f"Database connection failed: {str(e)}")

# ================= SETUP TABLES =================
def setup_tables():
    """Create all required tables if they don't exist"""
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE,
            email VARCHAR(255) UNIQUE,
            password VARCHAR(255),
            FullName VARCHAR(255),
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP
        )""")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id SERIAL PRIMARY KEY,
            user_id INT,
            filename VARCHAR(255),
            upload_date TIMESTAMP DEFAULT NOW(),
            total_rows INT,
            total_columns INT,
            duplicate_rows INT,
            memory_usage_kb FLOAT,
            numeric_columns INT,
            categorical_columns INT
        )""")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id SERIAL PRIMARY KEY,
            upload_id INT,
            user_id INT,
            total_sales FLOAT,
            total_profit FLOAT,
            product_count INT,
            analysis_date TIMESTAMP DEFAULT NOW()
        )""")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS upload_products (
            id SERIAL PRIMARY KEY,
            upload_id INT,
            user_id INT,
            product_name VARCHAR(255),
            total_sales FLOAT,
            total_profit FLOAT
        )""")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS column_statistics (
            id SERIAL PRIMARY KEY,
            upload_id INT,
            column_name VARCHAR(255),
            column_type VARCHAR(50),
            missing_count INT,
            missing_percent FLOAT,
            unique_count INT,
            mean_value FLOAT,
            min_value FLOAT,
            max_value FLOAT,
            std_value FLOAT
        )""")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Tables verified/created successfully!")
    except Exception as e:
        print(f"⚠️ Table setup failed: {e}")

# ================= TEST CONNECTION =================
def test_database_connection():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print("✅ Database connection successful!")
        print(f"   SQL Server: {version[:60]}...")
        return True
    except Exception as e:
        print("❌ Database connection FAILED!")
        print(f"   Error: {e}")
        return False

# ================= PAGE ROUTES =================
@app.route("/")
def home():
    return render_template("login.html")

@app.route("/dashboard")
def dashboard_page():
    if "user_id" not in session:
        return redirect("/")
    return render_template("dashboard.html")

@app.route("/my-dashboard")
def my_dashboard_page():
    if "user_id" not in session:
        return redirect("/")
    return render_template("my_dashboard.html")

@app.route("/upload/<int:upload_id>")
def upload_detail_page(upload_id):
    if "user_id" not in session:
        return redirect("/")
    return render_template("upload_detail.html", upload_id=upload_id)

# ================= MY DASHBOARD API =================
@app.route("/api/my-dashboard", methods=["GET"])
def my_dashboard_data():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not authenticated"}), 401
    try:
        conn = get_db(); cursor = conn.cursor()
        uid = session["user_id"]
        cursor.execute("SELECT COUNT(*) FROM uploads WHERE user_id=%s", (uid,))
        total_uploads = cursor.fetchone()[0]
        cursor.execute("SELECT COALESCE(SUM(total_sales),0), COALESCE(SUM(total_profit),0), COUNT(*) FROM analysis_results WHERE user_id=%s", (uid,))
        row = cursor.fetchone()
        total_sales_all = float(row[0]); total_profit_all = float(row[1]); analyzed_count = int(row[2])
        cursor.execute("SELECT product_name, SUM(total_sales) s FROM upload_products WHERE user_id=%s GROUP BY product_name ORDER BY s DESC LIMIT 1", (uid,))
        best = cursor.fetchone()
        cursor.execute("SELECT DATE(upload_date) d, COUNT(*) c FROM uploads WHERE user_id=%s GROUP BY DATE(upload_date) ORDER BY d ASC", (uid,))
        activity = [{"date": str(r[0]), "count": int(r[1])} for r in cursor.fetchall()]
        cursor.execute("SELECT u.filename, ar.total_sales, ar.total_profit FROM uploads u JOIN analysis_results ar ON ar.upload_id=u.id WHERE u.user_id=%s ORDER BY u.upload_date DESC", (uid,))
        sales_per_dataset = [{"filename": r[0], "sales": float(r[1]), "profit": float(r[2])} for r in cursor.fetchall()]
        cursor.execute("""SELECT u.id, u.filename, u.upload_date, u.total_rows, u.total_columns,
            ar.total_sales, ar.total_profit, ar.product_count
            FROM uploads u LEFT JOIN analysis_results ar ON ar.upload_id=u.id
            WHERE u.user_id=%s ORDER BY u.upload_date DESC LIMIT 10""", (uid,))
        uploads = [{"id": r[0], "filename": r[1], "upload_date": str(r[2]), "total_rows": r[3],
                    "total_columns": r[4], "total_sales": float(r[5]) if r[5] else None,
                    "total_profit": float(r[6]) if r[6] else None, "product_count": r[7]}
                   for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify({"success": True, "total_uploads": total_uploads,
                        "total_sales_all": total_sales_all, "total_profit_all": total_profit_all,
                        "best_product": best[0] if best else "N/A",
                        "best_product_sales": float(best[1]) if best else 0, "analyzed_count": analyzed_count,
                        "activity": activity, "sales_per_dataset": sales_per_dataset,
                        "recent_uploads": uploads})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ================= UPLOAD DETAIL API =================
@app.route("/api/upload/<int:upload_id>", methods=["GET"])
def get_upload_detail(upload_id):
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not authenticated"}), 401
    try:
        conn = get_db(); cursor = conn.cursor()
        cursor.execute("SELECT id,filename,upload_date,total_rows,total_columns,duplicate_rows,memory_usage_kb,numeric_columns,categorical_columns FROM uploads WHERE id=%s AND user_id=%s", (upload_id, session["user_id"]))
        u = cursor.fetchone()
        if not u: return jsonify({"success": False, "error": "Not found"}), 404
        upload = {"id":u[0],"filename":u[1],"upload_date":str(u[2]),"total_rows":u[3],"total_columns":u[4],"duplicate_rows":u[5],"memory_usage_kb":u[6],"numeric_columns":u[7],"categorical_columns":u[8]}
        cursor.execute("SELECT total_sales,total_profit,product_count FROM analysis_results WHERE upload_id=%s", (upload_id,))
        ar = cursor.fetchone()
        analysis = {"total_sales":float(ar[0]),"total_profit":float(ar[1]),"product_count":int(ar[2])} if ar else None
        cursor.execute("SELECT product_name,total_sales,total_profit FROM upload_products WHERE upload_id=%s ORDER BY total_sales DESC", (upload_id,))
        products = [{"product":r[0],"sales":float(r[1]),"profit":float(r[2])} for r in cursor.fetchall()]
        cursor.execute("SELECT column_name,column_type,missing_count,missing_percent,unique_count,mean_value,min_value,max_value,std_value FROM column_statistics WHERE upload_id=%s", (upload_id,))
        columns = {r[0]:{"type":r[1],"missing":r[2],"missing_percent":float(r[3]) if r[3] else 0,"unique":r[4],"mean":float(r[5]) if r[5] else None,"min":float(r[6]) if r[6] else None,"max":float(r[7]) if r[7] else None,"std":float(r[8]) if r[8] else None} for r in cursor.fetchall()}
        cursor.close(); conn.close()
        return jsonify({"success":True,"upload":upload,"analysis":analysis,"products":products,"columns":columns})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ================= TEST CONNECTION ENDPOINT =================
@app.route("/api/test-connection", methods=["GET"])
def test_connection():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return jsonify({"success": True, "status": "connected", "version": version[:100]})
    except Exception as e:
        return jsonify({"success": False, "status": "disconnected", "error": str(e)}), 500

# ================= AUTH API =================
@app.route("/api/register", methods=["POST"])
def register():
    try:
        data = request.get_json()
        username = data.get("username")
        email = data.get("email")
        password = data.get("password")
        fullname = data.get("fullname")

        if not username or not email or not password:
            return jsonify({"success": False, "error": "All fields required"}), 400

        salt = secrets.token_hex(8)
        password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
        stored_password = f"sha256${salt}${password_hash}"

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users WHERE username=%s OR email=%s", (username, email))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({"success": False, "error": "Username or email already exists"}), 400

        cursor.execute(
            "INSERT INTO users (username,email,password,FullName,created_at) VALUES (%s,%s,%s,%s,NOW())",
            (username, email, stored_password, fullname)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True, "message": "Registration successful!"})
    except Exception as e:
        return jsonify({"success": False, "error": f"Registration failed: {str(e)}"}), 500


@app.route("/api/login", methods=["POST"])
def login():
    try:
        data = request.get_json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify({"success": False, "error": "Username and password required"}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id,username,email,password,FullName FROM users WHERE email=%s OR username=%s",
            (username, username)
        )
        user = cursor.fetchone()

        # Update last login
        if user:
            try:
                cursor.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (user[0],))
                conn.commit()
            except:
                pass  # last_login column may not exist yet

        cursor.close()
        conn.close()

        if not user:
            return jsonify({"success": False, "error": "Invalid credentials"}), 401

        user_id, db_username, db_email, stored_password, fullname = user

        password_valid = False
        if stored_password.startswith("sha256$"):
            parts = stored_password.split("$")
            if len(parts) == 3:
                salt = parts[1]
                db_hash = parts[2]
                password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
                password_valid = (password_hash == db_hash)
        else:
            password_valid = (stored_password == password)

        if not password_valid:
            return jsonify({"success": False, "error": "Invalid credentials"}), 401

        session.permanent = True
        session["user_id"] = user_id
        session["username"] = db_username
        session["email"] = db_email
        session["fullname"] = fullname or db_username

        return jsonify({
            "success": True,
            "user": {"username": db_username, "email": db_email, "fullname": fullname}
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"Login failed: {str(e)}"}), 500


@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"success": True})


# ================= USER PROFILE API =================
@app.route("/api/profile", methods=["GET"])
@login_required
def get_profile():
    try:
        conn = get_db()
        cursor = conn.cursor()

        # Get user info
        cursor.execute("SELECT username, email, FullName FROM users WHERE id=%s", (session["user_id"],))
        user = cursor.fetchone()

        # Get total uploads count
        cursor.execute("SELECT COUNT(*) FROM uploads WHERE user_id=%s", (session["user_id"],))
        upload_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        if not user:
            return jsonify({"success": False, "error": "User not found"}), 404

        username, email, fullname = user
        initials = "".join([w[0].upper() for w in (fullname or username).split()[:2]])

        return jsonify({
            "success": True,
            "username": username,
            "email": email,
            "fullname": fullname or username,
            "initials": initials,
            "upload_count": upload_count
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ================= UPLOAD HISTORY API =================
@app.route("/api/upload-history", methods=["GET"])
@login_required
def upload_history():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.id, u.filename, u.upload_date, u.total_rows, u.total_columns,
                   ar.total_sales, ar.total_profit
            FROM uploads u
            LEFT JOIN analysis_results ar ON ar.upload_id = u.id
            WHERE u.user_id = ?
            ORDER BY u.upload_date DESC LIMIT 10
        """, (session["user_id"],))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        history = []
        for row in rows:
            history.append({
                "id": row[0],
                "filename": row[1],
                "upload_date": str(row[2]),
                "total_rows": row[3],
                "total_columns": row[4],
                "total_sales": float(row[5]) if row[5] else None,
                "total_profit": float(row[6]) if row[6] else None
            })

        return jsonify({"success": True, "history": history})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ================= ANALYZE API =================
@app.route('/api/analyze', methods=['POST'])
@login_required
def analyze():
    try:
        if "user_id" not in session:
            return jsonify({"success": False, "error": "Session expired. Please refresh and login again."}), 401
        if 'file' not in request.files:
            return jsonify({"success": False, "error": "No file uploaded"}), 400

        file = request.files['file']

        if file.filename == "":
            return jsonify({"success": False, "error": "Empty filename"}), 400

        if not file.filename.endswith('.csv'):
            return jsonify({"success": False, "error": "Only CSV files allowed"}), 400

        df = pd.read_csv(file)

        if df.empty:
            return jsonify({"success": False, "error": "CSV file is empty"}), 400

        total_rows = int(df.shape[0])
        total_columns = int(df.shape[1])
        duplicate_rows = int(df.duplicated().sum())
        memory_usage = float(df.memory_usage(deep=True).sum() / 1024)
        num_cols = len(df.select_dtypes(include=np.number).columns)
        cat_cols = total_columns - num_cols

        overview = {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "duplicate_rows": duplicate_rows,
            "memory_usage": f"{memory_usage:.2f} KB",
            "numeric_columns": num_cols,
            "categorical_columns": cat_cols
        }

        columns_info = {}
        desc = df.describe(include='all').to_dict()

        for col in df.columns:
            missing = int(df[col].isnull().sum())
            missing_percent = float((missing / total_rows) * 100) if total_rows > 0 else 0

            def get_val(key):
                if key in desc[col] and pd.notna(desc[col][key]):
                    if isinstance(desc[col][key], np.integer):
                        return int(desc[col][key])
                    elif isinstance(desc[col][key], np.floating):
                        return float(desc[col][key])
                    return str(desc[col][key])
                return None

            is_numeric = pd.api.types.is_numeric_dtype(df[col])

            col_data = {
                "type": "numeric" if is_numeric else "categorical",
                "missing": missing,
                "missing_percent": round(missing_percent, 2),
                "unique": get_val('unique') or int(df[col].nunique())
            }

            if is_numeric:
                col_data.update({
                    "mean": get_val('mean'),
                    "std": get_val('std'),
                    "min": get_val('min'),
                    "25%": get_val('25%'),
                    "50%": get_val('50%'),
                    "75%": get_val('75%'),
                    "max": get_val('max'),
                    "skewness": float(df[col].skew()) if pd.notna(df[col].skew()) else None,
                    "kurtosis": float(df[col].kurtosis()) if pd.notna(df[col].kurtosis()) else None,
                    "zeros": int((df[col] == 0).sum())
                })
            else:
                top_val = get_val('top')
                freq_val = get_val('freq')
                col_data.update({
                    "top": str(top_val) if top_val is not None else None,
                    "freq": int(freq_val) if freq_val is not None else None
                })

            columns_info[col] = col_data

        corr_matrix = {}
        if num_cols > 1:
            numeric_df = df.select_dtypes(include=np.number)
            corr_df = numeric_df.corr().fillna(0)
            for col_x in corr_df.columns:
                corr_matrix[col_x] = {}
                for col_y in corr_df.columns:
                    corr_matrix[col_x][col_y] = round(float(corr_df.loc[col_x, col_y]), 2)

        # ===== SALES BI LOGIC — SMART AUTO DETECTION =====
        lower_cols = {c.lower().strip(): c for c in df.columns}
        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        categorical_cols_list = df.select_dtypes(exclude=np.number).columns.tolist()

        # Smart keyword scoring for each column
        product_keywords = ['product','item','category','name','type','brand','line','description','sku','model','group','class','segment']
        sales_keywords = ['sales','revenue','total','amount','price','income','gross','turnover','receipt','billing','value','earned']
        profit_keywords = ['profit','margin','net','earning','gain','income','return','benefit','surplus','pnl']

        def best_match(col_list, keywords):
            best, best_score = None, 0
            for col in col_list:
                cl = col.lower()
                score = sum(1 for kw in keywords if kw in cl)
                if score > best_score:
                    best, best_score = col, score
            return best if best_score > 0 else None

        # Product col — keyword match first, then first categorical, then first column with lowest unique ratio
        product_col = best_match(categorical_cols_list, product_keywords)
        if not product_col and categorical_cols_list:
            product_col = categorical_cols_list[0]
        if not product_col:
            # No categorical columns — pick column with fewest unique values (most like a category)
            all_cols = df.columns.tolist()
            product_col = min(all_cols, key=lambda c: df[c].nunique())

        # Sales col — keyword match, else highest sum numeric
        sales_col = best_match(numeric_cols, sales_keywords)
        if not sales_col and len(numeric_cols) >= 1:
            sales_col = max(numeric_cols, key=lambda c: df[c].sum())

        # Profit col — keyword match, else second highest numeric (different from sales)
        profit_col = best_match([c for c in numeric_cols if c != sales_col], profit_keywords)
        if not profit_col and len(numeric_cols) >= 2:
            remaining = [c for c in numeric_cols if c != sales_col]
            profit_col = max(remaining, key=lambda c: abs(df[c].sum())) if remaining else None
        
        # Last resort — if still no profit col, calculate from sales - cost
        if not profit_col and sales_col:
            cost_candidates = [c for c in numeric_cols if c != sales_col]
            if cost_candidates:
                profit_col = cost_candidates[0]

        print(f"DEBUG FINAL: product={product_col}, sales={sales_col}, profit={profit_col}")

        sales_data = None
        total_sales = None
        total_profit = None
        product_count = None

        if product_col and sales_col and profit_col:
            clean_df = df.dropna(subset=[product_col])
            clean_df[sales_col] = pd.to_numeric(clean_df[sales_col], errors='coerce').fillna(0)
            clean_df[profit_col] = pd.to_numeric(clean_df[profit_col], errors='coerce').fillna(0)

            total_sales = float(clean_df[sales_col].sum())
            total_profit = float(clean_df[profit_col].sum())

            product_group = clean_df.groupby(product_col).agg({
                sales_col: 'sum',
                profit_col: 'sum'
            }).reset_index().sort_values(by=sales_col, ascending=False)

            product_count = len(product_group)

            product_breakdown = []
            for _, row in product_group.iterrows():
                product_breakdown.append({
                    "product": str(row[product_col]),
                    "sales": float(row[sales_col]),
                    "profit": float(row[profit_col])
                })

            # Generate chart
            chart_base64 = ""
            if len(product_group) > 0:
                top_products = product_group.head(10)
                fig, ax = plt.subplots(figsize=(10, 6))
                barWidth = 0.35
                br1 = np.arange(len(top_products))
                br2 = [x + barWidth for x in br1]
                ax.bar(br1, top_products[sales_col], color='#1a73e8', width=barWidth, edgecolor='grey', label='Sales')
                ax.bar(br2, top_products[profit_col], color='#34a853', width=barWidth, edgecolor='grey', label='Profit/Loss')
                ax.set_xlabel('Products', fontweight='bold', fontsize=12)
                ax.set_ylabel('Amount ($)', fontweight='bold', fontsize=12)
                ax.set_xticks([r + barWidth / 2 for r in range(len(top_products))])
                ax.set_xticklabels([str(p)[:15] + ('...' if len(str(p)) > 15 else '') for p in top_products[product_col]], rotation=45, ha='right')
                ax.legend()
                ax.grid(axis='y', linestyle='--', alpha=0.7)
                plt.tight_layout()
                buf = io.BytesIO()
                fig.savefig(buf, format="png", dpi=100)
                buf.seek(0)
                chart_base64 = base64.b64encode(buf.read()).decode("utf-8")
                plt.close(fig)

            sales_data = {
                "found_metrics": True,
                "total_sales": total_sales,
                "total_profit": total_profit,
                "products": product_breakdown,
                "chart_image": chart_base64
            }
        else:
            sales_data = {
                "found_metrics": False,
                "missing": [col_name for col_name, found in [('Product', product_col), ('Sales', sales_col), ('Profit', profit_col)] if not found]
            }

        # ========== SAVE TO DATABASE ==========
        upload_id = None
        try:
            conn = get_db()
            cursor = conn.cursor()

            # Insert upload record
            cursor.execute("""
                INSERT INTO uploads (user_id, filename, total_rows, total_columns, duplicate_rows, memory_usage_kb, numeric_columns, categorical_columns)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (session['user_id'], file.filename, total_rows, total_columns, duplicate_rows, memory_usage, num_cols, cat_cols))
            cursor.execute("SELECT lastval()")
            row = cursor.fetchone()
            upload_id = int(row[0]) if row and row[0] is not None else None
            conn.commit()
            print(f"DEBUG upload_id={upload_id}")

            # Save analysis results (KPIs)
            if total_sales is not None:
                cursor.execute("""
                    INSERT INTO analysis_results (upload_id, user_id, total_sales, total_profit, product_count)
                    VALUES (%s, %s, %s, %s, %s)
                """, (upload_id, session['user_id'], total_sales, total_profit, product_count))
                conn.commit()
                for p in product_breakdown:
                    cursor.execute("INSERT INTO upload_products (upload_id, user_id, product_name, total_sales, total_profit) VALUES (%s, %s, %s, %s, %s)",
                        (upload_id, session['user_id'], p['product'], p['sales'], p['profit']))
                conn.commit()

            # Save per-column statistics
            for col_name, info in columns_info.items():
                cursor.execute("""
                    INSERT INTO column_statistics (upload_id, column_name, column_type, missing_count, missing_percent, unique_count, mean_value, min_value, max_value, std_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    upload_id,
                    col_name,
                    info['type'],
                    info['missing'],
                    info['missing_percent'],
                    info['unique'],
                    info.get('mean'),
                    info.get('min'),
                    info.get('max'),
                    info.get('std')
                ))
            conn.commit()

            cursor.close()
            conn.close()
            print(f"✅ Saved! Upload ID={upload_id} user_id={session.get(chr(117)+chr(115)+chr(101)+chr(114)+chr(95)+chr(105)+chr(100))}")

        except Exception as db_error:
            print(f"⚠️ Database save failed: {db_error}")

        return jsonify({
            "success": True,
            "filename": file.filename,
            "upload_id": upload_id,
            "saved_to_database": upload_id is not None,
            "overview": overview,
            "columns": columns_info,
            "correlation": corr_matrix,
            "sales_data": sales_data
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


# ================= RUN =================
if __name__ == "__main__":
    print("=" * 60)
    print("SALES ANALYTICS BI - FINAL YEAR PROJECT")
    print("=" * 60)

    db_connected = test_database_connection()

    if db_connected:
        setup_tables()
        print("✅ Starting server...")
        print("🔗 Open: http://localhost:5000")
        print("=" * 60)
        app.run(debug=True, host='0.0.0.0', port=5000)
    else:
        print("❌ Cannot start - fix database connection first!")
        print("=" * 60)
