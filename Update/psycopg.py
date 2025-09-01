import psycopg2
import csv
from datetime import datetime

def main():
    # データベース接続設定（必要に応じて変更してください）
    db_config = {
        'host': 'localhost',
        'database': 'mydatabase',
        'user': 'myuser', 
        'password': 'Aya1126s'
    }
    
    conn = None
    cursor = None
    success = False
    
    try:
        # データベース接続
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("データベースに接続しました")
        
        # テーブル作成（存在しない場合）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                department VARCHAR(100),
                salary INTEGER
            )
        """)
        
        # サンプルデータ挿入（employee_id=1が存在しない場合）
        cursor.execute("SELECT COUNT(*) FROM employees WHERE employee_id = 1")
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO employees (employee_id, first_name, last_name, department, salary)
                VALUES (1, 'John', 'Doe', 'Engineering', 75000)
            """)
            print("サンプル従業員を作成しました")
        
        # UPDATE前のデータ確認
        cursor.execute("SELECT * FROM employees WHERE employee_id = 1")
        before_data = cursor.fetchone()
        print(f"UPDATE前: {before_data}")
        
        # salaryをUPDATE
        cursor.execute("UPDATE employees SET salary = %s WHERE employee_id = %s", (60000, 1))
        
        # UPDATE後のデータ確認
        cursor.execute("SELECT * FROM employees WHERE employee_id = 1")
        after_data = cursor.fetchone()
        print(f"UPDATE後: {after_data}")
        
        # コミット
        conn.commit()
        success = True
        print("UPDATE処理成功")
        
    except Exception as e:
        print(f"エラー: {e}")
        if conn:
            conn.rollback()
        success = False
    
    finally:
        # CSV出力
        try:
            with open('simple_result.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Success' if success else 'Failure'])
            print("結果をCSVに出力しました")
        except Exception as e:
            print(f"CSV出力エラー: {e}")
        
        # 接続を閉じる
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("データベース接続を閉じました")

if __name__ == "__main__":
    main()
