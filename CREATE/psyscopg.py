import psycopg2
import csv
from datetime import datetime

def create_employee_table_if_not_exists(cursor):
    """employeesテーブルが存在しない場合は作成する"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS employees (
        employee_id INTEGER PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        department VARCHAR(100),
        salary INTEGER
    );
    """
    cursor.execute(create_table_query)

def insert_employee(conn, cursor):
    """新しい従業員データをINSERTする"""
    try:
        # employeesテーブルの作成（存在しない場合）
        create_employee_table_if_not_exists(cursor)
        
        # 新しい従業員データのINSERT
        insert_query = """
        INSERT INTO employees (employee_id, first_name, last_name, department, salary)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        employee_data = (3, 'Alice', 'Smith', 'IT', 55000)
        
        cursor.execute(insert_query, employee_data)
        
        # コミット
        conn.commit()
        
        print("従業員データの挿入に成功しました")
        print(f"挿入されたデータ: {employee_data}")
        
        return True
        
    except psycopg2.IntegrityError as e:
        print(f"データ整合性エラー: {e}")
        conn.rollback()
        return False
    except psycopg2.Error as e:
        print(f"データベースエラー: {e}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"予期しないエラー: {e}")
        conn.rollback()
        return False

def write_result_to_csv(success, filename="insert_result.csv"):
    """処理結果をCSVファイルに出力する"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # ヘッダー行
            writer.writerow(['timestamp', 'operation', 'result'])
            
            # 結果行
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            operation = 'INSERT_EMPLOYEE'
            result = 'Success' if success else 'Failure'
            
            writer.writerow([timestamp, operation, result])
        
        print(f"結果を{filename}に出力しました: {result}")
        
    except Exception as e:
        print(f"CSVファイル出力エラー: {e}")

def main():
    """メイン処理"""
    # データベース接続設定（環境に合わせて変更してください）
    db_config = {
        'host': 'localhost',
        'database': 'mydatabase',  # 実際のデータベース名に変更
        'user': 'myuser',          # 実際のユーザー名に変更
        'password': 'mypassword'   # 実際のパスワードに変更
    }
    
    conn = None
    cursor = None
    
    try:
        # データベースに接続
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("データベースに接続しました")
        
        # INSERT処理実行
        success = insert_employee(conn, cursor)
        
        # 結果をCSVファイルに出力
        write_result_to_csv(success)
        
        # 挿入後のテーブル内容を確認
        if success:
            cursor.execute("SELECT * FROM employees WHERE employee_id = 3")
            result = cursor.fetchone()
            if result:
                print(f"確認: {result}")
        
    except psycopg2.OperationalError as e:
        print(f"データベース接続エラー: {e}")
        print("データベース設定を確認してください")
        write_result_to_csv(False)
        
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")
        write_result_to_csv(False)
        
    finally:
        # リソースのクリーンアップ
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("データベース接続を閉じました")

if __name__ == "__main__":
    main()