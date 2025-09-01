import psycopg2
import csv
from datetime import datetime

def get_employee_before_update(cursor, employee_id):
    """UPDATE前の従業員データを取得"""
    try:
        select_query = """
        SELECT employee_id, first_name, last_name, department, salary
        FROM employees
        WHERE employee_id = %s;
        """
        
        cursor.execute(select_query, (employee_id,))
        employee = cursor.fetchone()
        
        return employee
        
    except psycopg2.Error as e:
        print(f"従業員データ取得エラー: {e}")
        return None

def update_employee_salary(conn, cursor, employee_id, new_salary):
    """従業員のsalaryをUPDATEする"""
    try:
        # UPDATE前のデータを取得
        before_data = get_employee_before_update(cursor, employee_id)
        
        if not before_data:
            print(f"employee_id {employee_id} の従業員が見つかりません")
            return False, None, None
        
        print(f"UPDATE前のデータ: {before_data}")
        
        # salary を UPDATE
        update_query = """
        UPDATE employees
        SET salary = %s
        WHERE employee_id = %s;
        """
        
        cursor.execute(update_query, (new_salary, employee_id))
        
        # 影響を受けた行数を確認
        rows_affected = cursor.rowcount
        
        if rows_affected == 0:
            print(f"employee_id {employee_id} の従業員が見つからないため、UPDATEされませんでした")
            conn.rollback()
            return False, before_data, None
        
        # UPDATE後のデータを取得して確認
        after_data = get_employee_before_update(cursor, employee_id)
        
        # コミット
        conn.commit()
        
        print(f"UPDATE後のデータ: {after_data}")
        print(f"salaryが {before_data[4]} から {new_salary} に更新されました")
        
        return True, before_data, after_data
        
    except psycopg2.Error as e:
        print(f"UPDATE処理でエラーが発生: {e}")
        conn.rollback()
        return False, before_data if 'before_data' in locals() else None, None
    except Exception as e:
        print(f"予期しないエラー: {e}")
        conn.rollback()
        return False, before_data if 'before_data' in locals() else None, None

def write_result_to_csv(success, employee_id, old_salary=None, new_salary=None, filename="update_result.csv"):
    """処理結果をCSVファイルに出力する"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # ヘッダー行
            writer.writerow(['timestamp', 'operation', 'employee_id', 'old_salary', 'new_salary', 'result'])
            
            # 結果行
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            operation = 'UPDATE_SALARY'
            result = 'Success' if success else 'Failure'
            
            writer.writerow([timestamp, operation, employee_id, old_salary, new_salary, result])
        
        print(f"結果を{filename}に出力しました: {result}")
        
    except Exception as e:
        print(f"CSVファイル出力エラー: {e}")

def write_simple_result_to_csv(success, filename="simple_result.csv"):
    """シンプルな結果をCSVファイルに出力する（要求仕様通り）"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # 結果のみを出力
            result = 'Success' if success else 'Failure'
            writer.writerow([result])
        
        print(f"処理結果を{filename}に出力しました: {result}")
        
    except Exception as e:
        print(f"CSVファイル出力エラー: {e}")

def create_sample_employee_if_not_exists(conn, cursor):
    """employee_id=1の従業員が存在しない場合、サンプルデータを作成"""
    try:
        # employee_id=1が存在するか確認
        cursor.execute("SELECT COUNT(*) FROM employees WHERE employee_id = 1")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("employee_id=1の従業員が存在しないため、サンプルデータを作成します...")
            
            # テーブルが存在するか確認し、必要に応じて作成
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    employee_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    department VARCHAR(100),
                    salary INTEGER
                );
            """)
            
            # サンプル従業員を挿入
            insert_query = """
            INSERT INTO employees (employee_id, first_name, last_name, department, salary)
            VALUES (1, 'John', 'Doe', 'Engineering', 75000)
            """
            
            cursor.execute(insert_query)
            conn.commit()
            
            print("employee_id=1のサンプル従業員を作成しました (John Doe, salary: 75000)")
            return True
            
        return False
        
    except psycopg2.Error as e:
        print(f"サンプルデータ作成エラー: {e}")
        conn.rollback()
        return False

def main():
    """メイン処理"""
    # データベース接続設定（環境に合わせて変更してください）
    db_config = {
        'host': 'localhost',
        'database': 'mydatabase',  # 実際のデータベース名に変更
        'user': 'myuser',          # 実際のユーザー名に変更
        'password': 'mypassword'   # 実際のパスワードに変更
    }
    
    # UPDATE対象の設定
    target_employee_id = 1
    new_salary = 60000
    
    conn = None
    cursor = None
    success = False
    old_salary = None
    
    try:
        # データベースに接続
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("データベースに接続しました")
        
        # テーブル存在確認とサンプルデータ作成
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'employees'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("employeesテーブルが存在しません。作成します...")
        
        # 必要に応じてサンプル従業員を作成
        create_sample_employee_if_not_exists(conn, cursor)
        
        # UPDATE処理実行
        print(f"\nemployee_id={target_employee_id} の salary を {new_salary} に更新します...")
        success, before_data, after_data = update_employee_salary(conn, cursor, target_employee_id, new_salary)
        
        if before_data:
            old_salary = before_data[4]
        
        # 結果をCSVファイルに出力
        write_result_to_csv(success, target_employee_id, old_salary, new_salary)
        write_simple_result_to_csv(success)
        
        if success:
            print("UPDATE処理が正常に完了しました")
        else:
            print("UPDATE処理が失敗しました")
        
    except psycopg2.OperationalError as e:
        print(f"データベース接続エラー: {e}")
        print("データベース設定を確認してください")
        write_result_to_csv(False, target_employee_id)
        write_simple_result_to_csv(False)
        
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")
        write_result_to_csv(False, target_employee_id)
        write_simple_result_to_csv(False)
        
    finally:
        # リソースのクリーンアップ
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("データベース接続を閉じました")

if __name__ == "__main__":
    main()