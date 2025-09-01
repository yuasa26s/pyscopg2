import psycopg2
import csv
from datetime import datetime
import os

def select_all_employees(cursor):
    """employeesテーブルから全従業員データを取得"""
    try:
        # 全従業員データを取得
        select_query = """
        SELECT employee_id, first_name, last_name, department, salary
        FROM employees
        ORDER BY employee_id;
        """
        
        cursor.execute(select_query)
        employees = cursor.fetchall()
        
        # カラム名を取得
        column_names = [desc[0] for desc in cursor.description]
        
        print(f"取得した従業員数: {len(employees)}")
        
        return employees, column_names
        
    except psycopg2.Error as e:
        print(f"SELECT処理でエラーが発生: {e}")
        return None, None
    except Exception as e:
        print(f"予期しないエラー: {e}")
        return None, None

def write_employees_to_csv(employees, column_names, filename="employees_data.csv"):
    """従業員データをCSVファイルに出力"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # ヘッダー行（カラム名）を書き込み
            writer.writerow(column_names)
            
            # データ行を書き込み
            for employee in employees:
                writer.writerow(employee)
        
        print(f"従業員データを{filename}に出力しました")
        print(f"出力された行数: {len(employees) + 1}行（ヘッダー含む）")
        
        return True
        
    except Exception as e:
        print(f"CSVファイル出力エラー: {e}")
        return False

def write_operation_log(success, record_count=0, filename="operation_log.csv"):
    """操作ログをCSVファイルに出力"""
    try:
        # ファイルが存在するかチェック
        file_exists = os.path.isfile(filename)
        
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # ファイルが新規作成の場合はヘッダーを追加
            if not file_exists:
                writer.writerow(['timestamp', 'operation', 'result', 'record_count'])
            
            # ログ行を追加
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            operation = 'SELECT_ALL_EMPLOYEES'
            result = 'Success' if success else 'Failure'
            
            writer.writerow([timestamp, operation, result, record_count])
        
        print(f"操作ログを{filename}に記録しました: {result}")
        
    except Exception as e:
        print(f"ログファイル出力エラー: {e}")

def display_sample_data(employees, column_names, max_rows=5):
    """取得したデータのサンプルを表示"""
    if not employees:
        print("表示するデータがありません")
        return
    
    print(f"\n=== 取得データのサンプル（最大{max_rows}行）===")
    
    # ヘッダー表示
    header = " | ".join(f"{col:>12}" for col in column_names)
    print(header)
    print("-" * len(header))
    
    # データ表示
    for i, employee in enumerate(employees[:max_rows]):
        row = " | ".join(f"{str(val):>12}" for val in employee)
        print(row)
    
    if len(employees) > max_rows:
        print(f"... (他{len(employees) - max_rows}行)")
    print()

def create_sample_data_if_empty(conn, cursor):
    """テーブルが空の場合、サンプルデータを挿入"""
    try:
        # 既存データ数を確認
        cursor.execute("SELECT COUNT(*) FROM employees")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("テーブルが空のため、サンプルデータを挿入します...")
            
            sample_employees = [
                (1, 'John', 'Doe', 'Engineering', 75000),
                (2, 'Jane', 'Wilson', 'Marketing', 65000),
                (3, 'Alice', 'Smith', 'IT', 55000),
                (4, 'Bob', 'Johnson', 'Sales', 60000),
                (5, 'Carol', 'Brown', 'HR', 50000)
            ]
            
            insert_query = """
            INSERT INTO employees (employee_id, first_name, last_name, department, salary)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, sample_employees)
            conn.commit()
            
            print(f"サンプルデータ{len(sample_employees)}件を挿入しました")
            return True
            
        return False
        
    except psycopg2.Error as e:
        print(f"サンプルデータ挿入エラー: {e}")
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
    
    conn = None
    cursor = None
    success = False
    record_count = 0
    
    try:
        # データベースに接続
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("データベースに接続しました")
        
        # テーブルが存在するか確認
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'employees'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("employeesテーブルが存在しません")
            write_operation_log(False, 0)
            return
        
        # テーブルが空の場合はサンプルデータを挿入
        create_sample_data_if_empty(conn, cursor)
        
        # 全従業員データを取得
        employees, column_names = select_all_employees(cursor)
        
        if employees is not None:
            record_count = len(employees)
            
            # データのサンプルを表示
            display_sample_data(employees, column_names)
            
            # CSVファイルに出力
            csv_success = write_employees_to_csv(employees, column_names)
            
            if csv_success:
                success = True
                print("SELECT処理が正常に完了しました")
            else:
                print("CSV出力で問題が発生しました")
        else:
            print("データの取得に失敗しました")
        
    except psycopg2.OperationalError as e:
        print(f"データベース接続エラー: {e}")
        print("データベース設定を確認してください")
        
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")
        
    finally:
        # 操作ログを記録
        write_operation_log(success, record_count)
        
        # リソースのクリーンアップ
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("データベース接続を閉じました")

if __name__ == "__main__":
    main()