# -----------------------------------------------------------------------------
# 会社名: 湘南技術センター株式会社
# 名前: 塩見 和則
# 作成日: 2024/7/11
# -----------------------------------------------------------------------------

# 標準ライブラリ
import datetime as dt

# 外部ライブラリ
import mysql.connector
import file_util

# DB接続情報
DB_HOST = "XXX.XXX.XXX.XXX"
DB_USER = "XXX"
DB_PASSWORD = "XXX"
DB_DATABASE = "XXX"

# テーブル名
ORIGIN_TABLE_NAME = "izumi_sola_origin"
HOURLY_TABLE_NAME = "izumi_sola_hourly"
DAILY_TABLE_NAME = "izumi_sola_daily"
MONTHLY_TABLE_NAME = "izumi_sola_monthly"

def db_init():
    
    """
    db_init 関数

    概要:
        MySQLデータベースに接続し、コネクションとカーソルを取得する。

    引数:
        なし

    戻り値:
        conn (mysql.connector.connection.MySQLConnection): MySQLデータベース接続オブジェクト。
        cursor (mysql.connector.cursor.MySQLCursor): MySQLデータベースカーソルオブジェクト。

    例外処理:
        MySQLデータベースへの接続に失敗した場合は例外が発生する可能性がある。
    """

    # MySQLに接続
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

    # カーソルを取得
    cursor = conn.cursor()

    # コネクタとカーソルを返す
    return conn,cursor

def db_close(conn,cursor):

    """
    db_close 関数

    概要:
        MySQLデータベースのコネクションとカーソルを閉じる。

    引数:
        conn (mysql.connector.connection.MySQLConnection): MySQLデータベース接続オブジェクト。
        cursor (mysql.connector.cursor.MySQLCursor): MySQLデータベースカーソルオブジェクト。

    戻り値:
        なし

    例外処理:
        なし

    """

    # カーソルを閉じる
    cursor.close()

    # コネクタを閉じる
    conn.close()

def db_UpdateInsertOriginTable(csv_df,oha_yymmddhh):
    
    """
    db_UpdateInsertOriginTable 関数

    概要:
        元データテーブルにデータをUPDATEまたはINSERTする。

    引数:
        csv_df: CSVファイルを格納したDataframe
            - 現場コード
            - 日付
            - 時間
            - 1-8番目の電力
            - 1-8番目の電圧
            - 1-8番目の発電量
            - 日射量
            - 温度
            - エラーコード
            - 売電量

    戻り値:
        なし

    例外処理:
        データベース操作中にエラーが発生した場合は、エラーログを出力する。
    """

    # CSVファイルの行数分ループする
    for index,row in csv_df.iterrows():

        # DBに接続
        my_conn,my_cursor = db_init()

        # 現場コード、観測日、観測時間を取得
        genba_cd = int(row[0])
        kansoku_date_int = int(row[1])
        kansoku_time_int = int(row[2])

        # 観測日時を取得する
        year = int('20' + oha_yymmddhh[:2])  # 年を取得（YYを補完して年とする）
        month = int(oha_yymmddhh[2:4])  # 月を取得
        day = int(oha_yymmddhh[4:6])    # 日を取得
        hour = int(oha_yymmddhh[6:8])   # 時を取得
        minute = int(str(kansoku_time_int)[-2:])  # 分を取得（最後の2桁を整数に変換）
        kansoku_datetime = dt.datetime(year, month, day,hour,minute).strftime('%Y-%m-%d %H:%M')  # 日時型で出力

        # 更新日時を取得する
        update_datetime = str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))

        # 日別テーブルに対して該当日の件数を取得
        sql = ""
        sql = f"""
        SELECT
            COUNT(*) 
        FROM
            {ORIGIN_TABLE_NAME}
        WHERE
            GENBA_CD = {genba_cd}
            AND KANSOKU_DATE_INT = {kansoku_date_int} 
            AND KANSOKU_TIME_INT = {kansoku_time_int} 
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 既存レコード件数を取得
        record_count = results[0][0]

        # 既存レコード件数が0の場合、INSERTする
        if record_count == 0:
            
            # INSERT文を作成する
            sql = 'INSERT INTO ' + ORIGIN_TABLE_NAME + ' VALUES('
            for value in row:
                sql = sql + str(value)
                sql += ','

            # 観測日時をINSERT文に追加する
            sql = sql + "'" + kansoku_datetime + "'"
            sql += ','

            # 更新日時をINSERT文に追加する
            sql = sql  + "'" + update_datetime + "'"
            sql += ')'

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"Inserted into {ORIGIN_TABLE_NAME} : {row}")

        # 既存レコード件数が1の場合、UPDATEする
        elif record_count == 1 :

            # UPDATEするデータ項目を取得
            denryu_1 = row[3]
            denatsu_1 = row[4]
            hatsuden_1 = row[5]
            denryu_2 = row[6]
            denatsu_2 = row[7]
            hatsuden_2 = row[8]
            denryu_3 = row[9]
            denatsu_3 = row[10]
            hatsuden_3 = row[11]
            denryu_4 = row[12]
            denatsu_4 = row[13]
            hatsuden_4 = row[14]
            denryu_5 = row[15]
            denatsu_5 = row[16]
            hatsuden_5 = row[17]
            denryu_6 = row[18]
            denatsu_6 = row[19]
            hatsuden_6 = row[20]
            denryu_7 = row[21]
            denatsu_7 = row[22]
            hatsuden_7 = row[23]
            denryu_8 = row[24]
            denatsu_8 = row[25]
            hatsuden_8 = row[26]
            nissya = row[27]
            temp = row[28]
            error_cd = row[29]
            baiden = row[30]

            # UPDATE文を作成する
            sql = ""
            sql = f"""
                    UPDATE {ORIGIN_TABLE_NAME}
                    SET
                        DENRYU_01 = {denryu_1}
                        , DENATSU_01 = {denatsu_1}
                        , HATSUDEN_01_kWH = {hatsuden_1}
                        , DENRYU_02 = {denryu_2}
                        , DENATSU_02 = {denatsu_2}
                        , HATSUDEN_02_kWH = {hatsuden_2}
                        , DENRYU_03 = {denryu_3}
                        , DENATSU_03 = {denatsu_3}
                        , HATSUDEN_03_kWH = {hatsuden_3}
                        , DENRYU_04 = {denryu_4}
                        , DENATSU_04 = {denatsu_4}
                        , HATSUDEN_04_kWH = {hatsuden_4}
                        , DENRYU_05 = {denryu_5}
                        , DENATSU_05 = {denatsu_5}
                        , HATSUDEN_05_kWH = {hatsuden_5}
                        , DENRYU_06 = {denryu_6}
                        , DENATSU_06 = {denatsu_6}
                        , HATSUDEN_06_kWH = {hatsuden_6}
                        , DENRYU_07 = {denryu_7}
                        , DENATSU_07 = {denatsu_7}
                        , HATSUDEN_07_kWH = {hatsuden_7}
                        , DENRYU_08 = {denryu_8}
                        , DENATSU_08 = {denatsu_8}
                        , HATSUDEN_08_kWH = {hatsuden_8}
                        , NISSYA = {nissya}
                        , TEMP = {temp}
                        , ERROR_CD = {error_cd}
                        , BAIDEN = {baiden}
                        , KANSOKU_DATETIME = '{kansoku_datetime}'
                        , UPDATE_DATETIME  = '{update_datetime}'
            """
            
            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Update {ORIGIN_TABLE_NAME} : {row}")


def db_UpdateInsertHourlyTable(insert_list):
    
    """
    db_UpdateInsertHourlyTable 関数

    概要:
        時間別テーブルにデータをUPDATEまたはINSERTする。

    引数:
        insert_list (list): INSERTまたはUPDATEするデータを含むリスト。
            - 観測日
            - 観測時間
            - 1-7番目の電力
            - 1-7番目の電圧
            - 1-7番目の発電量
            - 1日の平均値
            - 平均値
            - バイデンhourly
            - バイデン
            - 観測日時
            - 更新日時

    戻り値:
        なし

    例外処理:
        データベース操作中にエラーが発生した場合は、エラーログを出力する。
    """

    try :
        kansoku_date_int = insert_list[0]
        kansoku_time_int = insert_list[1]

        # DBに接続
        my_conn,my_cursor = db_init()

        # 時間別テーブルに対して該当日時の件数を取得
        sql = ''
        sql = f"""
        SELECT
            COUNT(*) 
        FROM
            {HOURLY_TABLE_NAME}
        WHERE
            KANSOKU_DATE_INT = '{kansoku_date_int}' 
            And KANSOKU_TIME_INT = '{kansoku_time_int}'
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 既存レコード件数を取得
        record_count = results[0][0]
        
        # 既存レコード件数が0の場合、INSERTする
        if record_count == 0:
            
            # INSERT文を作成する
            sql = 'INSERT INTO ' + HOURLY_TABLE_NAME + ' VALUES('
            for value in insert_list:
                sql = sql + str(value)
                sql += ','

            # 最後のカンマを削除して)で閉じる
            sql = sql[:-1] + ')'

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"Inserted into {HOURLY_TABLE_NAME} : {insert_list}")

        # 既存レコード件数が1の場合、UPDATEする
        elif record_count == 1 :

            kansoku_date_int = insert_list[0]
            kansoku_time_int = insert_list[1]
            denryu_1 = insert_list[2]
            denatsu_1 = insert_list[3]
            hatsuden_1 = insert_list[4]
            denryu_2 = insert_list[5]
            denatsu_2 = insert_list[6]
            hatsuden_2 = insert_list[7]
            denryu_3 = insert_list[8]
            denatsu_3 = insert_list[9]
            hatsuden_3 = insert_list[10]
            denryu_4 = insert_list[11]
            denatsu_4 = insert_list[12]
            hatsuden_4 = insert_list[13]
            denryu_5 = insert_list[14]
            denatsu_5 = insert_list[15]
            hatsuden_5 = insert_list[16]
            denryu_6 = insert_list[17]
            denatsu_6 = insert_list[18]
            hatsuden_6 = insert_list[19]
            denryu_7 = insert_list[20]
            denatsu_7 = insert_list[21]
            hatsuden_7 = insert_list[22]
            nissya = insert_list[23]
            temp = insert_list[24]
            baiden_hourly = insert_list[25]
            baiden = insert_list[26]
            kansoku_datetime = insert_list[27]
            update_datetime = insert_list[28]

            # UPDATE文を作成する
            sql = ""
            sql = f"""
            UPDATE {HOURLY_TABLE_NAME}
            SET
                DENRYU_01 = {denryu_1}
                , DENATSU_01 = {denatsu_1}
                , HATSUDEN_01_kWH = {hatsuden_1}
                , DENRYU_02 = {denryu_2}
                , DENATSU_02 = {denatsu_2}
                , HATSUDEN_02_kWH = {hatsuden_2}
                , DENRYU_03 = {denryu_3}
                , DENATSU_03 = {denatsu_3}
                , HATSUDEN_03_kWH = {hatsuden_3}
                , DENRYU_04 = {denryu_4}
                , DENATSU_04 = {denatsu_4}
                , HATSUDEN_04_kWH = {hatsuden_4}
                , DENRYU_05 = {denryu_5}
                , DENATSU_05 = {denatsu_5}
                , HATSUDEN_05_kWH = {hatsuden_5}
                , DENRYU_06 = {denryu_6}
                , DENATSU_06 = {denatsu_6}
                , HATSUDEN_06_kWH = {hatsuden_6}
                , DENRYU_07 = {denryu_7}
                , DENATSU_07 = {denatsu_7}
                , HATSUDEN_07_kWH = {hatsuden_7}
                , NISSYA_AVG = {nissya}
                , TEMP_AVG = {temp}
                , BAIDEN_HOURLY = {baiden_hourly}
                , BAIDEN = {baiden}
                , KANSOKU_DATETIME = {kansoku_datetime}
                , UPDATE_DATETIME = {update_datetime}
            WHERE
                KANSOKU_DATE_INT = {kansoku_date_int}
                And KANSOKU_TIME_INT = {kansoku_time_int}
            """
            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Update {HOURLY_TABLE_NAME} : {insert_list}")

        # DBの接続を閉じる
        db_close(my_cursor,my_conn)

    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in db_UpdateInsertHourlyTable: {str(e)}"
        file_util.write_log(error_message)

def db_UpdateInsertDailyTable(oha_yyyymmdd):
    
    """
    db_UpdateInsertDailyTable 関数

    概要:
        日別テーブルにデータをUPDATEまたはINSERTする。

    引数:
        oha_yyyymmdd (int): 観測日を表すYYYYMMDD形式の整数。

    戻り値:
        なし

    例外処理:
        データベース操作中にエラーが発生した場合は、エラーログを出力する。
    """

    try:
        # DBに接続
        my_conn,my_cursor = db_init()

        # 日別テーブルに対して該当日の件数を取得
        sql = ""
        sql = f"""
        SELECT
            COUNT(*) 
        FROM
            {DAILY_TABLE_NAME}
        WHERE
            KANSOKU_DATE_INT = {oha_yyyymmdd}
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 既存レコード件数を取得
        record_count = results[0][0]

        # 時間別テーブルのデータを集計するSQLを作成
        sql = ""
        sql = f"""
        SELECT
            KANSOKU_DATE_INT,
            MAX(
                CASE
                    WHEN KANSOKU_TIME_INT = (
                        SELECT MAX(KANSOKU_TIME_INT)
                        FROM {HOURLY_TABLE_NAME}
                        WHERE KANSOKU_DATE_INT = {oha_yyyymmdd}
                    )
                    THEN BAIDEN
                    ELSE 0
                END
            ) - MAX(
                CASE
                    WHEN KANSOKU_TIME_INT = (
                        SELECT MIN(KANSOKU_TIME_INT)
                        FROM {HOURLY_TABLE_NAME}
                        WHERE KANSOKU_DATE_INT = {oha_yyyymmdd}
                    )
                    THEN BAIDEN
                    ELSE 0
                END
            ) AS BAIDEN_DAILY,
            (
                SELECT MAX(BAIDEN)
                FROM {HOURLY_TABLE_NAME}
                WHERE KANSOKU_TIME_INT = (
                    SELECT MIN(KANSOKU_TIME_INT)
                    FROM {HOURLY_TABLE_NAME}
                    WHERE KANSOKU_DATE_INT = {oha_yyyymmdd}
                )
                AND KANSOKU_DATE_INT = {oha_yyyymmdd}
            ) AS BAIDEN_00,
            (
                SELECT MAX(BAIDEN)
                FROM {HOURLY_TABLE_NAME}
                WHERE KANSOKU_TIME_INT = (
                    SELECT MAX(KANSOKU_TIME_INT)
                    FROM {HOURLY_TABLE_NAME}
                    WHERE KANSOKU_DATE_INT = {oha_yyyymmdd}
                )
                AND KANSOKU_DATE_INT = {oha_yyyymmdd}
            ) AS BAIDEN_23
        FROM
            {HOURLY_TABLE_NAME}
        WHERE
            KANSOKU_DATE_INT = {oha_yyyymmdd}
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 結果セットを変数に格納
        kansoku_date_int = results[0][0] # 観測日
        baiden_daily = results[0][1] # 1日あたりの売電量
        baiden_00 = results[0][2] # 該当日の0時時点の売電量
        baiden_23 = results[0][3] # 該当日の23時時点の売電量

        # 既存レコード件数が0の場合、INSERTする
        if record_count == 0 :

            # INSERT文を作成        
            sql = ""
            sql = f""" INSERT INTO {DAILY_TABLE_NAME} VALUES(
                {kansoku_date_int} , {baiden_daily} , {baiden_00} , {baiden_23} ,
                '{oha_yyyymmdd}' , '{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))}'
            )"""

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"Inserted into {HOURLY_TABLE_NAME} : {results[0]}")

        # 既存レコード件数が1の場合、UPDATEする
        elif record_count == 1 :

            # UPDATE文を作成        
            sql = ""
            sql = f"""
            UPDATE
                {DAILY_TABLE_NAME} 
            SET
                BAIDEN_DAILY = {baiden_daily}
                , BAIDEN_00 = {baiden_00}
                , BAIDEN_23 = {baiden_23}
            WHERE
                KANSOKU_DATE_INT = {oha_yyyymmdd};
            """

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Update {DAILY_TABLE_NAME} : {results[0]}")

        # DBの接続を閉じる
        db_close(my_cursor,my_conn)
    
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in db_UpdateInsertDailyTable: {str(e)}"
        file_util.write_log(error_message)

def db_UpdateInsertMonthlyTable(oha_yyyymm):

    """
    db_UpdateInsertMonthlyTable 関数

    概要:
        月別テーブルにデータをUPDATEまたはINSERTする。

    引数:
        oha_yyyymm (int): 観測月を表すYYYYMM形式の整数。

    戻り値:
        なし

    例外処理:
        データベース操作中にエラーが発生した場合は、エラーログを出力する。
    """

    try:
        # DBに接続
        my_conn,my_cursor = db_init()

        # 月別テーブルに対して該当月の件数を取得
        sql = ""
        sql = f"""
        SELECT
            COUNT(*) 
        FROM
            {MONTHLY_TABLE_NAME}
        WHERE KANSOKU_MONTH_INT = {oha_yyyymm}
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 既存レコード件数を取得
        record_count = results[0][0]

        # 時間別テーブルのデータを集計するSQLを作成
        sql = ""
        sql = f"""
        SELECT
            {oha_yyyymm} AS KANSOKU_MONTH_INT
            ,MAX(( 
            SELECT
                BAIDEN_23 
            FROM
                {DAILY_TABLE_NAME}
            WHERE
                YEAR (KANSOKU_DATE) = {int(str(oha_yyyymm)[0:4])} 
                AND MONTH (KANSOKU_DATE) = {int(str(oha_yyyymm)[5:6])}
            ORDER BY
                KANSOKU_DATE_INT DESC 
            LIMIT
                1
            ) - ( 
            SELECT
                BAIDEN_00 
            FROM
                {DAILY_TABLE_NAME} 
            WHERE
                YEAR (KANSOKU_DATE) = {int(str(oha_yyyymm)[0:4])} 
                AND MONTH (KANSOKU_DATE) = {int(str(oha_yyyymm)[5:6])} 
            ORDER BY
                KANSOKU_DATE_INT ASC 
            LIMIT
                1
        )) AS BAIDEN_MONTHLY 
        FROM
            {DAILY_TABLE_NAME}
        WHERE
            YEAR (KANSOKU_DATE) = {int(str(oha_yyyymm)[0:4])}
            AND MONTH (KANSOKU_DATE) = {int(str(oha_yyyymm)[5:6])}
        """

        # SQLクエリを実行し、結果を取得
        my_cursor.execute(sql)
        results = my_cursor.fetchall()

        # 結果セットを変数に格納
        kansoku_month_int = results[0][0] # 観測月
        baiden_monthly = results[0][1] # 1か月あたりの売電量

        # 既存レコード件数が0の場合、INSERTする
        if record_count == 0 :

            # INSERT文を作成        
            sql = ""
            sql = f"""
            INSERT
            INTO {MONTHLY_TABLE_NAME}
            VALUES ({kansoku_month_int}, {baiden_monthly} ,'{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))}')
            """

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"Inserted into {MONTHLY_TABLE_NAME} : {results[0]}")

        # 既存レコード件数が1の場合、UPDATEする
        elif record_count == 1 :

            # UPDATE文を作成        
            sql = ""
            sql = f"""
            UPDATE {MONTHLY_TABLE_NAME} 
            SET
                BAIDEN_MONTHLY = {baiden_monthly}
            WHERE
            KANSOKU_MONTH_INT = {oha_yyyymm}
            """

            # SQLを実行する
            my_cursor.execute(sql)
            my_conn.commit()

            # ログを出力する
            file_util.write_log(f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Update {MONTHLY_TABLE_NAME} : {results[0]}") 

        # DBの接続を閉じる
        db_close(my_cursor,my_conn)

    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in db_UpdateInsertMonthlyTable: {str(e)}"
        file_util.write_log(error_message)