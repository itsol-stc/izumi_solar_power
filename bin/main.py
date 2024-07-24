# -----------------------------------------------------------------------------
# 会社名: 湘南技術センター株式会社
# 名前: 塩見 和則
# 作成日: 2024/7/11
# -----------------------------------------------------------------------------

# 標準ライブラリ
import pandas as pd
import datetime as dt

# 外部ライブラリ
import mysql_ops as mysql
import csvfile_ops as csv
import file_util

# CSVディレクトリパスの定義
LOCAL_CSV_DIRECTORY = "../tmp"
FTP_CSV_DIRECTORY = "/LOG"

def main():

    """
    main 関数

    概要:
        FTP サーバーからCSVファイルをダウンロードし、それをDBに登録する処理を行う。

    処理内容:
        1. ログディレクトリを作成する。
        2. FTPサーバーから直近1時間前のCSVファイルをダウンロードする。
        3. CSVファイルのダウンロードが成功した場合は、以下の処理を実行する:
            - CSVファイルをpandasで読み込む。
            - 元データテーブルにデータをUPDATE-INSERTする。
            - 時間別テーブルにINSERTするデータを取得する。
            - 時間別テーブルにデータをUPDATE-INSERTする。
            - 日別テーブルにデータを集計しUPDATE-INSERTする。
            - 月別テーブルにデータを集計しUPDATE-INSERTする。
            - ダウンロードしたCSVファイルを削除する。
        4. CSVファイルのダウンロードが失敗した場合は、エラーログを出力する。

    利用するライブラリ:
        - pandas: CSVファイルの読み込みに使用。
        - datetime: 日時の操作に使用。

    依存するモジュール:
        - mysql_ops: MySQLデータベース操作用のモジュール。
        - csvfile_ops: CSVファイルのダウンロードと削除用のモジュール。
        - file_util: ログディレクトリ作成、ログ出力、日時操作用のユーティリティモジュール。

    定数:
        - LOCAL_CSV_DIRECTORY: ローカルにダウンロードするCSVファイルの保存先ディレクトリ。
        - FTP_CSV_DIRECTORY: FTPサーバー上のCSVファイルの保存先ディレクトリ。

    使用方法:
        main 関数はスクリプトのエントリーポイントとして定義されており、直接実行することができる。

    例外処理:
        - CSVファイルのダウンロードやDBへの登録処理でエラーが発生した場合は、詳細なエラーメッセージをログに出力する。
    """

    # ログディレクトリを作成
    file_util.create_log_directory()

    # 1時間前の日時型データ、YYYYMMddhh形式データ、YYYYMMdd形式データ、
    # YYYYMM形式データ、YYYY形式データ、hh形式データを取得
    oha_date,oha_yymmddhh,oha_yyyymmdd,oha_yyyymm,oha_year,oha_month = file_util.getOneHourAgoDate()

    # FTPサーバーのディレクトリ情報を取得
    directory = f"{FTP_CSV_DIRECTORY}/{oha_year}"
    filename = f"{oha_yymmddhh}.CSV"

    # FTPサーバーからCSVファイルをダウンロードする
    csv_download_successful = csv.download_csv_file(directory,filename,LOCAL_CSV_DIRECTORY)

    # CSVファイルのダウンロードに成功した場合はDBに登録する
    if csv_download_successful == True:

        # ダウンロードしたCSVファイルのパスを取得
        csv_file = LOCAL_CSV_DIRECTORY + "/" + filename

        # CSVファイルをpandasで読み込む
        csv_df = pd.read_csv(csv_file,header=None)

        # 元データテーブルにINSERTする
        mysql.db_UpdateInsertOriginTable(csv_df,oha_yymmddhh)

        # 時間別テーブルにINSERTするリストを取得
        insert_data_list = file_util.getInsertDataListForDailyTable(csv_df,oha_date)

        # 作成したリストを時間別テーブルにUPDATE-INSERTする
        mysql.db_UpdateInsertHourlyTable(insert_data_list)

        # 時間別テーブルのデータを集計し日別テーブルにUPDATE-INSERTする
        mysql.db_UpdateInsertDailyTable(oha_yyyymmdd)

        # 日別テーブルのデータを集計し月別データにUPDATE-INSERTする
        mysql.db_UpdateInsertMonthlyTable(oha_yyyymm)

        # ダウンロードしたCSVファイルを削除
        csv.delete_csv_file(csv_file)

    else:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Failed to CSV Download"
        file_util.write_log(error_message)

if __name__ == '__main__':
    main()
