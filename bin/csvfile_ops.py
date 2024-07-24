# -----------------------------------------------------------------------------
# 会社名: 湘南技術センター株式会社
# 名前: 塩見 和則
# 作成日: 2024/7/11
# -----------------------------------------------------------------------------

# 標準ライブラリ
import ftplib
import os
import datetime as dt

# 外部ライブラリ
import file_util

# FTPサーバーの情報
ftp_server = "XXX.XXX.XXX.XXX"
username = "XXX"
password = "XXX"

def download_csv_file(directory,filename,local_directory):

    """
    download_csv_file 関数

    概要:
        FTPサーバーから指定されたディレクトリ内のCSVファイルをダウンロードする関数。

    処理内容:
        1. ローカルに保存するディレクトリを作成する（存在しない場合）。
        2. FTPサーバーに接続し、指定されたディレクトリに移動する。
        3. 指定されたCSVファイルをダウンロードして、ローカルに保存する。
        4. ダウンロードが成功した場合、ダウンロードしたファイルのパスをログに出力する。
        5. FTPサーバーとの接続を閉じる。

    引数:
        directory: str - ダウンロードするファイルのディレクトリパス
        filename: str - ダウンロードするファイル名
        local_directory: str - ローカルに保存するディレクトリパス

    戻り値:
        bool - ダウンロードの成功/失敗を示す真偽値

    例外処理:
        FTP接続やファイルのダウンロード中にエラーが発生した場合、エラーログを出力し、False を返す。
    """

    try:
        # ローカルに保存するディレクトリ
        os.makedirs(local_directory, exist_ok=True)

        # FTPサーバーに接続
        ftp = ftplib.FTP(ftp_server)
        ftp.login(user=username, passwd=password)
        ftp.cwd(directory)

        # 特定のCSVファイルをダウンロード
        local_file_path = os.path.join(local_directory, filename)
        with open(local_file_path, 'wb') as local_file:
            ftp.retrbinary(f"RETR {filename}", local_file.write)
        print(f"Downloaded: {filename}")

        # 接続を閉じる
        ftp.quit()
        
        # ログを出力する
        message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Downloaded: {local_directory+"/"+filename}"
        file_util.write_log(message)
    
        return True
    
    except ftplib.all_errors as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} FTP error: {str(e)}"
        file_util.write_log(error_message)
    
        return False
    
def delete_csv_file(file_path):

    """
    delete_csv_file 関数

    概要:
        指定されたパスのCSVファイルを削除する関数。

    処理内容:
        1. 指定されたパスのファイルが存在するか確認する。
        2. ファイルが存在する場合は削除し、削除したファイルのパスをログに出力する。
        3. ファイルが存在しない場合は、削除失敗のメッセージをログに出力する。

    引数:
        file_path: str - 削除するファイルのパス

    戻り値:
        なし

    例外処理:
        ファイルの削除中にエラーが発生した場合、エラーログを出力する。
    """

    try:
        # ファイルが存在するか確認
        if os.path.exists(file_path):
            # ファイルを削除する
            os.remove(file_path)
            # ログを出力する    
            message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Deleted: {file_path}"
            file_util.write_log(message)
        else:
            # ログを出力する
            message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Failed to delete: {file_path}"
            file_util.write_log(message)
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in delete_csv_file: {str(e)}"
        file_util.write_log(error_message)
