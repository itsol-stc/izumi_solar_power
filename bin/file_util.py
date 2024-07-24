# -----------------------------------------------------------------------------
# 会社名: 湘南技術センター株式会社
# 名前: 塩見 和則
# 作成日: 2024/7/11
# -----------------------------------------------------------------------------

# 標準ライブラリ
import os
import datetime as dt

# 外部ライブラリ
import file_util

# ログディレクトリ
LOG_DIR = "../log"

# 最大ログファイル数
MAX_LOG_FILES = 366  

def getOneHourAgoDate():

    """
    getOneHourAgoDate 関数

    概要:
        現在時刻から1時間前の日時情報を計算し、それを複数のフォーマットで返す関数。

    処理内容:
        1. 現在の年月日時分を取得する。
        2. 現在時刻から1時間前の日時を計算する。
        3. 複数のフォーマットで日時情報を変換する。

    引数:
        なし

    戻り値:
        oha_date: str - 日時の文字列 (YYYY-MM-DD HH:MM)
        oha_yymmddhh: str - YYMMddhh 形式の日時文字列
        oha_yyyymmdd: str - YYYYMMdd 形式の日付文字列
        oha_yyyymm: str - YYYYMM 形式の年月文字列
        oha_year: str - YYYY 形式の年文字列
        oha_month: str - MM 形式の月文字列

    例外処理:
        エラーが発生した場合、エラーログを出力する。
    """

    try:
        # 現在の年月日時分を取得
        current_datetime = dt.datetime.now()

        # 現在から1時間前の日時を計算
        oha = current_datetime - dt.timedelta(hours=1)

        # 取得した年月日時分のフォーマットを変換
        oha_yymmddhh = oha.strftime('%y%m%d%H') # YYMMddhh形式に変換
        oha_yyyymmdd= oha.strftime('%Y%m%d') # YYYYMMdd形式に変換
        oha_yyyymm= oha.strftime('%Y%m') # YYYYMM形式に変換
        oha_year = oha.strftime('%Y') # YYYY形式に変換
        oha_month = oha.strftime('%m') # mm形式に変換

        # 日時型のデータを作成
        year = int('20' + oha_yymmddhh[:2])  # 年を取得（YYを補完して年とする）
        month = int(oha_yymmddhh[2:4])  # 月を取得
        day = int(oha_yymmddhh[4:6])    # 日を取得
        hour = int(oha_yymmddhh[6:8])   # 時を取得
        oha_date_time = dt.datetime(year, month, day, hour)  # 日付と時刻を作成
        oha_date = oha_date_time.strftime('%Y-%m-%d %H:%M') # 日時型で出力
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in getOneHourAgoDate: {str(e)}"
        file_util.write_log(error_message)

    return oha_date,oha_yymmddhh,oha_yyyymmdd,oha_yyyymm,oha_year,oha_month

def getInsertDataListForDailyTable(df,date):

    """
    getInsertDataListForDailyTable 関数

    概要:
        pandas の DataFrame を元に、izumi_sola_daily テーブルへの INSERT 用リストを作成する関数。

    処理内容:
        1. izumi_sola_daily テーブルに INSERT するためのリストを初期化する。
        2. DataFrame の特定の列を取得し、リストに追加する。
        3. 数値列の平均値を計算し、リストに追加する。
        4. 日時情報をリストに追加する。

    引数:
        df: pandas.DataFrame - CSV ファイルを読み込んだ DataFrame
        date: str - 観測日時の文字列

    戻り値:
        temp_list: list - INSERT 用のデータが格納されたリスト

    例外処理:
        エラーが発生した場合、エラーログを出力する。
    """

    try:
        # izumi_sola_dailyテーブルにINSERTするリストを作成
        temp_list = []
        temp_list.append(df.loc[0,1]) #列1(日付)
        temp_list.append(str(df.loc[0,2])[:-2]) #列2(時間) 頭2桁を抜き出す
        temp_list.append(round(df.iloc[:,3].mean(),4)) #列3(01-電流の平均値)
        temp_list.append(round(df.iloc[:,4].mean(),4)) #列4(01-電圧の平均値)
        temp_list.append(round(df.iloc[:,5].mean()/1000,4))#列5(01-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,6].mean(),4)) #列6(02-電流の平均値)
        temp_list.append(round(df.iloc[:,7].mean(),4)) #列7(02-電圧の平均値)
        temp_list.append(round(df.iloc[:,8].mean()/1000,4)) #列8(02-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,9].mean(),4)) #列9(03-電流の平均値)
        temp_list.append(round(df.iloc[:,10].mean(),4)) #列10(03-電圧の平均値)
        temp_list.append(round(df.iloc[:,11].mean()/1000,4)) #列11(03-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,12].mean(),4)) #列12(04-電流の平均値)
        temp_list.append(round(df.iloc[:,13].mean(),4)) #列13(04-電圧の平均値)
        temp_list.append(round(df.iloc[:,14].mean()/1000,4)) #列14(04-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,15].mean(),4)) #列15(05-電流の平均値)
        temp_list.append(round(df.iloc[:,16].mean(),4)) #列16(05-電圧の平均値)
        temp_list.append(round(df.iloc[:,17].mean()/1000,4)) #列17(05-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,18].mean(),4)) #列18(06-電流の平均値)
        temp_list.append(round(df.iloc[:,19].mean(),4)) #列19(06-電圧の平均値)
        temp_list.append(round(df.iloc[:,20].mean()/1000,4)) #列20(06-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,21].mean(),4)) #列21(07-電流の平均値)
        temp_list.append(round(df.iloc[:,22].mean(),4)) #列22(07-電圧の平均値)
        temp_list.append(round(df.iloc[:,23].mean()/1000,4)) #列23(07-発電量[kWH]の平均値)
        temp_list.append(round(df.iloc[:,27].mean(),4)) #列24(日射量の平均値)
        temp_list.append(round(df.iloc[:,28].mean(),4)) #列25(温度の平均値)
        temp_list.append(df[30][df.shape[0]-1] - df[30][0]) #列26(1時間あたりの売電量[kWH])
        temp_list.append(df[30][df.shape[0]-1]) #列27(売電量[kWH])
        temp_list.append("'" + date + "'") #列28(観測日時)
        temp_list.append("'" + str(dt.datetime.now().strftime('%Y-%m-%d %H:%M')) + "'") #列29(更新日時)
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in getInsertDataListForDailyTable: {str(e)}"
        file_util.write_log(error_message)

    return temp_list

def create_log_directory():

    """
    create_log_directory 関数

    概要:
        ログディレクトリを作成する関数。

    処理内容:
        1. ログディレクトリが存在しない場合、作成する。

    引数:
        なし

    戻り値:
        なし

    例外処理:
        エラーが発生した場合、エラーログを出力する。
    """

    try:
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} Error in create_log_directory: {str(e)}"
        file_util.write_log(error_message)

def write_log(log_content):

    """
    write_log 関数

    概要:
        ログファイルにログを書き込む関数。

    処理内容:
        1. 現在の日付を基にログファイルのパスを生成し、ログを追記する。
        2. ログファイルの管理を行う。

    引数:
        log_content: str - 書き込むログの内容

    戻り値:
        なし

    例外処理:
        エラーが発生した場合、エラーログを出力する。
    """

    try:
        log_filename = dt.datetime.now().strftime("%Y%m%d.log")
        log_path = os.path.join(LOG_DIR, log_filename)
        with open(log_path, "a") as log_file:
            log_file.write(log_content + "\n")
        manage_log_files()
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} write_log: {str(e)}"
        file_util.write_log(error_message)

def manage_log_files():

    """
    manage_log_files 関数

    概要:
        ログファイルの管理を行う関数。

    処理内容:
        1. ログディレクトリ内のログファイルを取得し、最大ログファイル数を超えた場合、古いファイルを削除する。

    引数:
        なし

    戻り値:
        なし

    例外処理:
        エラーが発生した場合、エラーログを出力する。
    """
    
    try:
        # ログディレクトリ内のログファイルを取得
        log_files = sorted([f for f in os.listdir(LOG_DIR) if f.endswith('.log')])

        # ファイル数が最大値を超えた場合、古いファイルから削除する
        if len(log_files) > MAX_LOG_FILES:
            files_to_delete = len(log_files) - MAX_LOG_FILES
            for i in range(files_to_delete):
                file_to_delete = os.path.join(LOG_DIR, log_files[i])
                os.remove(file_to_delete)
    except Exception as e:
        # エラーログを出力する
        error_message = f"{str(dt.datetime.now().strftime('%Y-%m-%d %H:%M'))} manage_log_files: {str(e)}"
        file_util.write_log(error_message)
