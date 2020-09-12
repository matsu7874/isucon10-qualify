from os import getenv, path
import json
import subprocess
from io import StringIO
import csv
import flask
from werkzeug.exceptions import BadRequest, NotFound
import mysql.connector
from sqlalchemy.pool import QueuePool
from humps import camelize # snake_case を camelCase に変換するものらしい
from bisect import bisect_left

# この定数が何なのか気になる
LIMIT = 20
NAZOTTE_LIMIT = 50

chair_search_condition = json.load(open("../fixture/chair_condition.json", "r"))
estate_search_condition = json.load(open("../fixture/estate_condition.json", "r"))

app = flask.Flask(__name__)

mysql_connection_env = {
    "host": getenv("MYSQL_HOST", "127.0.0.1"),
    "port": getenv("MYSQL_PORT", 3306),
    "user": getenv("MYSQL_USER", "isucon"),
    "password": getenv("MYSQL_PASS", "isucon"),
    "database": getenv("MYSQL_DBNAME", "isuumo"),
}

# pool_size=10っていうのは良い値なのか？
cnxpool = QueuePool(lambda: mysql.connector.connect(**mysql_connection_env), pool_size=10)

CHAIR_COLORS = {
    "黒": 0,
    "白": 1,
    "赤": 2,
    "青": 3,
    "緑": 4,
    "黄": 5,
    "紫": 6,
    "ピンク": 7,
    "オレンジ": 8,
    "水色": 9,
    "ネイビー": 10,
    "ベージュ": 11,
}

CHIAR_FEATURES = {
    "ヘッドレスト付き": 0,
    "肘掛け付き": 1,
    "キャスター付き": 2,
    "アーム高さ調節可能": 3,
    "リクライニング可能": 4,
    "高さ調節可能": 5,
    "通気性抜群": 6,
    "メタルフレーム": 7,
    "低反発": 8,
    "木製": 9,
    "背もたれつき": 10,
    "回転可能": 11,
    "レザー製": 12,
    "昇降式": 13,
    "デザイナーズ": 14,
    "金属製": 15,
    "プラスチック製": 16,
    "法事用": 17,
    "和風": 18,
    "中華風": 19,
    "西洋風": 20,
    "イタリア製": 21,
    "国産": 22,
    "背もたれなし": 23,
    "ラテン風": 24,
    "布貼地": 25,
    "スチール製": 26,
    "メッシュ貼地": 27,
    "オフィス用": 28,
    "料理店用": 29,
    "自宅用": 30,
    "キャンプ用": 31,
    "クッション性抜群": 32,
    "モーター付き": 33,
    "ベッド一体型": 34,
    "ディスプレイ配置可能": 35,
    "ミニ机付き": 36,
    "スピーカー付属": 37,
    "中国製": 38,
    "アンティーク": 39,
    "折りたたみ可能": 40,
    "重さ500g以内": 41,
    "24回払い無金利": 42,
    "現代的デザイン": 43,
    "近代的なデザイン": 44,
    "ルネサンス的なデザイン": 45,
    "アームなし": 46,
    "オーダーメイド可能": 47,
    "ポリカーボネート製": 48,
    "フットレスト付き": 49,
}

CHAIR_KINDS = {
    "ゲーミングチェア": 0,
    "座椅子": 1,
    "エルゴノミクス": 2,
    "ハンモック": 3,
}

ESTATE_FEATURES = {
    "最上階": 0,
    "防犯カメラ": 1,
    "ウォークインクローゼット": 2,
    "ワンルーム": 3,
    "ルーフバルコニー付": 4,
    "エアコン付き": 5,
    "駐輪場あり": 6,
    "プロパンガス": 7,
    "駐車場あり": 8,
    "防音室": 9,
    "追い焚き風呂": 10,
    "オートロック": 11,
    "即入居可": 12,
    "IHコンロ": 13,
    "敷地内駐車場": 14,
    "トランクルーム": 15,
    "角部屋": 16,
    "カスタマイズ可": 17,
    "DIY可": 18,
    "ロフト": 19,
    "シューズボックス": 20,
    "インターネット無料": 21,
    "地下室": 22,
    "敷地内ゴミ置場": 23,
    "管理人有り": 24,
    "宅配ボックス": 25,
    "ルームシェア可": 26,
    "セキュリティ会社加入済": 27,
    "メゾネット": 28,
    "女性限定": 29,
    "バイク置場あり": 30,
    "エレベーター": 31,
    "ペット相談可": 32,
    "洗面所独立": 33,
    "都市ガス": 34,
    "浴室乾燥機": 35,
    "インターネット接続可": 36,
    "テレビ・通信": 37,
    "専用庭": 38,
    "システムキッチン": 39,
    "高齢者歓迎": 40,
    "ケーブルテレビ": 41,
    "床下収納": 42,
    "バス・トイレ別": 43,
    "駐車場2台以上": 44,
    "楽器相談可": 45,
    "フローリング": 46,
    "オール電化": 47,
    "TVモニタ付きインタホン": 48,
    "デザイナーズ物件": 49,
}

def select_all(query, *args, dictionary=True):
    cnx = cnxpool.connect()
    try:
        cur = cnx.cursor(dictionary=dictionary)
        cur.execute(query, *args)
        return cur.fetchall()
    finally:
        cnx.close()

# select_allしてからその先頭を返す
# いかにも効率が悪そう
# これって順序は保証されているんだろうか…？
def select_row(*args, **kwargs):
    rows = select_all(*args, **kwargs)
    return rows[0] if len(rows) > 0 else None


@app.route("/initialize", methods=["POST"])
def post_initialize():
    sql_dir = "../mysql/db"
    sql_files = [
        "0_Schema.sql",
        "1_DummyEstateData.sql",
        "2_DummyChairData.sql",
        "3_Create_search_estate.sql",
    ]

    for sql_file in sql_files:
        command = f"mysql -h {mysql_connection_env['host']} -u {mysql_connection_env['user']} -p{mysql_connection_env['password']} -P {mysql_connection_env['port']} {mysql_connection_env['database']} < {path.join(sql_dir, sql_file)}"
        subprocess.run(["bash", "-c", command])

    return {"language": "python"}

# estate = 土地
# LIMIT = 20 と上で定義されていたので20行取ってくる
@app.route("/api/estate/low_priced", methods=["GET"])
def get_estate_low_priced():
    rows = select_all("SELECT * FROM estate ORDER BY rent ASC, id ASC LIMIT %s", (LIMIT,))
    return {"estates": camelize(rows)}

# where stock > 0 なので在庫があるものを取ってくる
@app.route("/api/chair/low_priced", methods=["GET"])
def get_chair_low_priced():
    rows = select_all("SELECT * FROM chair WHERE stock > 0 ORDER BY price ASC, id ASC LIMIT %s", (LIMIT,))
    return {"chairs": camelize(rows)}

# 椅子の絞り込み条件検索
@app.route("/api/chair/search", methods=["GET"])
def get_chair_search():
    args = flask.request.args

    conditions = []
    params = []

    # このあたりのコード、個人的に読みづらい…
    # priceRangeId -> [min, max) みたいな対応付けがありそう
    # この下４つのrangeに関する処理は全部同じ
    if args.get("priceRangeId"):
        for _range in chair_search_condition["price"]["ranges"]:
            if _range["id"] == int(args.get("priceRangeId")):
                price = _range
                break
        else:
            raise BadRequest("priceRangeID invalid")
        if price["min"] != -1:
            conditions.append("price >= %s")
            params.append(price["min"])
        if price["max"] != -1:
            conditions.append("price < %s")
            params.append(price["max"])

    if args.get("heightRangeId"):
        for _range in chair_search_condition["height"]["ranges"]:
            if _range["id"] == int(args.get("heightRangeId")):
                height = _range
                break
        else:
            raise BadRequest("heightRangeId invalid")
        if height["min"] != -1:
            conditions.append("height >= %s")
            params.append(height["min"])
        if height["max"] != -1:
            conditions.append("height < %s")
            params.append(height["max"])

    if args.get("widthRangeId"):
        for _range in chair_search_condition["width"]["ranges"]:
            if _range["id"] == int(args.get("widthRangeId")):
                width = _range
                break
        else:
            raise BadRequest("widthRangeId invalid")
        if width["min"] != -1:
            conditions.append("width >= %s")
            params.append(width["min"])
        if width["max"] != -1:
            conditions.append("width < %s")
            params.append(width["max"])

    # 椅子のdepthってなんだ…？全長じゃなくて足の長さとかなのかな
    if args.get("depthRangeId"):
        for _range in chair_search_condition["depth"]["ranges"]:
            if _range["id"] == int(args.get("depthRangeId")):
                depth = _range
                break
        else:
            raise BadRequest("depthRangeId invalid")
        if depth["min"] != -1:
            conditions.append("depth >= %s")
            params.append(depth["min"])
        if depth["max"] != -1:
            conditions.append("depth < %s")
            params.append(depth["max"])

    # kind, color による完全一致絞り込み
    # もし文字列で行われていたとすると少しパフォーマンスが悪くなりそうなので
    # 整数に紐づけたほうがよさそう？
    if args.get("kind"):
        conditions.append("kind = %s")
        params.append(args.get("kind"))

    if args.get("color"):
        conditions.append("color = %s")
        params.append(args.get("color"))

    # features(特徴)による部分一致検索
    # 文字列検索だし LIKE 使っているのでSQLの改善余地がありそう
    if args.get("features"):
        for feature_confition in args.get("features").split(","):
            conditions.append("features LIKE CONCAT('%', %s, '%')")
            params.append(feature_confition)

    # 条件が未指定の場合、検索に失敗する
    if len(conditions) == 0:
        raise BadRequest("Search condition not found")

    # 在庫があるという条件は必須
    # stockが0になったらそもそもテーブルから落とせばいいのでは？
    conditions.append("stock > 0")

    try:
        page = int(args.get("page"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format page parameter")

    try:
        per_page = int(args.get("perPage"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format perPage parameter")

    search_condition = " AND ".join(conditions)

    # 最初に全件数を取得する
    query = f"SELECT COUNT(*) as count FROM chair WHERE {search_condition}"
    count = select_row(query, params)["count"]

    # (人気の降順, id昇順) にソート
    query = f"SELECT * FROM chair WHERE {search_condition} ORDER BY popularity DESC, id ASC LIMIT %s OFFSET %s"
    chairs = select_all(query, params + [per_page, per_page * page])

    return {"count": count, "chairs": camelize(chairs)}

# 検索条件の一覧を取得する?
@app.route("/api/chair/search/condition", methods=["GET"])
def get_chair_search_condition():
    return chair_search_condition

# ある特定の椅子の情報を取得
@app.route("/api/chair/<int:chair_id>", methods=["GET"])
def get_chair(chair_id):
    chair = select_row("SELECT * FROM chair WHERE id = %s", (chair_id,))
    if chair is None or chair["stock"] <= 0:
        raise NotFound()
    return camelize(chair)

# 椅子の購入
# 購入件数がスコアに反映される（金額は無し）
@app.route("/api/chair/buy/<int:chair_id>", methods=["POST"])
def post_chair_buy(chair_id):
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor(dictionary=True)
        cur.execute("SELECT * FROM chair WHERE id = %s AND stock > 0 FOR UPDATE", (chair_id,))
        chair = cur.fetchone()
        if chair is None:
            raise NotFound()
        cur.execute("UPDATE chair SET stock = stock - 1 WHERE id = %s", (chair_id,))
        cnx.commit()
        return {"ok": True}
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()

# 不動産の絞り込み検索
# ロジックは椅子の絞り込み検索と全く同様(=椅子の課題を解決できればこちらも改善につながる)
@app.route("/api/estate/search", methods=["GET"])
def get_estate_search():
    args = flask.request.args

    conditions = []
    params = []

    if args.get("doorHeightRangeId"):
        door_height = None
        for _range in estate_search_condition["doorHeight"]["ranges"]:
            if _range["id"] == int(args.get("doorHeightRangeId")):
                door_height = _range
                break
        if door_height is None:
            raise BadRequest("doorHeightRangeId invalid")
        conditions.append("dh_idx = %s")
        params.append(door_height["id"])

    if args.get("doorWidthRangeId"):
        door_width = None
        for _range in estate_search_condition["doorWidth"]["ranges"]:
            if _range["id"] == int(args.get("doorWidthRangeId")):
                door_width = _range
                break
        if door_width is None:
            raise BadRequest("doorWidthRangeId invalid")
        conditions.append("dw_idx = %s")
        params.append(door_width["id"])

    if args.get("rentRangeId"):
        rent = None
        for _range in estate_search_condition["rent"]["ranges"]:
            if _range["id"] == int(args.get("rentRangeId")):
                rent = _range
        if rent is None:
            raise BadRequest("rentRangeId invalid")
        conditions.append("rent_idx = %s")
        params.append(rent["id"])

    if args.get("features"):
        for feature_confition in args.get("features").split(","):
            conditions.append("features LIKE CONCAT('%', %s, '%')")
            params.append(feature_confition)

    if len(conditions) == 0:
        raise BadRequest("Search condition not found")

    try:
        page = int(args.get("page"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format page parameter")

    try:
        per_page = int(args.get("perPage"))
    except (TypeError, ValueError):
        raise BadRequest("Invalid format perPage parameter")

    search_condition = " AND ".join(conditions)

    query = f"SELECT COUNT(*) as count FROM _search_estate WHERE {search_condition}"
    count = select_row(query, params)["count"]

    # 人気降順にソート
    query = f'''
        SELECT
            es.*
        FROM (SELECT *
            FROM _search_estate
            WHERE {search_condition}
            ORDER BY popularity DESC, id ASC
            LIMIT %s
            OFFSET %s) as sc
        INNER JOIN estate es USING (id)
    '''
    estates = select_all(query, params + [per_page, per_page * page])

    return {"count": count, "estates": camelize(estates)}


@app.route("/api/estate/search/condition", methods=["GET"])
def get_estate_search_condition():
    return estate_search_condition

# 不動産に関しては資料請求数がスコアに繋がる
# わざわざSQL叩かなくても良いような気がする(その不動産idが存在することさえ分かればよいので)
@app.route("/api/estate/req_doc/<int:estate_id>", methods=["POST"])
def post_estate_req_doc(estate_id):
    estate = select_row("SELECT * FROM estate WHERE id = %s", (estate_id,))
    if estate is None:
        raise NotFound()
    return {"ok": True}

# nazotte とは？(そのような英語は無い) 不動産に関連している
# このAPIが重いらしい(timeoutしている)
@app.route("/api/estate/nazotte", methods=["POST"])
def post_estate_nazotte():
    # coordinates = 座標に関する指定が必須
    if "coordinates" not in flask.request.json:
        raise BadRequest()
    coordinates = flask.request.json["coordinates"]
    if len(coordinates) == 0:
        raise BadRequest()

    # longitude = 経度, latitude = 緯度
    # 複数の地点の(経度, 緯度)情報が与えられている
    longitudes = [c["longitude"] for c in coordinates]
    latitudes = [c["latitude"] for c in coordinates]

    # 与えられた点全てを囲むような長方形
    bounding_box = {
        "top_left_corner": {"longitude": min(longitudes), "latitude": min(latitudes)},
        "bottom_right_corner": {"longitude": max(longitudes), "latitude": max(latitudes)},
    }

    cnx = cnxpool.connect()
    try:
        cur = cnx.cursor(dictionary=True)

        # bounding_box に含まれる全ての不動産を取得(人気降順)
        cur.execute(
            (
                "SELECT * FROM estate"
                " WHERE latitude <= %s AND latitude >= %s AND longitude <= %s AND longitude >= %s"
                " ORDER BY popularity DESC, id ASC"
            ),
            (
                bounding_box["bottom_right_corner"]["latitude"],
                bounding_box["top_left_corner"]["latitude"],
                bounding_box["bottom_right_corner"]["longitude"],
                bounding_box["top_left_corner"]["longitude"],
            ),
        )
        estates = cur.fetchall() # 上とまとめてselect_allで良さそう(どうでもよいが)

        # 上で得た不動産を何かの条件でさらに絞り込んでる？
        # あえて段階を踏んでいるのは何か理由がある？
        # polygon(多角形)に含まれる不動産のみを抽出しようとしてる(polygonとは？)
        estates_in_polygon = []
        for estate in estates:
            # ST_Contains(ST_PolygonFromText(%s), ST_GeomFromText(%s))
            # ↑これが何かよく分からない
            query = "SELECT * FROM estate WHERE id = %s AND ST_Contains(ST_PolygonFromText(%s), ST_GeomFromText(%s))"
            polygon_text = (
                # ここ、読みづらい書き方ですね（ワンライナーでおしゃれではある）
                # POLYGON(1 1,2 2,3 3) みたいな文字列になる
                f"POLYGON(({','.join(['{} {}'.format(c['latitude'], c['longitude']) for c in coordinates])}))"
            )
            geom_text = f"POINT({estate['latitude']} {estate['longitude']})"
            cur.execute(query, (estate["id"], polygon_text, geom_text))
            if len(cur.fetchall()) > 0:
                estates_in_polygon.append(estate)
    finally:
        cnx.close()

    results = {"estates": []}
    for i, estate in enumerate(estates_in_polygon):
        if i >= NAZOTTE_LIMIT: # NAZOTTE_LIMIT = 50
            break
        results["estates"].append(camelize(estate))
    results["count"] = len(results["estates"])
    return results

# 特定の不動産の情報を取得(by id)
@app.route("/api/estate/<int:estate_id>", methods=["GET"])
def get_estate(estate_id):
    estate = select_row("SELECT * FROM estate WHERE id = %s", (estate_id,))
    if estate is None:
        raise NotFound()
    return camelize(estate)

# 特定の椅子(by id)に対するオススメの不動産を取得(人気降順)
@app.route("/api/recommended_estate/<int:chair_id>", methods=["GET"])
def get_recommended_estate(chair_id):
    chair = select_row("SELECT * FROM chair WHERE id = %s", (chair_id,))
    if chair is None:
        raise BadRequest(f"Invalid format searchRecommendedEstateWithChair id : {chair_id}")
    w, h, d = chair["width"], chair["height"], chair["depth"]

    # このクエリ条件が多くて遅そう?
    # 椅子がドアを通るかどうかを判定している
    query = (
        "SELECT * FROM estate"
        " WHERE (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の幅, ドアの高さ >= 椅子の高さ
        "    OR (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の幅, ドアの高さ >= 椅子の奥行
        "    OR (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の高さ, ドアの高さ >= 椅子の幅
        "    OR (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の高さ, ドアの高さ >= 椅子の奥行
        "    OR (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の奥行, ドアの高さ >= 椅子の幅
        "    OR (door_width >= %s AND door_height >= %s)" # ドアの幅 >= 椅子の奥行, ドアの高さ >= 椅子の高さ
        # まず w, h, d をソートして w <= h <= d である状態にしておく、一番長いところは明らかに使わないほうが良いので
        # w, h だけ考えればよくなる
        # w <= h より
        # w >= min(door_width, door_height)
        # h >= max(door_width, dorr_height)
        # であることだけを確かめればよい？（要検証）
        " ORDER BY popularity DESC, id ASC"
        " LIMIT %s"
    )
    estates = select_all(query, (w, h, w, d, h, w, h, d, d, w, d, h, LIMIT))
    return {"estates": camelize(estates)}

# 新しい椅子の入稿
@app.route("/api/chair", methods=["POST"])
def post_chair():
    if "chairs" not in flask.request.files:
        raise BadRequest()
    records = csv.reader(StringIO(flask.request.files["chairs"].read().decode()))
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor()
        for record in records:
            query = "INSERT INTO chair(id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(query, record)
            query_search = """
                INSERT INTO _search_chair
                    (id, feature, popularity, stock, price_idx, h_idx, w_idx, d_idx, color_idx, kind_idx, feature_idx)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            bitset_feature = 0
            for feature_cond in record[9].split(','):
                if not(feature_cond in CHIAR_FEATURES):
                    continue
                bitset_feature |= 1 << CHIAR_FEATURES[feature_cond]
            hwd_limits = [80, 110, 150]
            price_limits = [3000, 6000, 9000, 15000]
            param = [
                record[0],
                record[9],
                record[11],
                record[12],
                bisect_left(price_limits, record[4]),
                bisect_left(hwd_limits, record[5]),
                bisect_left(hwd_limits, record[6]),
                bisect_left(hwd_limits, record[7]),
                CHAIR_COLORS(record[8]),
                CHAIR_KINDS(record[10]),
                bitset_feature
            ]
            cur.execute(query, record)
        cnx.commit()
        return {"ok": True}, 201
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()

# 新しい不動産の入稿
@app.route("/api/estate", methods=["POST"])
def post_estate():
    if "estates" not in flask.request.files:
        raise BadRequest()
    records = csv.reader(StringIO(flask.request.files["estates"].read().decode()))
    cnx = cnxpool.connect()
    try:
        cnx.start_transaction()
        cur = cnx.cursor()
        for record in records:
            query = "INSERT INTO estate(id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(query, record)
            query_search = """
                INSERT INTO _search_estate
                    (id, latitude, longitude, rent, door_height, door_width, feature, popularity, rent_idx, dh_idx, dw_idx, feature_idx)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            bitset_feature = 0
            for feature_cond in record[10].split(','):
                if not(feature_cond in ESTATE_FEATURES):
                    continue
                bitset_feature |= 1 << ESTATE_FEATURES[feature_cond]
            hwd_limits = [80, 110, 150]
            rent_limits = [50000, 100000, 150000]
            param = [
                record[0],
                record[5],
                record[6],
                record[7],
                record[8],
                record[9],
                record[10],
                record[11],
                bisect_left(price_limits, record[7]),
                bisect_left(hwd_limits, record[8]),
                bisect_left(hwd_limits, record[9]),
                bitset_feature
            ]
            cur.execute(query, record)
        cnx.commit()
        return {"ok": True}, 201
    except Exception as e:
        cnx.rollback()
        raise e
    finally:
        cnx.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=getenv("SERVER_PORT", 1323), debug=True, threaded=True)
