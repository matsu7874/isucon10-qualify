import csv
import unittest

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


def convert(features, hash):
    ret = 0
    for feature in features.strip(" ").strip("'").split(","):
        if feature == '':
            continue
        ret |= 1 << hash[feature]
    return ret

def insert_head_estate():
    return 'INSERT INTO isuumo.estate (id, thumbnail, name, latitude, longitude, address, rent, door_height, door_width, popularity, description, features) VALUES '
def insert_head_chair():
    return 'INSERT INTO isuumo.chair (id, thumbnail, name, price, height, width, depth, popularity, stock, color, description, features, kind) VALUES '
def insert_tail ():
    return ';'

def main():
    with open('1_DummyEstateData.sql', 'w') as fout:
        with open('1_DummyEstateData.txt') as fin:
            reader = csv.reader(fin, quotechar="'", delimiter='\t')
            fout.write(insert_head_estate())
            for i,row in enumerate(reader):
                if i%500 == 499:
                    fout.write(insert_tail())
                    fout.write(insert_head_estate())
                elif i > 0:
                    fout.write(',')
                row[11] = str(convert(row[11], ESTATE_FEATURES))
                row_str = "('" + "','".join(row) + "')"
                fout.write(row_str)
            fout.write(insert_tail())
    with open('2_DummyChairData.sql', 'w') as fout:
        with open('2_DummyChairData.txt') as fin:
            reader = csv.reader(fin, quotechar="'", delimiter='\t')
            fout.write(insert_head_chair())
            for i,row in enumerate(reader):
                if i%500 == 499:
                    fout.write(insert_tail())
                    fout.write(insert_head_chair())
                elif i > 0:
                    fout.write(',')
                if len(row) < 11:
                    print(i, row)
                row[11] = str(convert(row[11], CHIAR_FEATURES))
                row_str = "('" + "','".join(row) + "')"
                fout.write(row_str)
            fout.write(insert_tail())

if __name__ == "__main__":
    main()

class TestConvert(unittest.TestCase):
    def test_convert(self):
        self.assertEqual(convert("'ウォークインクローゼット,最上階'", ESTATE_FEATURES), 1+4)
        self.assertEqual(convert("''", ESTATE_FEATURES), 0)
        self.assertEqual(convert("'ディスプレイ配置可能'", CHIAR_FEATURES), 1<<35)