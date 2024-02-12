# gsij_tile_downloader

国土地理院地図タイルダウンロードツール

国土地理院が発行している地図タイルをダウンロード、最新の状態に保つツールです。

### 背景

国土地理院が実験的に配布している地図タイルダウンロードツール（[tdlmn](https://github.com/gsi-cyberjapan/tdlmn/tree/main)）がありますが、下のような課題があります。

- Windows環境が想定されており、そのままでは他の環境で動作しない
- 逐次的処理のためダウンロード完了まで時間がかかる

gsij_tile_downloaderはこれらの課題を解消するために開発されました。

### 特徴

gsij_tile_downloaderには下記の様な特徴があります。

- [mokuroku](https://github.com/gsi-cyberjapan/mokuroku-spec), [nippo](https://github.com/gsi-cyberjapan/nippo-spec)に基づいて必要な最新を取得
- 最新版とmd5値が異なるタイルのみをダウンロード
- 作業の並列化による短い処理時間
- windows/mac/linux環境での動作
- PNG形式からJPEG形式へのconversion機能

# Usage

```bash
git clone https://github.com/akchan/gsij_tile_downloader.git

cd gsij_tile_downloader

python gsij_tile_downloader.py
```

# License

MIT License
