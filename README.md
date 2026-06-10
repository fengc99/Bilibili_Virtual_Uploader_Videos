# Bilibili 关键词视频增量爬虫

输入关键词列表和日期范围后，程序按“关键词 × 日期”搜索 B 站视频。每一天都是独立时间窗口；如果某天结果超过设定的 24 页，程序会自动把该天拆成更短窗口，直到结果不再触顶或达到最小 60 秒窗口。

程序将搜索进度和视频详情写入 SQLite，支持断点续爬、去重和重复刷新。原来的“虚拟 UP 主”频道抓取方式仍保留在 `crawl` 子命令中。

## 输出字段

- BV/AV 号、标题、链接、封面、分区、上传时间、视频时长
- 播放量、弹幕量、评论数、收藏、投币、分享、点赞
- 视频简介和全部标签
- 投稿者 UID 列表、联合投稿者顺序及角色
- 创作者列表第一位 UP 的 UID、昵称、粉丝数和个人简介
- 是否联合投稿
- 是否疑似 AI、AI 分数及判定依据
- 简介或标题中明确写出的原曲、歌名、BGM 候选及依据
- 命中的所有搜索关键词及命中窗口数

## 按 Tag 或简介保留

如果只希望保留 tag 或视频简介中包含搜索关键词的视频，加入：

```bash
python3 crawler.py search \
  --keyword-file keywords.example.txt \
  --start-date 2026-04-01 \
  --end-date 2026-05-01 \
  --require-keyword-tag
```

Tag 默认使用精确匹配。例如关键词 `又一充电中` 匹配名为 `又一充电中` 的完整 tag。视频简介始终使用子串匹配，并忽略大小写和空白。需要允许关键词只是 tag 的一部分时使用：

```bash
--require-keyword-tag --tag-match-mode substring
```

多关键词情况下，程序逐个删除不匹配的关键词关联；只要至少一个搜索关键词存在于 tag 或简介中，视频仍会保留。如果所有命中关键词都不在 tag 和简介中，程序会从视频表、搜索命中和最终 CSV 中删除该视频。删除依据保存在 SQLite 的 `tag_filter_rejections` 表中。

这个过滤也会扫描数据库中以前已经抓取的视频，因此后来增加 `--require-keyword-tag` 时不需要再加 `--refresh`。

## 关键词文件

复制或修改 [keywords.example.txt](keywords.example.txt)，每行放一个关键词：

```text
又一充电中
虚拟主播
AI翻唱
```

空行和 `#` 开头的注释行会被忽略，重复关键词会自动去重。

## 运行

```bash
cd "/Users/fengcheng/Documents/New project/bilibili_virtual_crawler"

python3 crawler.py --sleep 1.5 search \
  --keyword-file keywords.example.txt \
  --start-date 2026-04-01 \
  --end-date 2026-05-01
```

也可以直接在命令中输入多个关键词：

```bash
python3 crawler.py --sleep 1.5 search \
  --keywords '又一充电中,虚拟主播,AI翻唱' \
  --start-date 2026-04-01 \
  --end-date 2026-05-01 \
  --timezone Asia/Shanghai
```

`--start-date` 和 `--end-date` 都包含在抓取范围内。搜索日期边界和 CSV 中的上传时间默认统一使用北京时间 `Asia/Shanghai`（UTC+8），不受电脑所在地时区影响。只有确有需要时才使用 `--timezone` 覆盖。

默认产物：

- `data/bilibili_videos.sqlite3`：断点数据库、搜索命中和错误日志
- `output/bilibili_keyword_videos.csv`：视频关键信息宽表
- `output/bilibili_keyword_videos_coverage.csv`：每个搜索时间窗的覆盖审计

## 覆盖完整性

搜索使用 B 站当前网页同类参数：

- `order=pubdate`
- `pubtime_begin_s=<窗口开始时间戳>`
- `pubtime_end_s=<窗口结束时间戳>`
- `page=<页码>`

默认 `--max-search-pages 24`。当 API 报告的页数超过 24 时，程序不会直接丢弃第 25 页之后的数据，而是递归拆分时间窗口。默认最小窗口是 60 秒，可进一步改为 1 秒：

```bash
python3 crawler.py search \
  --keyword-file keywords.example.txt \
  --start-date 2026-04-01 \
  --end-date 2026-05-01 \
  --min-window-seconds 1
```

长历史区间可用较大的初始窗口减少空日期请求，完整性仍由自动拆分保证：

```bash
python3 crawler.py search \
  --keyword-file keywords.example.txt \
  --start-date 2022-01-01 \
  --end-date 2026-06-10 \
  --initial-window-days 31 \
  --require-keyword-tag
```

程序先搜索每 31 天窗口；只有窗口超过页数阈值时才递归拆分，不会因为使用 31 天初始窗口而截断结果。

检查 coverage CSV：

- `complete`：该窗口报告页数未超过阈值，并已抓完全部报告页。
- `split`：父窗口触顶，已拆成两个子窗口。
- `truncated`：最小时间窗仍超过页数阈值，存在无法从公开搜索接口消除的缺口。
- `failed`：接口请求失败，可重复执行原命令续跑。

严格意义上仍不能证明获得 B 站数据库中的历史全集：删除、审核、仅自己可见、搜索未收录或平台不返回的视频无法通过公开搜索获得。但 coverage CSV 可以证明程序是否抓完了 B 站对每个关键词和时间窗口公开报告的结果页。

## 分两阶段运行

大日期范围可能产生数千条视频。可以先只枚举 BV 号：

```bash
python3 crawler.py search \
  --keyword-file keywords.example.txt \
  --start-date 2026-01-01 \
  --end-date 2026-12-31 \
  --discover-only
```

随后重复同一命令去掉 `--discover-only`。已完成的搜索窗口会跳过，程序只补全尚未抓取详情的视频。

其他常用参数：

```text
--max-videos 100          本次最多补全 100 条视频
--refresh                 刷新已有视频的播放量、评论量等动态字段
--rediscover              重新执行已经完成的关键词时间窗口
--creator-cache-hours 24  第一位 UP 的资料缓存时长
--coverage-out FILE       指定覆盖审计 CSV 路径
--require-keyword-tag     删除 tag 和简介均不含搜索关键词的视频
--tag-match-mode exact    精确 tag 匹配；也可选择 substring
```

## Cookie 与风控

搜索接口较容易触发 `-412/-352`。建议使用自己的登录 Cookie，并保持低请求频率：

```bash
export BILI_COOKIE='SESSDATA=...; bili_jct=...'
python3 crawler.py --sleep 1.5 search \
  --keyword-file keywords.example.txt \
  --start-date 2026-04-01 \
  --end-date 2026-05-01
```

也可使用 `--cookie-file /path/to/bilibili_cookie.txt`。不要把 Cookie 文件提交到 Git 或发给其他人。

### 风控处理策略

程序参考了仍在维护的 [Nemo2011/bilibili-api](https://github.com/Nemo2011/bilibili-api) 的请求抽象、WBI 缓存和凭据分层思路，但没有引入代理轮换、验证码绕过或伪造设备身份。已经归档的 `SocialSisterYi/bilibili-API-collect` 不作为当前接口实现依据。

当前策略：

- `-352`、`-412` 和 HTTP 412 被识别为明确风控响应，立即停止该请求，不做机械重试。
- UP 个人简介是可选字段。简介接口首次触发风控后开启端点熔断，默认 60 分钟内不再请求该接口；视频详情、Tag 和粉丝数继续抓取。
- 搜索、视频详情、Tag 或粉丝数等核心接口触发风控时停止本轮任务。SQLite 已提交的数据和已完成日期窗口会保留，等待一段时间后重复原命令即可续跑。
- 只有超时、网络故障、HTTP 429 和 HTTP 5xx 才会指数退避；若服务器返回 `Retry-After`，优先遵守该等待时间。
- WBI 密钥默认缓存 6 小时，避免为每条视频重复请求导航接口。
- 每次运行的请求次数、重试、风控和熔断统计写入 SQLite 的 `request_run_stats` 表。
- 日志只显示 Cookie 字段是否存在，不输出 Cookie 值。

建议的保守参数：

```bash
python3 crawler.py \
  --sleep 1.5 \
  --jitter 0.8 \
  --retries 3 \
  --max-backoff 60 \
  --profile-circuit-minutes 120 \
  search \
  --keyword-file keywords.example.txt \
  --start-date 2026-04-01 \
  --end-date 2026-05-01 \
  --require-keyword-tag
```

如果经常出现 `-352`：

1. 停止运行一段时间，不要立即反复重启。
2. 使用本人浏览器的有效登录 Cookie，至少包含 `SESSDATA`；`buvid3/buvid4` 缺失时日志会明确显示。
3. 增大 `--sleep` 和 `--jitter`，缩小单次日期范围。
4. 不要同时启动多个实例访问同一批接口。

这些措施只能降低触发概率，不能保证平台一定接受请求。程序不会尝试绕过验证码或平台访问控制。

## 字段限制

- `ai_suspected` 是规则变量，不是人工审核结论。应结合 `ai_score` 和 `ai_reasons_json` 使用。
- `music_candidates_json` 只提取标题、简介和标签中明确出现的音乐信息。音频指纹识别需要另接音乐识别服务。
- 粉丝数、播放量和评论数是抓取时点的快照，会随时间变化。

## 原虚拟 UP 主频道模式

```bash
python3 crawler.py crawl --categories game,music,douga
```

## 测试

```bash
PYTHONPYCACHEPREFIX=/tmp/bili_pycache python3 -m unittest -v test_rules.py
```
