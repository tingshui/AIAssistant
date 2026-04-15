语音备忘录 → 本地镜像目录（不修改 ahacatcher / Cursor；不删系统「语音备忘录」里的源文件）

一、做什么
  ahacatcher/scripts/sync_voice_memos_mirror.py 从系统常见路径递归查找 .m4a、.qta（QuickTime Audio）等，复制到你指定的文件夹。
  已存在且大小、mtime 与源一致的文件会跳过（增量同步）。
  默认还会「修剪」镜像：若在 App 里删除了录音（源文件没了），下次同步会删除镜像里对应路径的文件，使镜像与当前库一致。
  若你希望镜像只增不减，使用：--no-prune 或环境变量 VOICE_MEMOS_MIRROR_NO_PRUNE=1。

二、手动试跑
  python3 ahacatcher/scripts/sync_voice_memos_mirror.py
  （默认镜像目录：~/Documents/VoiceMemosMirror；可用 --dest 或 VOICE_MEMOS_MIRROR_DEST 覆盖）
  先看计划：加 --dry-run

三、权限（必读）
  venv 里 bin/python3 多半是符号链接，系统设置「+」里的选取界面里会**灰色不可点**。
  先运行（可随时执行）：
    python3 ahacatcher/scripts/sync_voice_memos_mirror.py --print-fda-paths
  按说明在「+」对话框中按 ⌘⇧G，粘贴打印的「解析后的路径」再打开。
  若仍 PermissionError：列表里需**同时**勾选（1）解析后的 python（2）你用来跑命令的 App：
    Cursor 集成终端 → 加 Cursor.app + python；**Command+Q 完全退出 Cursor** 再重开。
    系统终端 → 加 Terminal.app + python。
  launchd 跑脚本时，plist 里的 python 与 FDA 里勾选的要一致。

四、定时自动跑（launchd）
  1. 复制 ahacatcher/scripts/sync_voice_memos_launchd.plist.example
  2. 改名为 ~/Library/LaunchAgents/com.<你的名字>.sync-voice-memos-mirror.plist
  3. 编辑其中 YOUR_HOME、脚本路径、--dest 镜像目录、python 路径
  4. launchctl load ~/Library/LaunchAgents/com.<你的名字>.sync-voice-memos-mirror.plist
  5. 日志：~/Library/Logs/sync-voice-memos-mirror*.log（若在 plist 里配置了）

五、和 ahacatcher 配合
  默认 Import 已只读镜像 ~/Documents/VoiceMemosMirror（可用 AHA_CATCHER_VOICE_MEMOS_MIRROR_DIR 改路径）。
  可选再在 .env 里增加其它目录（逗号分隔）：
    AHA_CATCHER_VOICE_MEMOS_DIRS=/Users/你/其它/导出夹
  若要从系统「语音备忘录」库直接列文件（不推荐，需 FDA），设置：
    AHA_CATCHER_VOICE_MEMOS_USE_MIRROR_ONLY=0
  重启 uvicorn。只要镜像目录可读，Import 即可工作。

六、可选环境变量
  VOICE_MEMOS_MIRROR_DEST       默认镜像目录（不配则用 --dest）
  VOICE_MEMOS_MIRROR_NO_PRUNE   设为 1 则关闭「删除镜像中已无源的录音」
  VOICE_MEMOS_MIRROR_EXTRA_SOURCES  逗号分隔的额外源目录
